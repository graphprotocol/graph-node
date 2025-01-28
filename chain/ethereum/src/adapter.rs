use anyhow::Error;
use ethabi::{Error as ABIError, Function, ParamType, Token};
use graph::blockchain::ChainIdentifier;
use graph::components::subgraph::MappingError;
use graph::data::store::ethereum::call;
// use graph::data::subgraph::status::EthereumBlock;
use graph::firehose::CallToFilter;
use graph::firehose::CombinedFilter;
use graph::firehose::LogFilter;
use graph::futures01::Future;
use graph::prelude::web3::types::Bytes;
use graph::prelude::web3::types::H160;
use graph::prelude::web3::types::U256;
use itertools::Itertools;
use prost::Message;
use prost_types::Any;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::marker::Unpin;
use std::str::FromStr;
use thiserror::Error;
use tiny_keccak::keccak256;
use web3::types::{Address, Log, H256};

use graph::prelude::*;
use graph::{
    blockchain as bc,
    components::metrics::{CounterVec, GaugeVec, HistogramVec},
    futures01::Stream,
    petgraph::{self, graphmap::GraphMap},
};

const COMBINED_FILTER_TYPE_URL: &str =
    "type.googleapis.com/sf.ethereum.transform.v1.CombinedFilter";

use crate::capabilities::NodeCapabilities;
use crate::data_source::{BlockHandlerFilter, DataSource};
use crate::{Chain, Mapping, ENV_VARS};

pub type EventSignature = H256;
pub type FunctionSelector = [u8; 4];

/// `EventSignatureWithTopics` is used to match events with
/// indexed arguments when they are defined in the subgraph
/// manifest.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EventSignatureWithTopics {
    pub address: Option<Address>,
    pub signature: H256,
    pub topic1: Option<Vec<H256>>,
    pub topic2: Option<Vec<H256>>,
    pub topic3: Option<Vec<H256>>,
}

impl EventSignatureWithTopics {
    pub fn new(
        address: Option<Address>,
        signature: H256,
        topic1: Option<Vec<H256>>,
        topic2: Option<Vec<H256>>,
        topic3: Option<Vec<H256>>,
    ) -> Self {
        EventSignatureWithTopics {
            address,
            signature,
            topic1,
            topic2,
            topic3,
        }
    }

    /// Checks if an event matches the `EventSignatureWithTopics`
    /// If self.address is None, it's considered a wildcard match.
    /// Otherwise, it must match the provided address.
    /// It must also match the topics if they are Some
    pub fn matches(&self, address: Option<&H160>, sig: H256, topics: &Vec<H256>) -> bool {
        // If self.address is None, it's considered a wildcard match. Otherwise, it must match the provided address.
        let address_matches = match self.address {
            Some(ref self_addr) => address == Some(self_addr),
            None => true, // self.address is None, so it matches any address.
        };

        address_matches
            && self.signature == sig
            && self.topic1.as_ref().map_or(true, |t1| {
                topics.get(1).map_or(false, |topic| t1.contains(topic))
            })
            && self.topic2.as_ref().map_or(true, |t2| {
                topics.get(2).map_or(false, |topic| t2.contains(topic))
            })
            && self.topic3.as_ref().map_or(true, |t3| {
                topics.get(3).map_or(false, |topic| t3.contains(topic))
            })
    }
}

#[derive(Clone, Debug)]
pub struct ContractCall {
    pub contract_name: String,
    pub address: Address,
    pub block_ptr: BlockPtr,
    pub function: Function,
    pub args: Vec<Token>,
    pub gas: Option<u32>,
}

#[derive(Error, Debug)]
pub enum EthereumRpcError {
    #[error("call error: {0}")]
    Web3Error(web3::Error),
    #[error("ethereum node took too long to perform call")]
    Timeout,
}

#[derive(Error, Debug)]
pub enum ContractCallError {
    #[error("ABI error: {0}")]
    ABIError(#[from] ABIError),
    /// `Token` is not of expected `ParamType`
    #[error("type mismatch, token {0:?} is not of kind {1:?}")]
    TypeError(Token, ParamType),
    #[error("error encoding input call data: {0}")]
    EncodingError(ethabi::Error),
    #[error("call error: {0}")]
    Web3Error(web3::Error),
    #[error("ethereum node took too long to perform call")]
    Timeout,
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<ContractCallError> for MappingError {
    fn from(e: ContractCallError) -> Self {
        match e {
            // Any error reported by the Ethereum node could be due to the block no longer being on
            // the main chain. This is very unespecific but we don't want to risk failing a
            // subgraph due to a transient error such as a reorg.
            ContractCallError::Web3Error(e) => MappingError::PossibleReorg(anyhow::anyhow!(
                "Ethereum node returned an error for an eth_call: {e}"
            )),
            // Also retry on timeouts.
            ContractCallError::Timeout => MappingError::PossibleReorg(anyhow::anyhow!(
                "Ethereum node did not respond in time to eth_call"
            )),
            e => MappingError::Unknown(anyhow::anyhow!("Error when making an eth_call: {e}")),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
enum LogFilterNode {
    Contract(Address),
    Event(EventSignature),
}

/// Corresponds to an `eth_getLogs` call.
#[derive(Clone, Debug)]
pub struct EthGetLogsFilter {
    pub contracts: Vec<Address>,
    pub event_signatures: Vec<EventSignature>,
    pub topic1: Option<Vec<EventSignature>>,
    pub topic2: Option<Vec<EventSignature>>,
    pub topic3: Option<Vec<EventSignature>>,
}

impl EthGetLogsFilter {
    fn from_contract(address: Address) -> Self {
        EthGetLogsFilter {
            contracts: vec![address],
            event_signatures: vec![],
            topic1: None,
            topic2: None,
            topic3: None,
        }
    }

    fn from_event(event: EventSignature) -> Self {
        EthGetLogsFilter {
            contracts: vec![],
            event_signatures: vec![event],
            topic1: None,
            topic2: None,
            topic3: None,
        }
    }

    fn from_event_with_topics(event: EventSignatureWithTopics) -> Self {
        EthGetLogsFilter {
            contracts: event.address.map_or(vec![], |a| vec![a]),
            event_signatures: vec![event.signature],
            topic1: event.topic1,
            topic2: event.topic2,
            topic3: event.topic3,
        }
    }
}

impl fmt::Display for EthGetLogsFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let base_msg = if self.contracts.len() == 1 {
            format!(
                "contract {:?}, {} events",
                self.contracts[0],
                self.event_signatures.len()
            )
        } else if self.event_signatures.len() == 1 {
            format!(
                "event {:?}, {} contracts",
                self.event_signatures[0],
                self.contracts.len()
            )
        } else {
            "unspecified filter".to_string()
        };

        // Helper to format topics as strings
        let format_topics = |topics: &Option<Vec<EventSignature>>| -> String {
            topics.as_ref().map_or_else(
                || "None".to_string(),
                |ts| {
                    let signatures: Vec<String> = ts.iter().map(|t| format!("{:?}", t)).collect();
                    signatures.join(", ")
                },
            )
        };

        // Constructing topic strings
        let topics_msg = format!(
            ", topic1: [{}], topic2: [{}], topic3: [{}]",
            format_topics(&self.topic1),
            format_topics(&self.topic2),
            format_topics(&self.topic3),
        );

        // Combine the base message with topic information
        write!(f, "{}{}", base_msg, topics_msg)
    }
}

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) log: EthereumLogFilter,
    pub(crate) call: EthereumCallFilter,
    pub(crate) block: EthereumBlockFilter,
}

impl TriggerFilter {
    pub(crate) fn requires_traces(&self) -> bool {
        !self.call.is_empty() || self.block.requires_traces()
    }

    #[cfg(debug_assertions)]
    pub fn log(&self) -> &EthereumLogFilter {
        &self.log
    }

    #[cfg(debug_assertions)]
    pub fn call(&self) -> &EthereumCallFilter {
        &self.call
    }

    #[cfg(debug_assertions)]
    pub fn block(&self) -> &EthereumBlockFilter {
        &self.block
    }
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn with_addresses(&mut self, contracts: impl Iterator<Item = (BlockNumber, String)>) {
        contracts.into_iter().for_each(|(number, addr)| {
            self.block
                .contract_addresses
                .insert((number, H160::from_str(&addr).unwrap()));
        });
    }

    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        self.log
            .extend(EthereumLogFilter::from_data_sources(data_sources.clone()));
        self.call
            .extend(EthereumCallFilter::from_data_sources(data_sources.clone()));
        self.block
            .extend(EthereumBlockFilter::from_data_sources(data_sources));
    }

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            archive: false,
            traces: self.requires_traces(),
        }
    }

    fn extend_with_template(
        &mut self,
        data_sources: impl Iterator<Item = <Chain as bc::Blockchain>::DataSourceTemplate>,
    ) {
        const ADDRESSES: [(BlockNumber, &'static str); 3029] = [
            (12369621, "0x1f98431c8ad98523631ae4a59f267346ea31f984"),
            (12369739, "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801"),
            (12369760, "0x6c6bc977e13df9b0de53b251522280bb72383700"),
            (12369811, "0x7bea39867e4169dbe237d55c8242a8f2fcdcc387"),
            (12369821, "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed"),
            (12369854, "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8"),
            (12369863, "0x7858e59e0c01ea06df3af3d20ac7b0003275d4bf"),
            (12369879, "0x886072a44bdd944495eff38ace8ce75c1eacdaf6"),
            (12369901, "0xf83d5aaab14507a53f97d3c18bdb52c4a62efc40"),
            (12369922, "0xd1d5a4c0ea98971894772dcd6d2f1dc71083c44e"),
            (12370078, "0x6f48eca74b38d2936b02ab603ff4e36a6c0e3a77"),
            (12370088, "0xff2bdf3044c601679dede16f5d4a460b35cebfee"),
            (12370239, "0x04916039b1f59d9745bf6e0a21f191d1e0a84287"),
            (12370256, "0x5598931bfbb43eec686fa4b5b92b5152ebadc2f6"),
            (12370351, "0xea4ba4ce14fdd287f380b55419b1c5b6c3f22ab6"),
            (12370396, "0xdc7b403e2e967eaf6c97d79316d285b8a112fda7"),
            (12370537, "0x1470ee019f673d0a4ec2c7cd0322a8c17c4f1a61"),
            (12370594, "0xbb2e5c2ff298fd96e166f90c8abacaf714df14f8"),
            (12370624, "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"),
            (12370687, "0x1c74dde716d3f2a1df5097b7c2543c5d97cfa4d3"),
            (12371099, "0xe8c6c9227491c0a8156a0106a0204d881bb7e531"),
            (12371145, "0x8581788cef3b7ee7313fea15fe279dc2f6c43899"),
            (12371220, "0x14de8287adc90f0f95bf567c0707670de52e3813"),
            (12371524, "0x0dc9877f6024ccf16a470a74176c9260beb83ab6"),
            (12372402, "0x151ccb92bc1ed5c6d0f9adb5cec4763ceb66ac7f"),
            (12375267, "0xcb0c5d9d92f4f2f80cce7aa271a1e148c226e19d"),
            (12375326, "0x4e68ccd3e89f51c3074ca5072bbac773960dfa36"),
            (12375644, "0x5ab53ee1d50eef2c1dd3d5402789cd27bb52c1bb"),
            (12375680, "0xa6cc3c2531fdaa6ae1a3ca84c2855806728693e8"),
            (12375729, "0x3fe55d440adb6e07fa8e69451f5511d983882487"),
            (12375738, "0x60594a405d53811d3bc4766596efd80fd545a270"),
            (12375752, "0x2efec2097beede290b2eed63e2faf5ecbbc528fc"),
            (12375778, "0x788f0399b9f012926e255d9f22ceea845b8f7a32"),
            (12375837, "0xbb256c2f1b677e27118b0345fd2b3894d2e6d487"),
            (12375847, "0x5447b274859457f11d7cc7131b378363bbee4e3a"),
            (12375874, "0x19e286157200418d6a1f7d1df834b82e65c920aa"),
            (12375885, "0xfaace66bd25abff62718abd6db97560e414ec074"),
            (12375890, "0x82bd0e16516f828a0616038002e152aa6f27aedc"),
            (12375916, "0x2a84e2bd2e961b1557d6e516ca647268b432cba4"),
            (12375922, "0x32a2c4c25babedc810fa466ab0f0c742df3a3555"),
            (12375929, "0xefbd546647fda46067225bd0221e08ba91071584"),
            (12375947, "0x86e69d1ae728c9cd229f07bbf34e01bf27258354"),
            (12375960, "0x3482296547783cb714c49e13717cd163b2951ba8"),
            (12375964, "0xae614a7a56cb79c04df2aeba6f5dab80a39ca78e"),
            (12375966, "0x2552018fa2768fd0af10d32a4c38586a9bade6ce"),
            (12375968, "0x07aa6584385cca15c2c6e13a5599ffc2d177e33b"),
            (12375969, "0x3139bbba7f4b9125595cb4ebeefdac1fce7ab5f1"),
            (12375974, "0x7ddc2c3d12a9212112e5f99602ab16c338ec1116"),
            (12375977, "0x3cec6746ebd7658f58e5d786e0999118fea2905c"),
            (12375980, "0x85498e26aa6b5c7c8ac32ee8e872d95fb98640c4"),
            (12375982, "0x5eb5dc3d8c74413834f1ca65c3412f48cd1c67a6"),
            (12375991, "0x35815d67f717e7bce9cc8519bdc80323ecf7d260"),
            (12375996, "0x4628a0a564debfc8798eb55db5c91f2200486c24"),
            (12376003, "0x2356b745747ed77191844c025eddc894fce5f5f6"),
            (12376019, "0xede8dd046586d22625ae7ff2708f879ef7bdb8cf"),
            (12376022, "0xa80964c5bbd1a0e95777094420555fead1a26c1e"),
            (12376023, "0xbfa7b27ac817d57f938541e0e86dbec32a03ce53"),
            (12376027, "0xdceaf5d0e5e0db9596a47c0c4120654e80b1d706"),
            (12376028, "0x2f62f2b4c5fcd7570a709dec05d68ea19c82a9ec"),
            (12376033, "0xb2cd930798efa9b6cb042f073a2ccea5012e7abf"),
            (12376034, "0x5c0f97e0ed70e0163b799533ce142f650e39a3e6"),
            (12376035, "0xf15054bc50c39ad15fdc67f2aedd7c2c945ca5f6"),
            (12376036, "0x28af48a3468bc4a00221cd35e10b746b9f945b14"),
            (12376038, "0xc3881fbb90daf3066da30016d578ed024027317c"),
            (12376039, "0xc2ceaa15e6120d51daac0c90540922695fcb0fc7"),
            (12376048, "0x99ac8ca7087fa4a2a1fb6357269965a2014abc35"),
            (12376059, "0x632e675672f2657f227da8d9bb3fe9177838e726"),
            (12376059, "0xc9c3d642cfefc60858655d4549cb5acd5495e90e"),
            (12376066, "0x57af956d3e2cca3b86f3d8c6772c03ddca3eaacb"),
            (12376072, "0x7f54b107fb1552292d8f2eb0c95c9ae14af1a181"),
            (12376078, "0xbd5fdda17bc27bb90e37df7a838b1bfc0dc997f5"),
            (12376085, "0xb6873431c2c0e6502143148cae4fab419a325826"),
            (12376086, "0x45f199b8af62ab2847f56d0d2866ea20da0c9bbc"),
            (12376091, "0x9db9e0e53058c89e5b94e29621a205198648425b"),
            (12376093, "0xd340b57aacdd10f96fc1cf10e15921936f41e29c"),
            (12376104, "0x13236638051f7643c0006f92b72789684dc92477"),
            (12376106, "0x44f6a8b9ff94ac5dbbafb305b185e940561c5c7f"),
            (12376110, "0x391e8501b626c623d39474afca6f9e46c2686649"),
            (12376115, "0x9663f2ca0454accad3e094448ea6f77443880454"),
            (12376119, "0xb13b2113dc40e8c2064f6d49577250d9f6131c28"),
            (12376120, "0x1c83f897788c1bb0880de4801422f691f34406b6"),
            (12376124, "0xd94fdb60194fefa7ef8b416f8ba99278ab3e00dc"),
            (12376130, "0x64652315d86f5dfae30885fbd29d1da05b63add7"),
            (12376134, "0x564ac0f88de8b7754ee4c0403a26a386c6bf89f5"),
            (12376138, "0xfaa318479b7755b2dbfdd34dc306cb28b420ad12"),
            (12376139, "0x6b1c477b4c67958915b194ae8b007bf078dadb81"),
            (12376147, "0xdc2c21f1b54ddaf39e944689a8f90cb844135cc9"),
            (12376147, "0xf5381d47148ee3606448df3764f39da0e7b25985"),
            (12376148, "0xb853cf2383d2d07c3c62c7841ec164fd3a05a676"),
            (12376151, "0xa424cea71c4aea3d11877240b2f221c027c0e0be"),
            (12376155, "0x8661ae7918c0115af9e3691662f605e9c550ddc9"),
            (12376166, "0x9e0905249ceefffb9605e034b534544684a58be6"),
            (12376168, "0xb97909512d2711b69710b4ca0da10a3a7e624805"),
            (12376170, "0x5b7e3e37a1aa6369386e5939053779abd3597508"),
            (12376174, "0x5494d3a61369460147d754f3562b769218e90e96"),
            (12376182, "0xd390b185603a730b00c546a951ce961b44f5f899"),
            (12376188, "0xbd233d685ede81e00faaefebd55150c76778a34e"),
            (12376194, "0xc1df8037881df17dc88998824b9aea81c71bbb1b"),
            (12376221, "0x903d26296a9269f9cfc08d6e5f640436b6d2f8f5"),
            (12376233, "0xc75a99fa00803896349891f8471da3614bd07564"),
            (12376234, "0x6ad1a683e09843c32d0092400613d6a590f3a949"),
            (12376237, "0xc5af84701f98fa483ece78af83f11b6c38aca71d"),
            (12376251, "0xd626e123da3a2161ccaad13f28a12fda472752af"),
            (12376253, "0x695b30d636e4f232d443af6a93df95afd2ff485c"),
            (12376261, "0xe845469aae04f8823202b011a848cf199420b4c1"),
            (12376263, "0xa6f253d4894e0cbb68679816cfee647eec999964"),
            (12376280, "0xa2f6eb84cf53a326152de0255f87828c647d9b95"),
            (12376283, "0x919fa96e88d67499339577fa202345436bcdaf79"),
            (12376297, "0x5764a6f2212d502bc5970f9f129ffcd61e5d7563"),
            (12376303, "0x360b9726186c0f62cc719450685ce70280774dc8"),
            (12376307, "0x7eac602913b707a6115f384fc4cfd7c5a68f538e"),
            (12376309, "0x9d9590cd131a03a31942f1a198554d37d164e994"),
            (12376312, "0xc4580c566202f5343883d0c6a378a9de245c9399"),
            (12376313, "0xb2d26108582ac26d665f8a00ef0e5b94c50e67aa"),
            (12376332, "0x157dfa656fdf0d18e1ba94075a53600d81cb3a97"),
            (12376334, "0xe0e1b825474ae06e7e932e214a735640c9bc3e71"),
            (12376335, "0x48783a921e9fe6e9f48de8966b98a427d2ccbef0"),
            (12376339, "0x46add4b3f80672989b9a1eaf62cad5206f5e2164"),
            (12376350, "0x5654b1dd37af02f327d98c04b72acdf01ba2835c"),
            (12376360, "0x27878ae7f961a126755042ee8e5c074ea971511f"),
            (12376368, "0x5d4f3c6fa16908609bac31ff148bd002aa6b8c83"),
            (12376378, "0x9e588733b77abd51879f391fce7beb6a1de7bdbd"),
            (12376384, "0xfc6f0f91a8f61bea100b3c15affabd486fc1507c"),
            (12376385, "0xa1446c173f9a78a471958c3d667f82213995f1e4"),
            (12376387, "0x4585fe77225b41b697c938b018e2ac67ac5a20c0"),
            (12376390, "0x9359c87b38dd25192c5f2b07b351ac91c90e6ca7"),
            (12376394, "0x71eaa3f86a541177f64a15c5f5479aedcc426860"),
            (12376397, "0xf8dbd52488978a79dfe6ffbd81a01fc5948bf9ee"),
            (12376397, "0x4e57f830b0b4a82321071ead6ffd1df1575a16e2"),
            (12376406, "0x7cf70ed6213f08b70316bd80f7c2dddc94e41ac5"),
            (12376420, "0x290a6a7460b308ee3f19023d2d00de604bcf5b42"),
            (12376427, "0x608dadd4b1673a651a4cd35729fc657e76a1f9e6"),
            (12376433, "0xa9fc406744afdd46ef96f6ddb343fc124ec25254"),
            (12376451, "0xc3f6b81fb9e6db259272026601689e383f94c0b0"),
            (12376453, "0xd8e1a96b9be06c3f62789a308c963ca015bd84f3"),
            (12376458, "0x2467bc23cd16023de91eded28f89593018a4bfe5"),
            (12376460, "0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424"),
            (12376461, "0x381fe4eb128db1621647ca00965da3f9e09f4fac"),
            (12376469, "0x091c0158ab410bd73ca1541409d5a22e90146a04"),
            (12376472, "0x21c4921201c19aa96cc01d50f9c7fbdd6032b829"),
            (12376475, "0x9a9cf34c3892acdb61fb7ff17941d8d81d279c75"),
            (12376478, "0xeb95781f32adac717b162de2b5e3a7949e98f940"),
            (12376479, "0x94391eecd76d9cb35013d7f263b64921e31842c8"),
            (12376501, "0x3a0f221ea8b150f3d3d27de8928851ab5264bb65"),
            (12376503, "0xeab149986139bb13b95f90df397d6e89af3ae589"),
            (12376505, "0x92995d179a5528334356cb4dc5c6cbb1c068696c"),
            (12376507, "0x0e2c4be9f3408e5b1ff631576d946eb8c224b5ed"),
            (12376512, "0x86d257cdb7bc9c0df10e84c8709697f92770b335"),
            (12376513, "0x79115e1d29457a1a13cfd2b6f3058cb5be731615"),
            (12376514, "0x80c7770b4399ae22149db17e97f9fc8a10ca5100"),
            (12376519, "0xe35ae121d44c3d884de7821978802486cf9bf715"),
            (12376525, "0x73a6a761fe483ba19debb8f56ac5bbf14c0cdad1"),
            (12376527, "0x082eb122f23802e0375086d966b8a5496e03266c"),
            (12376533, "0xa6b9a13b34db2a00284299c47dacf49fb62c1755"),
            (12376535, "0x387c2455e9a75a0f9929fade4eb4d24d620187c8"),
            (12376536, "0x6ab3bba2f41e7eaa262fa5a1a9b3932fa161526f"),
            (12376543, "0x4c83a7f819a5c37d64b4c5a2f8238ea082fa1f4e"),
            (12376553, "0x2a5415c7c286209165362a269508057f3577a031"),
            (12376564, "0xb0f1fad0e0d8988c8c0b1e857febceab167100ce"),
            (12376567, "0x5116f278d095ec2ad3a14090fedb3e499b8b5af6"),
            (12376568, "0x06729eb2424da47898f935267bd4a62940de5105"),
            (12376576, "0x7a2fed060d14fe931cf089b2eb2bb8e371d7a3b8"),
            (12376598, "0x69d91b94f0aaf8e8a2586909fa77a5c2c89818d5"),
            (12376600, "0x8e6c18db54314108c6d9b4f2ef27b55dc8fd945d"),
            (12376610, "0x1a80afe14143637c0b7609e6e276464e4f748014"),
            (12376612, "0xb77c56f863fff3a9593a8716f2236c33df276950"),
            (12376614, "0x34b8487fc2912c486b04d1436b07f19f7730cd43"),
            (12376618, "0x5631ad9041e9e4595e3b1f87f904200b80ec669f"),
            (12376622, "0x6a219c7cbd18d539ae3aaea28d6dc05821aa2db4"),
            (12376633, "0x8e26e2fc8140280fba3e34bfdca7fc1102c1ae04"),
            (12376640, "0xb0f2fca52066920620a90e2e5198f3edf7c28686"),
            (12376642, "0x4d1ad4a9e61bc0e5529d64f38199ccfca56f5a42"),
            (12376647, "0xe74a0ce0a911fcbad8b8517372a1b3de5f62cd16"),
            (12376658, "0x2028d7ef0223c45cadbf05e13f1823c1228012bf"),
            (12376661, "0x477bfaef649b4cef1fba91caccb48080d6c9ca28"),
            (12376663, "0x13dc0a39dc00f394e030b97b0b569dedbe634c0d"),
            (12376680, "0x54514ee06c8c10029f631eafb0964573e554e319"),
            (12376686, "0x211d30e2929dbfa4f8395fd1854a1b6e720ca011"),
            (12376692, "0x26bb4b679ff64f8dddaced46c50410b91f96485c"),
            (12376701, "0xaad5eea6725f2ae5df8dce7cb625512a0e9882db"),
            (12376706, "0x9febc984504356225405e26833608b17719c82ae"),
            (12376707, "0x473bd625c88a26d334318a50d9885b3c08df4bbe"),
            (12376716, "0x666a5fd59b5e3c27fecc57f85f2a554966518e90"),
            (12376729, "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"),
            (12376731, "0x32f36bdc2c49775f1c31c80d83b7e6971b70c397"),
            (12376735, "0x38b13a9e112cbdb281d1aac4bdc10a5a0c0c9651"),
            (12376738, "0xe16be1798f860bc1eb0feb64cd67ca00ae9b6e58"),
            (12376740, "0x60e4952f8540b043b8b83e33c4893e13d9a43ea8"),
            (12376751, "0x11b815efb8f581194ae79006d24e0d814b7697f6"),
            (12376752, "0x5f95ce015fb0f935506aa1812f4b0a709abb7ea7"),
            (12376762, "0x575cc7f6ff8b8c0b2becc94a4f0282b7247a9e4e"),
            (12376765, "0xdd0cf265951256a0eb3a927f657959dea4827b6d"),
            (12376766, "0x8af7e476b1059597a402ceda71f37ce7f910b5a4"),
            (12376768, "0xb4cd8c338662590abd2d9eab0d79081f9507bf3d"),
            (12376771, "0x713ce36a75887ddbbfa1dacd8ef6128b36e34d0a"),
            (12376775, "0xb2f8b3bad4325c3c62f294da45fc144b1b180cc2"),
            (12376788, "0x1d9de3ad2e0742b6642524c12afe13568035fea3"),
            (12376789, "0x0df4019374c11a7795cfef78f88d4c1aa3300462"),
            (12376796, "0x37430d951a3876fd5337f215ffeb5c8f840cef75"),
            (12376796, "0xf49ab487fef27492f543ff9bfcf9ea9ca1e9023d"),
            (12376804, "0xd35efae4097d005720608eaf37e42a5936c94b44"),
            (12376807, "0x24017386cc0af0555a6e5b0fe0443e145648612b"),
            (12376809, "0x3d8b25d559c39fb22811569fb6944e8887ee9ea5"),
            (12376819, "0x57729d1496ab1f421a451019d28e5062dcbbdeda"),
            (12376820, "0x3d61c8c42d6cda9cefbebe6e322597ddc37692ff"),
            (12376821, "0xf148bdb609575b613cd615bafe2b22cc47532ee8"),
            (12376824, "0xf987b7aa76b175c3230c395a495350d2eb146391"),
            (12376826, "0xf87bb87fd9ea1c260ddf77b9c707ad9437ff8364"),
            (12376848, "0x634af6dbe36fd92d799d5442bac62873a8d96aa9"),
            (12376884, "0xc5003f495a5072a81771ef8aa38c6302f45f0ace"),
            (12376885, "0x905e3a9fea6c88dd8f05c6cb3df8f48919219229"),
            (12376897, "0x0bed31d1070a3713a2fcf0be500b53774f73acbb"),
            (12376910, "0x27dd7b7d610c9be6620a893b51d0f7856c6f3bfd"),
            (12376916, "0xe178380d5879e4c590f6cdd2996e2f601b2e0ecd"),
            (12376925, "0x4c54ff7f1c424ff5487a32aad0b48b19cbaf087f"),
            (12376926, "0x067b0d52df7da4a13142a02c1d2c19496d6b0508"),
            (12376936, "0x7c79d003a7973dbfd663b5124e06ee025a81eb1b"),
            (12376942, "0xdd5a65da22031b6ae5205fd89f67b5141e4f0ead"),
            (12376957, "0x84ae4aa9581e6b7f50f660c5b6970aef1ed82be8"),
            (12376974, "0x7cebafc6fd780c266c25329138b56bfe251c8f86"),
            (12376979, "0xba3f54d4ed114a359b3ab81ed11834a2b54addc1"),
            (12376980, "0xb26ba50fd79d5faa2202715f743e86c8deb9375e"),
            (12376993, "0x82d7b381b6144670fc6777edddd0e5737f6f0633"),
            (12377001, "0x504ab5b9f8c025505a3cc3c06d1cd7b22d32f093"),
            (12377014, "0x7d3e5afdb0e7416b401b17a64d63b63e07197272"),
            (12377036, "0xb9546374d2127a056cfb4dbc6d987e476146c4ea"),
            (12377037, "0xead17530d2d12986180a939fa70ba28f4d0d015e"),
            (12377038, "0x2ab653894ce12ae9b4538aefc90b2d6e96458e1d"),
            (12377048, "0xc129bed3196de3315b1be05c6d1a2455f610dcc0"),
            (12377078, "0x479f037ebcfc0a869b2b0a5a0d9227d730d8f0a3"),
            (12377088, "0xc287afcf1b54c9ec347d37df1707609be4594cca"),
            (12377102, "0x384469431c307370c34265213daaa75139522182"),
            (12377106, "0xef43c28f19b9e58444ad72d709c06a4a630f7531"),
            (12377108, "0xa98a9e4bd2fb25a4860a6ddb1a45a06f050a14c5"),
            (12377109, "0x0a09ccbaad2550746560b5a66bc77945820f2954"),
            (12377110, "0xa96c549fa361181a94c081597171e17d27459dbb"),
            (12377122, "0x4362a591eb53e4a1430fd6ed14e276d5b9df0577"),
            (12377140, "0xbc9b75563e8df1dc761589444103342ec582cc29"),
            (12377142, "0xe8e7201e427c2509f03f5bf6cece711687623dff"),
            (12377184, "0xde4d1af93dd7e333d071411a3d68cb1ae09bf4e3"),
            (12377194, "0x15321a9643e5f2b20c124f7fc809dfb299830459"),
            (12377197, "0x13de617a7a77ba23e3cb583774111f0a068ffced"),
            (12377209, "0x8ecc2244e67d0bb6a1850b1db825e25354cf881a"),
            (12377251, "0x3cb75142bdae2bef3eb119affb288aeb0599dc4f"),
            (12377258, "0x90904cb75a9bd6cb1d4fbeed96bc1615f854aa52"),
            (12377287, "0x2dd56b633faa1a5b46107d248714c9ccb6e20920"),
            (12377288, "0x753a90ae2fa03d31487141bf54bd853b27f7bcf5"),
            (12377292, "0xb201503fd668f5dd67b128ad7f4131f05f2b4ee4"),
            (12377301, "0x2b43fe4f41d871fbc75af6e0ce85bce38ff1edc7"),
            (12377301, "0xefc19ba107bf1ce57a7ab5dea228fffb899abeb0"),
            (12377317, "0xc3ea1c2e309ff99ed7b17a501bd8d5998918b097"),
            (12377318, "0x99c7550be72f05ec31c446cd536f8a29c89fdb77"),
            (12377321, "0xe331de28cd81b768c19a366b0e4e4675c45ec2da"),
            (12377322, "0x5f78a75ee92885cb8b32bdde19b17600bebfb270"),
            (12377323, "0x525a97f692928faabc6f85d0c6248b79fddf7e83"),
            (12377328, "0x07b1c12be0d62fe548a2b4b025ab7a5ca8def21e"),
            (12377331, "0x064880be0b82d07f5476653e3ceb1dbcd14be40b"),
            (12377359, "0x7cf12cef5ce9e5e068ebdef470ff8295e26c47b9"),
            (12377368, "0xba8eb224b656681b2b8cce9c3fc920d98594675b"),
            (12377377, "0x30677200a9e6a3f406dd4e0792ff0d7ef4286cdc"),
            (12377389, "0x3c0cea3b62de515285b6053a4afc6e7702300d53"),
            (12377398, "0x264c7a632ce9171f93fba58a834bb78b4dab9d82"),
            (12377417, "0xd0effc6828972483db1c64106f71d6ad12606a53"),
            (12377465, "0xca6ce1d5eb9f04300ba155e4bb45d62d9d034d4f"),
            (12377503, "0x24ee2c6b9597f035088cda8575e9d5e15a84b9df"),
            (12377545, "0x32ecd439b57fb881ea1f5df2f8581b6fdac0acac"),
            (12377552, "0x7ca5ce95b5494dae094423d52be0b8997bed1b83"),
            (12377553, "0xcd9dab5e666de980cecdc180cb31f296733e2587"),
            (12377593, "0xe72d262158f402faf553179b2b4aff23dfad6d4c"),
            (12377596, "0x1ef6295d1c0b321e7a139410ab4ab5ed73036d56"),
            (12377602, "0xc8d73d01a52ed6e212475c404ef900f1c831913e"),
            (12377612, "0x85205ea75a442f6897abef6b7e9d0c8e9366b57f"),
            (12377616, "0xc5d8491be7d9cae4b5e84735dda387d549ca50cd"),
            (12377621, "0x495864cad6cf60e97efcd3f14ff8b1f167956ba2"),
            (12377643, "0x9df48332aa27f22e2e709e07b4db8f0252f71bea"),
            (12377646, "0xd6107be86af0be3b822bb29eccd2fe37d2f706e5"),
            (12377655, "0xe04c78ae1ba3e398e6da81dce94ce6eebb3ad602"),
            (12377659, "0x23bb251705615b4efe9442be5f2aeb0e20b39858"),
            (12377693, "0xc837fe2e91cc7210eeb6e054ec9dfb9fdc4a26dc"),
            (12377699, "0x9781c95d9f7615b13d0b1cfc9a0a3e84ff4e7700"),
            (12377703, "0xcdc3dd220a81e7f020891131a535bd5e47fdcc6d"),
            (12377706, "0xcd83055557536eff25fd0eafbc56e74a1b4260b3"),
            (12377707, "0xe6868579ca50ef3f0d02d003e6d3e45240efcb35"),
            (12377710, "0xff90fb880da9738b2044b243daf8172bfe413b5c"),
            (12377729, "0x4cb77f47c77fd0cc8879a5574d80864b45fb286f"),
            (12377737, "0x68f8d8fbc1283f9667d6373c55f96f8c0aea1c33"),
            (12377760, "0xc246467ab1466f4963ba45c335479b3055e82060"),
            (12377768, "0x290c4ae844b49ecda04c14067dac992853fb72b1"),
            (12377771, "0xd0bfbe394efa343740857060092966fbed0d1a61"),
            (12377787, "0x49189746e349147c2a124ce30e83b036e3a850d8"),
            (12377795, "0x1295e6b8c73a39a577dfe8e00d954640970706ad"),
            (12377802, "0xd8c81e66a71fcfe8258b0cf84267275cdc91cac3"),
            (12377807, "0xee4cf3b78a74affa38c6a926282bcd8b5952818d"),
            (12377809, "0xef01fdbb48a7400963d6eaf1a1efd555a5bf2a92"),
            (12377811, "0x15e49d0bde9dc84175ab13e6c56b9a21e9639d53"),
            (12377879, "0x9b92e5222d50a36b5926ef646e4cc8d4b18f2a36"),
            (12377880, "0x5a822604e9928e3a65793da6a5409b2aa72216dd"),
            (12377889, "0xc2c67b8ae2acf7c8c0dd8bd570985d729fd6e1f5"),
            (12377906, "0xed45c401f948a543cb0ca6c5ad45f588cb388ec1"),
            (12377912, "0xf34efa220222b9232f0237918618000ff4860608"),
            (12377927, "0xfbba47b4c4ded47aa154a1b6dc06ec207166fc13"),
            (12377962, "0x1a82009e8d6d3e893e5b01c6e98133dfb119140d"),
            (12377983, "0xd223ef8ab212c7aa58e0d240418f8b0fdbde1aa1"),
            (12377988, "0x85c894e06791fc7a21d8915e14dd0956971488e7"),
            (12377989, "0x43a4fbb71025fa2c3073a58fee6de69da33b3aa8"),
            (12377994, "0x74033a2cfb93a0fc41219611950cd7bff21407df"),
            (12378005, "0xe9566699f443459dfab77cbf100a63f0d5f75d43"),
            (12378008, "0x211871204585ef0197423c97c44431dad9daa0ed"),
            (12378017, "0x6762e7229d97a7029cb2d5ec65719a3c19f6f4bc"),
            (12378020, "0x691c9c0b050c2c3d46dad37b1b4c3666f13ecfce"),
            (12378021, "0xf425ee359339695e234b973fb844085fa1b50014"),
            (12378067, "0xf75f75dd08418988c1b8e7ec9c64fa935b6b6f8d"),
            (12378068, "0x3840d56cfe2c80ec2d6555dcda986a0982b0d2db"),
            (12378085, "0x6d9e83bd525742d215690159d53dc09dde13e7be"),
            (12378098, "0xc8c5b5d0e2e4923e07d1aa1a79968f0c3b407c35"),
            (12378098, "0x8fe8d9bb8eeba3ed688069c3d6b556c9ca258248"),
            (12378113, "0x8f12c6e9119ce7fb3db0d67af757810c4a2f3199"),
            (12378140, "0x5d8beea6297c6cdbd7cf925e23b0ffe696212709"),
            (12378146, "0x2e3d86bd99f68328139378b285ce2e745b81bb75"),
            (12378146, "0xa46b45ff67dd5e3a37d6b40319c51aeae9c5bbd2"),
            (12378174, "0x1c4f761649c3d4dd31a83ea0548f79585c3999ba"),
            (12378190, "0x601f7efaca945bdba9cbcc6d1ebb22e04d0a1f2d"),
            (12378205, "0x4eb91340079712550055f648e0984655f3683107"),
            (12378228, "0x6350b0d4f9e7a52630073fe0cab6dea0f4052bf3"),
            (12378238, "0x565bfb5e5245fdb044845f2e01b5003f0d7abd80"),
            (12378254, "0x3b4f91c5f96d39235b1baf54dc0cfde5b6d36982"),
            (12378258, "0x5c1f3e356829bd5d215fc673b699843449828bff"),
            (12378271, "0x83c5e077dab04c79464b4c55d28ea5c9b8c49bae"),
            (12378290, "0x354dd08c557ff37301715adfdf83bd9fcd04707c"),
            (12378295, "0x1d0f3c580378daf7c66c090e098819f633e04880"),
            (12378320, "0x92c7b5ce4cb0e5483f3365c1449f21578ee9f21a"),
            (12378321, "0x095a1b2b2655a58cc1920b044c4b7305d4270524"),
            (12378334, "0x8d8278c40e30cc567c498273387aa377d15aa832"),
            (12378341, "0x868b7bbbfe148516e5397f23982923686182c2d2"),
            (12378370, "0x4b133a98c5f38bc74c9f56783399897f2e6e18b6"),
            (12378373, "0xe6edb1a920e230c0c7db5ffa61c9397b0624efce"),
            (12378420, "0xb645849765616c019c56d6c7bd339d606919d146"),
            (12378433, "0x80f91f7da5068a055e48f213ce8406a5ba0280b1"),
            (12378450, "0xa34f0d0314db32f41e1194816d56d55d1f7ca7b5"),
            (12378453, "0xd0fc8ba7e267f2bc56044a7715a489d851dc6d78"),
            (12378491, "0xa372c9e116c68f0c9fb2fc9b011a8513a4a5d211"),
            (12378508, "0xcc56baae6ae8abbf83b552dfe72672ae2e7b8b6b"),
            (12378508, "0x391d16f51dc64cd6c38ce041512741a47abfa0da"),
            (12378509, "0x45d8ced29bf3af9f2d369ec6ab86a0ed67dfec90"),
            (12378521, "0xadd1b6a7e3be5b8d5b8d8b1610bed78f8747d7e4"),
            (12378527, "0xd9ed2b5f292a0e319a04e6c1aca15df97705821c"),
            (12378559, "0xbd5c3633a15061d6dde572e36c4d1e59df4407b6"),
            (12378567, "0x29ac7e17a6dcb7d3ffd8cbd7c1096bec0966c743"),
            (12378592, "0xb60b34d830f26c5a11c47ddb1e0a1f31d90a78b1"),
            (12378618, "0x85dc3d90d4d85d38c8a033796af5691a0719625e"),
            (12378622, "0x8fe1b1b3c21fb4193766b87b6975e2f0a52acb8b"),
            (12378624, "0xcd272822b0271db048d90fac3357a79b62006c6e"),
            (12378649, "0xbaec0e18c770993ffb1175fef493b5113cc6e32d"),
            (12378655, "0xacd89784e7016738e74b35146dbedd8074f20733"),
            (12378675, "0x1bc71d896330ac8ac63e07a9da8bf27010e065bc"),
            (12378678, "0xb055103b7633b61518cd806d95beeb2d4cd217e7"),
            (12378700, "0x0c69e0438ee67a1881bb686ad4b87a9e0772dee4"),
            (12378729, "0x91f461f51e035fa00bd54074608f366885428a50"),
            (12378750, "0x4f25f309fbe94771e4f636d5d433a8f8cd5c332b"),
            (12378755, "0x7335118ece5464167ec940c4dba3291d1484e00f"),
            (12378762, "0x58b8a1cae4c8eede897c0c9987ff4b5714ef3975"),
            (12378762, "0xc1ed7f3086575fc5dc1b850ede2b65cbe43e022e"),
            (12378768, "0xfc4602aa8b7f070f3d0e17f27b10dbcc737474e1"),
            (12378816, "0xe062510e8c5249b2584c2382bd03e22f70dcc2f9"),
            (12378838, "0xe7e340f19b827bbdce1d74067cc49c76a3444772"),
            (12378852, "0x3470447f3cecffac709d3e783a307790b0208d60"),
            (12378853, "0x9f178e86e42ddf2379cb3d2acf9ed67a1ed2550a"),
            (12378856, "0x9da737f94c9722ddc132bacd1ccb8f6691b93033"),
            (12378895, "0x9a143f844831e2277cb27665f8b94b4f50e14942"),
            (12378930, "0xbeadfd4c74afaaa7bb62cb4924de4e4d2d0382b1"),
            (12378933, "0xd321f69da1285454705e71f3437d36e29a4febda"),
            (12378951, "0xc2757ea8b4bcf4ed59568b374d4b7a0de522735e"),
            (12379056, "0xcc7e0f514d8ef66e4ddcb6a5765ec3dd64492f4f"),
            (12379095, "0xf8a95b2409c27678a6d18d950c5d913d5c38ab03"),
            (12379134, "0x1090fe5dd07455318d24e0995791cd55f86e2d52"),
            (12379155, "0xa44002e8062c90a8f8b3cfce6ec70990bba28ae5"),
            (12379157, "0x6d59c765ac37a1953f31094beb38dbeeb451a8f6"),
            (12379181, "0xa6cf5cd8386af67d8aca50022afa22de6c98c6b5"),
            (12379206, "0xe95a4c410a7e1b8c3282be1af9df57c24e48eb85"),
            (12379213, "0xb8b71473301407f29c31f0e2b13af963aeab4393"),
            (12379227, "0x03a86d1488dd6ae052214cac0d845d6ac0153d7a"),
            (12379246, "0x421fa903fddbeb003606dd49ed3d12d068774093"),
            (12379254, "0xcbbc981bd5b358d09a9346726115d3ac8822d00b"),
            (12379264, "0xb971480221e24023ecf9e69f1a9bd2b600453950"),
            (12379279, "0x94651cbdd2c1fa4f7c1139e7e89fc975a9fcad5e"),
            (12379302, "0xdeaf677af91ec0655124860e1a15177aee86303e"),
            (12379313, "0xdc1577aa8885f296b84204cd94499242d46ec75c"),
            (12379317, "0x9861039bf9b66b3b30d59da1e7d4034fd08b8b3f"),
            (12379392, "0x2e8daf55f212be91d3fa882cceab193a08fddeb2"),
            (12379396, "0x14424eeecbff345b38187d0b8b749e56faa68539"),
            (12379405, "0x2fd08a4473caabb6a778ebb90b91cd95fc7b89cf"),
            (12379415, "0x6e84d5caa189c7c4e3d41801e54dedfdd74e23cf"),
            (12379453, "0x780bce7a74e61048139a4d81281058ae127e1284"),
            (12379465, "0x206c9f6ad08a8f2a3cc05bd3939cd49287560096"),
            (12379486, "0xcdb0bacd8dc32c45dce0f97536646cd40835fd93"),
            (12379489, "0x41d95f74cd12e606106ff1494e950bf77dd1c9c4"),
            (12379492, "0x0a615a87f843c714cf74b2f44f7398317cc603d8"),
            (12379502, "0xc63b0708e2f7e69cb8a1df0e1389a98c35a76d52"),
            (12379506, "0xf05e46757f2e119183a168e7d7f5d02125fa9899"),
            (12379512, "0x8776fc4408e7f115f5a77235844adc8ab4b7aa70"),
            (12379560, "0x689b322bf5056487eec7f9b2577cd43a37eb6302"),
            (12379577, "0x036af0f99eba3e8e90c77cb53140fc89f2b4ea97"),
            (12379585, "0x79e42a2bb91a0f9118e2b5231958c1eaefce390c"),
            (12379590, "0x5766a5231b40f0dc3e006d0f820fd2304c34699a"),
            (12379596, "0x00ef3b7e122853ba088a1833f7bd3b5ad3dd0d32"),
            (12379604, "0xebc6151f098b45790afdc2451811837df0c53c34"),
            (12379609, "0x838bea052c4ab25a94867cc4dadf45d1a6cafba5"),
            (12379644, "0x97967631c9efac106fd0441575dfd57322537ee8"),
            (12379648, "0x000ea4a83acefdd62b1b43e9ccc281f442651520"),
            (12379654, "0xaff587846a44aa086a6555ff69055d3380fd379a"),
            (12379654, "0xbe7e3ed03494a4ddd5b39ffec7d4eb6b5c8ad7db"),
            (12379670, "0x3a6c1f684a1c50aa371651cb336f3eb02e88bbee"),
            (12379693, "0xc08aeb46af5d4fb52582405b760567d26b9a36a1"),
            (12379712, "0x4012737a154f1c44df37379a765b87a1ea397edc"),
            (12379735, "0x5412be87c36d671358353dffc839ef79cdf79530"),
            (12379750, "0x919d9fb4f00089bda65dff6c90cb78552ee4c19d"),
            (12379769, "0xfe4ec8f377be9e1e95a49d4e0d20f52d07b1ff0d"),
            (12379783, "0xe936f0073549ad8b1fa53583600d629ba9375161"),
            (12379784, "0x39c9e3128b8736e02a30b2b9b7e50ff522b935c5"),
            (12379798, "0xe405113bfd5b988bdba4a4ca9419a18f9e2828a6"),
            (12379818, "0x70bb8e6844dfb681810fd557dd741bcaf027bf94"),
            (12379821, "0x75087d1032782be8d5a6d8fe2256bd64d50c8838"),
            (12379827, "0x83cfaa49b75e394935ffb9bbd18c045e797d6a35"),
            (12379849, "0x32dda59c0d3a5fb14a527b815d1596c2ee9a4ca4"),
            (12379863, "0x37063a1083788f04be5fab0365b65daee916b440"),
            (12379863, "0xec749ed17ebc1487e43bc5bbcb2f20a766d3a7f6"),
            (12379865, "0x11397974086d2a762ac829dce56a33d79fa2fab3"),
            (12379882, "0x5b78b01680dfd84948980ba91bc3be461d40ae49"),
            (12379922, "0xee5060126ccd0e8336be60c7ca576c4c2e392d36"),
            (12379942, "0x76885c4665793c21bede82cff2a1fc099bad7b4d"),
            (12379948, "0x487d9eb06343c2445766a21dfa27d91074a34485"),
            (12379960, "0xd5ad5ec825cac700d7deafe3102dc2b6da6d195d"),
            (12379963, "0x03d9190f2896aca1671ae1944dd8523dbef1847e"),
            (12379970, "0x92f0b57e3814e4bd74ef6a6fd6d825db522ccfe2"),
            (12379984, "0xf81ba0c35e59cf01682b7f2a7a3d8ea6756eba46"),
            (12379995, "0x7f6ab5f1798761c610388fcb7e1178dcd7b4e8ad"),
            (12380006, "0x122e55503a0b2e5cd528effa44d0b2fea300f24b"),
            (12380014, "0x4afdebe09d33fd914350c563bcbe1f6a396c68f2"),
            (12380016, "0x06783fe89bf5906cc48c2c53f73a99fc82996ead"),
            (12380018, "0xb8ab1f72f62d59f17ab92910528bbd031d827ba5"),
            (12380023, "0x99530180bd113a816f5920d0748528cfb2009585"),
            (12380042, "0x1a8fb0d1bb70a890404dc399b0964a405386a24b"),
            (12380045, "0x29fbf57eb9f7578eec4b5ff777a2cee5d6134e3a"),
            (12380071, "0x62cc5cb2700a1eca8eadba9a3467b4887a51a028"),
            (12380104, "0x14243ea6bb3d64c8d54a1f47b077e23394d6528a"),
            (12380114, "0x7c8dbf6e88f52cb56dd30190558cb982f62fc660"),
            (12380123, "0x6d15d1e082fea85f98471e029b7e56f847a4524f"),
            (12380143, "0x0ed18a54afe970cdff76d2b17251c22025d41c5d"),
            (12380176, "0x7d4afd8e363ccbdd879836eb9dce560d8d614699"),
            (12380204, "0xff29d3e552155180809ea3a877408a4620058086"),
            (12380221, "0x5cccad2332292cd5882365bb3584424241ab2210"),
            (12380224, "0x6e227a93687d9f32251cd236529fa59025a33845"),
            (12380224, "0x084b5191bd08412952337b1108b6e5942418928f"),
            (12380247, "0x64ac443960c7cc924acc42eb245035cc34f33154"),
            (12380262, "0x7f6de6c726d09915912d85a4b3ffe4838798b349"),
            (12380275, "0x471aca8f8dea063695b2b15fbca3e402fa4e3385"),
            (12380409, "0xfc9da2ffb74cefffcec376571af6876868e865d6"),
            (12380417, "0x05a5483f6e5a933c98c9987f81682c90e0133d79"),
            (12380457, "0xe92b617ab83ac1f9601ed584ae297665649c8c15"),
            (12380462, "0xfb9256c0f94d0b7495327426c0dd954bdf72d220"),
            (12380463, "0xe7477f50eca9f4f0d73717b455994b0086dc544f"),
            (12380498, "0xf4a7f5bd555ae46eab6b6c5f83332eee6c598970"),
            (12380521, "0x1f81a9ee204e699ed34f7be997ece58605108b16"),
            (12380565, "0xebdf58b78b2523611a34ded92ddf663c432d0a99"),
            (12380582, "0xd4146e01e8380624e4f69edf178aea1e8a880cb7"),
            (12380609, "0xc36180160b66c8882d66a93568a73131aef0c450"),
            (12380609, "0xd6eb16947ddaf5c307e701afb6b7f4b5857c6eba"),
            (12380637, "0xa79e94cb24b2f2fc75b1d30a315cf2d266628a7c"),
            (12380657, "0x5b781915b6b610a1aba8135ad5df084ffe528da4"),
            (12380698, "0x838cf121dde0d4c1bf85de6eb56a9c5015e59033"),
            (12380748, "0x27d1e619811e239588566b54b2aea5040aeb8aaf"),
            (12380946, "0x0923b26b1874d76e9141450cc69241229e04914d"),
            (12381061, "0x536fdc52df8ef6c5ec7b5616d6442965d451170b"),
            (12381096, "0xce3dd41cfe1d14e66df18fa66ff5bb1dfd98b880"),
            (12381099, "0xec77148d240b031146025517175f9dbc5af9774f"),
            (12381110, "0x9a06cbd76d8bd043dba1530be4686fd298e4b464"),
            (12381186, "0x05a9716cd1c08361252ebd7c8c88c3b26c5ab73f"),
            (12381209, "0xe931b03260b2854e77e8da8378a1bc017b13cb97"),
            (12381214, "0xed1da966136abaa240582166827021aedf55a6b5"),
            (12381267, "0x8119c00d8a02c61f813675d23d5acb98851cf274"),
            (12381292, "0x6f483af1e32cc556d92de33c0b15c0f8b03a6d12"),
            (12381306, "0x11184bf5937beb9085b4f070e7b410be052e37a6"),
            (12381307, "0xf22bb4e5b4352ac18eed4b7008e7e8ebbc587938"),
            (12381310, "0xb29148f6ea9f2d78ba9b2d6b30af37afdcc31a9e"),
            (12381318, "0x1afef33988d6c332415e83a5a2740ffebbaa4b75"),
            (12381351, "0xc090993fe84b9c0b1baa889b94b5e4bfbb9f13a0"),
            (12381406, "0xe0e9b1a40ca2b6a7c945c684ccf619449ee35d7b"),
            (12381423, "0xba9458e13df13517612a0f0b91a9bab0d22ade90"),
            (12381506, "0x29ee0ff115b8574b98eb6b5bd697059a15295c17"),
            (12381540, "0xdfe2b010ca3e95438f4442752655d6f99d2dadea"),
            (12381641, "0xf9d00163f49fb4429714b50242524b43aa404c7b"),
            (12381662, "0x3856986a62ea0f26c4aa383350bf1a873953739a"),
            (12381763, "0xb06e7ed37cfa8f0f2888355dd1913e45412798c5"),
            (12381801, "0x0bc1a6edafa658e6501d2107d8f31d490752b94b"),
            (12381812, "0xef2431e4d8c55711e9728c6f0006035394232c69"),
            (12381836, "0xdab59355190d110f9f71a5f97993511343264db3"),
            (12381839, "0xe1b517e0b8f9a996e67f9089dfce81fb494579ef"),
            (12381873, "0x5559d1c111dcb5591303ff14a869d84b6749aacd"),
            (12381919, "0xacdef3d53f2a487c0ab2ea68a1ce5e0bddf841ba"),
            (12381959, "0x81663f727bf8461a9115b01748105cec0fe44446"),
            (12382031, "0xc69b99e35d951a9d023355a89a848567d8b34687"),
            (12382054, "0x106eaa4c278f28c5c8bdc5adb39b6421caf6acad"),
            (12382110, "0x03a90943380f7eb21fde74b94d945aa3e8508740"),
            (12382168, "0x68c89c8ae023904070250c6b71ac4b6fce104b38"),
            (12382227, "0x0348287cc7e7b31caa986cf68fc2c665b09ddaa1"),
            (12382267, "0xc4ee13dcd81b99db885effe36930950e014b839d"),
            (12382280, "0x818dbfb7e215ae074db4167902da9d89aed6ad3f"),
            (12382323, "0xf0586fef0291c262fdd1a14cd30ea8075804dd66"),
            (12382397, "0xc0b338fd9ad61a808a9fcea24eeddcc89f96068b"),
            (12382465, "0x8ac760b834870a922970248085d71a683502e238"),
            (12382474, "0xad6d84516cbdfcdea2776afee0d17d187b47d170"),
            (12382589, "0x0ede8afc34e7f092f27eef121acb4de4f2e010cb"),
            (12382747, "0xfd8848704548ee535dc1e5d3a3e2fe1d67b31892"),
            (12382748, "0xc92b7c2dbda7de301fb682a9da80813def952a2b"),
            (12382841, "0x6f7328c2188b0a61c6d76165c272b8cb2dbc2f94"),
            (12382852, "0x95b70b320819b1566c9a00626f57fde85b402ea4"),
            (12382900, "0x91ea7e4dbec6115b6531dd90456b23dd83ed7b85"),
            (12382962, "0x0b17bce10337ac829c190a94add5855be588ebcc"),
            (12383035, "0xf8147d33614168ada826c7d2227506ce5ee278d9"),
            (12383086, "0x166b33973b9090e6c91e2c8a0e92d6cfbe84dd33"),
            (12383107, "0xaf50b29da68ea7b1acf5a0df0a975657d4c26ff9"),
            (12383122, "0x4674abc5796e1334b5075326b39b748bee9eaa34"),
            (12383186, "0x655e25fed94ddb846601705ace4349815e2a95d1"),
            (12383193, "0x84ea2e00fd6c4d08e88c5f59265b79de530bea0b"),
            (12383208, "0x63f89df548da9c9808daa80b21fd9b58b7921d75"),
            (12383228, "0x8d14dd4a87707d38f5f77cc893acd59ecd1e0d74"),
            (12383260, "0x5df72cf0e6c482544235c4c19d279e8a53c9fd62"),
            (12383321, "0x48e829622d3e660a9451d2e6cb1b192e2ff801df"),
            (12383330, "0x57d7d040438730d4029794799deed8601e23ff80"),
            (12383348, "0xf0e4f487bfdb6a7a7d9384eab36aab9b3db28eab"),
            (12383367, "0xedfb147fb1be5d1a6cab1d8633f8b02b040ebf56"),
            (12383386, "0x2eb28203576937c5f67f604e8534df9c74915ff4"),
            (12383403, "0x1f8598acf74d674ea6de96263cce55f66c669fd6"),
            (12383425, "0x126b3e5bfe28244626d4b294a84c50d8a2297859"),
            (12383469, "0xb1914469141ebb6e244e75cee3f35d43bf6b85e5"),
            (12383496, "0x7507042f30a9b74a4bd76461e46f1e95b8dc6a67"),
            (12383557, "0xa92fbc3e68ca77d11d4a8ada98e1ab8b9b7b6fc1"),
            (12383639, "0x98d7363bafb38209adf42813b0d4e0b9fb9936c6"),
            (12383685, "0xd99a73f743eb2821ec030acde00fb3377bfca064"),
            (12383820, "0xee42801737c98572acfc3573cce7a5cd79568612"),
            (12383823, "0x851907e244ee2075d985198bc72dbd44ed806230"),
            (12383860, "0x346d0390e18b241ef51cb5a080e2bfb2ddf8e5e0"),
            (12383882, "0x05f44e07c2ea8c574dcf94cef651a25c52557da5"),
            (12383921, "0xe288137d9e4d51eeab30bc30afd0f2da85e90acd"),
            (12384023, "0xda7dd9ad1b2af1e50c73a0cc5d23ec5397478763"),
            (12384076, "0xf5a9cf16bf5be5dc9946953ce4bde27ff8a0a478"),
            (12384150, "0x03dd2c88ae2e13cb4e47c1c85a57693542da28fd"),
            (12384166, "0x1e672b232fbf74639a9c3dc98525cf7651e8a4ac"),
            (12384242, "0x489cebe6cd5dc5dcb7047a1f0d4f358a5d2fb295"),
            (12384274, "0xab2c0a53f1be637d149e48f5e67071a6b24a6244"),
            (12384291, "0x41610532c101e3c83758c6491197884cc9d45ce8"),
            (12384300, "0x36e0fbf33c9bae7350353bfd7ed759bbb0b43415"),
            (12384318, "0x4c1e835df120716bc6f615c393932ea1c466b2c2"),
            (12384374, "0x02a30e41fb3180d0a3561f1f1742fcd4fbff9d55"),
            (12384470, "0x9c3e0e151a495c4921a5412241d29520eb5786c0"),
            (12384480, "0xb7feb8443c91a324317c15b50ebd4a37828920d1"),
            (12384534, "0x839461916fe1bef1bb5909e0b55c35d28c415bc8"),
            (12384553, "0x9a9dc10214391dcb2dd959bd8755d75b19847871"),
            (12384570, "0x00323a300261042dd5d697e3f92a06279cc7d15b"),
            (12384612, "0x0e6e9bf385cffa4b70ace38e1885e6e1730da54c"),
            (12384623, "0x8bb478f803026f62a70754a996cd37fc4a05c77e"),
            (12384641, "0x6a96e0640995d6159d1ae835d7d1b8f22702967f"),
            (12384666, "0xf4aaee4e6ffc79252f131635a0dbd6d630c69c12"),
            (12384707, "0xe1d92f1de49caec73514f696fea2a7d5441498e5"),
            (12384780, "0x0f397ac1b8fcfe10909c2130d83f798b8fc18de0"),
            (12384808, "0x66f8ab85f18f3281a2b704d3c734e7211d21d009"),
            (12384826, "0x8eba4113045b258fc5bea1033844948b3d333ef9"),
            (12384860, "0xf5e4a4480f5e73cfe697384525d6f4a07617ac71"),
            (12384946, "0x56ea002b411fd5887e55329852d5777ecb170713"),
            (12384980, "0x1415db6d7f91e79d1b03d179673fe63a6a6bc0b2"),
            (12384982, "0xcef7f42e1f5737c9c2a0530dbb05212b57fb6679"),
            (12385019, "0x3cd80800a1403a96c4c8aa28cc291cb8cd6442b3"),
            (12385020, "0xc7f1498df3f2c1d5adc614a24ddbbee45c047480"),
            (12385072, "0xf7a7309b3c2e1ade65a11e8ec65a6062c2d754a0"),
            (12385100, "0x5f4bb61c9d736b17c0dedd8081864d56484408d9"),
            (12385135, "0xa75ede894aa5e767674dd8043109b90ad4637b6f"),
            (12385230, "0xb6abc159fe86885f8bdc88a42ceebe9eba4acbd5"),
            (12385238, "0x957ea2c0145da5cb7959b53ca61295451cd8019b"),
            (12385245, "0x132d42bef414a4fcbb08a444db12bb5ba6ff1682"),
            (12385274, "0x99c58992057347622bdeeb5aaada7837e2755f88"),
            (12385306, "0xc57f35251782c1532173795c73b0209d6f480ee3"),
            (12385322, "0xb4a5ebfc53230212cdcc9621de294e3176860540"),
            (12385404, "0x7092a5d8ed974a6f4bd31850b9324041861b8e0a"),
            (12385405, "0x36bcf57291a291a6e0e0bff7b12b69b556bcd9ed"),
            (12385426, "0x87ade669de2a7cfc193112b7d885f5203a27635a"),
            (12385480, "0x22a0738bde54050ffc04408063fd5fbdc1205bdf"),
            (12385503, "0xe613efc483e263e7140062e64b96c19a124ff783"),
            (12385531, "0x5f2a70d1622e8fdfe10acd11f1717ab557ef390f"),
            (12385544, "0x76838fd2f22bdc1d3e96069971e65653173edb2a"),
            (12385566, "0x22298c24aa47ce21fc315d5cfde19811088f2000"),
            (12385583, "0x5626df2d1e555ab8d66a34932aa035d6733a23cf"),
            (12385629, "0xaff1025438da93bd86427f2f9f7d06088f720a8d"),
            (12385635, "0x22878c545228663350841f0255d2b541be4ee783"),
            (12385726, "0xd950cd33195a1ad36c416eecf6b3317eccba77e1"),
            (12385733, "0x117b0cc559275177925ad10b4161c0804b63ff3b"),
            (12385812, "0x283e2e83b7f3e297c4b7c02114ab0196b001a109"),
            (12385834, "0x74baa94462c00c800f0d49f9fc56d300609b1b3d"),
            (12385851, "0x9283452a8836879a2de27e05730c1e70bdc7cf8f"),
            (12385890, "0xa639a6bfda4149a5e716426dcf42bcdb7e93c753"),
            (12386116, "0x1f91969bce00d7bd2939fbb9cdaf96d9b50fac61"),
            (12386127, "0x99e99fddb5d4c5bfeb5a5fac5a17bcbe8d0169c3"),
            (12386159, "0x0b5437da77321d2b9828eeffcf47e42d2a8e360b"),
            (12386193, "0xb892ddf9f0837999f6232ee2028eadca6ca11a6e"),
            (12386207, "0x867379d1c5580bd30d030af0ff2e5bf2f59be17d"),
            (12386213, "0xb3b48af42cfc7e35b5fbbccf63adf7d0345d96cb"),
            (12386220, "0xfb59318b2115e752352571dbfff8feedcc4d4b0b"),
            (12386256, "0x06bd9d4b079c13eab44f54eed31d60c2019646d7"),
            (12386257, "0xdc9808168a89eb03179efb53996b24c7c491eaf8"),
            (12386276, "0x9558b165cb35ea5819340e5f5d84c868242c9ec1"),
            (12386328, "0x6031c7e147bb7d6191bd264aae4e338c797a7a19"),
            (12386329, "0x4a3be45d6701922359176e148d2336d382e9515f"),
            (12386340, "0x22a80b1365b651eb61ed90f5d732be7735efc43f"),
            (12386344, "0x8f10a53176641b4b673be37d1065e0690a5a3fb4"),
            (12386369, "0xf1dc1e314033ce484f13f8e574c8b1abbcbef4a1"),
            (12386380, "0xb2550b0bbc892e3505924c24e122ad89f89ddb65"),
            (12386382, "0xf78c7dbccbc627d15e588f686db2c479600ef062"),
            (12386394, "0x3a381e03c43938c24fc2ea9a86045c2ca1a94e92"),
            (12386421, "0x0425bf56a98294c1a5080a5a1f25cbeea9464ba2"),
            (12386469, "0x192298e57b0da73cc2b98ffd7dc6858119de6e6d"),
            (12386497, "0x7ab294c2769a2d3c6d8d89a7802e25042613b24e"),
            (12386506, "0x51b1adb7863b7fcafcd1dcd4fb3a2e2dc7263374"),
            (12386556, "0x0509f0b09beee2f61ff0afeb57bdbcb24b50c631"),
            (12386562, "0x8f0cb37cdff37e004e0088f563e5fe39e05ccc5b"),
            (12386584, "0x863229fb7908f06fc7a685d4e64327c2a82f482f"),
            (12386598, "0x27af1eae3a96cf94235ee69a99be77298d12dbba"),
            (12386613, "0x237d47565461f7a6928dc3483fedc9387b2f2c4b"),
            (12386614, "0xcf6bb54412148b157b3e087688878b56f84b1426"),
            (12386627, "0x66bf3ff8972639df2643a8ac2041186761c098b6"),
            (12386682, "0x238567a5b2930e04e0088f77846d7c981e9dedca"),
            (12386724, "0x6e2a6e92d1c0df4fbf7b35e9aebf2e681c9e6f5f"),
            (12386739, "0x05dab7a1b67f711e400f9790bd7cf8efb8f85f05"),
            (12386755, "0x209b4654b88437f79165bb33d1d1f67bd1759784"),
            (12386755, "0x60253945231436e57331287294e9c1ba0495ea0b"),
            (12386763, "0x247be0b9c1b7f81013bf9f7848ee979c86f9e91d"),
            (12386774, "0x97a5a0b2d7ed3accb7fd6404a1f5ca29320905af"),
            (12386787, "0x8e587b3c84a153baaabb551777db5740808fe8ab"),
            (12386799, "0xea13cb440f817758394a6971b935ca50adfaaeb4"),
            (12386803, "0x4b3b0daec7e180cf60409a8b388f0bd8a05fc68e"),
            (12386822, "0xe7c7114245553134f944aadf0ef2f29cbced3b68"),
            (12386898, "0x5aaa28ca43c6646fd1403e508f0fca1d92357dde"),
            (12386921, "0x11cd3862c16516d25bc566ddb73a21a785c5f165"),
            (12387129, "0xa63b490aa077f541c9d64bfc1cc0db2a752157b5"),
            (12387145, "0x924f230530e8fb5eb4db3e72c48a534e8d1fc06a"),
            (12387156, "0xeb3638255bca1509a400ae12d546bb09736869b9"),
            (12387158, "0x884981352cad4bb490818f302ee5fba489a401ad"),
            (12387260, "0x16980c16811bde2b3358c1ce4341541a4c772ec9"),
            (12387354, "0xf152f10e4781c0d3844310193a2050384a5581f2"),
            (12387366, "0xaef97897fc4e284526e6eee1115344e3f5ed979c"),
            (12387396, "0x2519042aa735edb4688a8376d69d4bb69431206c"),
            (12387417, "0x78c9db2cd074d581203810f379ef53a56da883ea"),
            (12387426, "0x09946d4e4ccde2a28ef269d26d9423034f5333e1"),
            (12387432, "0xfc2fb9fe0a038364cc718f3a5755d23095af0e7d"),
            (12387441, "0x5d8accd4ad21dca0c5add32139425f21621a13fc"),
            (12387482, "0xde2af72e916dc56f9a65f3c9766262c00c030c98"),
            (12387561, "0x5f85438a3bbbd878f87c619b694be38c4dda3a11"),
            (12387588, "0x19c4723e9ea59204ab654705532a789f27417087"),
            (12387637, "0xc2bf50fb4bbfabf283718944b68c62a9024d7a11"),
            (12387765, "0x120642582b3ca5367d46171c00ba637bdd9f08d1"),
            (12387840, "0x272a46a8d4b572a267b7c90ea64874c6655b1de4"),
            (12387893, "0xc8bbbbe0586764c05969878a5e6e6b3556a9b05f"),
            (12388040, "0x1064421c4b981104505d863b68a8abb4798e56ae"),
            (12388183, "0xe3744cc992073549794de98c82152d1aefbb2175"),
            (12388183, "0x129b40b8cd9d09af0ba90aca97d3097db219f033"),
            (12388255, "0x712abb65a43a84be3a07ac05bf81760521c83d76"),
            (12388387, "0x98409d8ca9629fbe01ab1b914ebf304175e384c8"),
            (12388468, "0x4120ff31b38253ca7013d193e9ae0e29e7f2285e"),
            (12388562, "0xf55de49ba7507428f282372b1e826787adcb2bc1"),
            (12388633, "0x4440e9962b65af35f6229c7efc040fd9adcbbb39"),
            (12388760, "0x5d2f4fa55d3894a84a7e481e311d031b80b3b09d"),
            (12388766, "0x9d799ed3d83141f3e52c1d0f56f68189ae915737"),
            (12388816, "0x4c97c393f0dab9304ea534e7bd459bc46d54bf2a"),
            (12388933, "0xfa6790b58e39d8f3f7e339398d15d39656487c7f"),
            (12388976, "0xbd68aff8e4fd87659bb772197a1ab153090906c3"),
            (12389127, "0xbb58829c1d47569de1d483338ee6274fe920d6f5"),
            (12389134, "0x82841dfd5c6c4a788db25caddfdcc4530b48336b"),
            (12389167, "0xb773a5a7ee006d2675537588e3233ad37be53bb9"),
            (12389200, "0xe4bb8d8b001502ed418ebd7a5b952ade6f305b18"),
            (12389214, "0x1992930ab49a367acc9515f485fb798341bb25e4"),
            (12389252, "0x29cd1da01a0d2f0ae84615e692d35971d52c2ec3"),
            (12389254, "0x537d64cdfd58376a5cf3c4807933cd647f35955d"),
            (12389289, "0xc11edba594a22926012b2b718a761bc5bc825ce5"),
            (12389504, "0xbfa31bfc9d4f1af0e0b31047621a5eab0f0d69ed"),
            (12389608, "0x97f1dcd123e8b89f54c74e67c904ed4f225fc2ae"),
            (12389667, "0x42e49c21e55a1f393a1b920bef2bc3cf8d022a23"),
            (12389703, "0xbf394c31598146d9ea3ee9bdb565f42a019f3ab7"),
            (12389709, "0xc542bc2b0740ceb61b63fc901fc853f42802a6af"),
            (12389778, "0x3bd3145047743b00b63050fd54b43d1e91019aa4"),
            (12389797, "0x3a5dbafbac514a0b40dcdf3bc3ff7763da9d2d37"),
            (12389873, "0x3f1cc52dda25f418250297afaba46fb179e6a410"),
            (12390007, "0x3730ecd0aa7eb9b35a4e89b032bef80a1a41aa7f"),
            (12390010, "0xb2db88b3823addf21fa274e570ea4e08cde4b210"),
            (12390016, "0x1803b7d43a17fe47b2ea70707e9ec8f4d4e5d7b7"),
            (12390027, "0x0e2e8d0e47a035d5118e6c5bc0212efa0bdf8504"),
            (12390082, "0x6fe9465012c38e3fb66df8894348967d5b48260d"),
            (12390239, "0x88977729330e55aa7111fec4967d8a561ac7c741"),
            (12390268, "0x87b1d1b59725209879cc5c5adeb99d8bc9eccf12"),
            (12390273, "0xf4bbeeaa60ee7aa94b1fb340ff77e6a77abb5fd8"),
            (12390278, "0x312e2e71a15df6f0a46385242fae95edad67c2f5"),
            (12390315, "0xaa55e92801fd60e477bcd1d997a8b6994e16d970"),
            (12390360, "0x5e8e2ab939ba8dd40f9895594aa81d2039623190"),
            (12390399, "0xd57a0bec7863f74f8bc114c4acc19f1da915043e"),
            (12390408, "0x9ef116ae7a058a41ada38afb2d3dca0ecea6af4a"),
            (12390467, "0x94e4b2e24523cf9b3e631a6943c346df9687c723"),
            (12390470, "0xb37d34c844dfe5400507ba5dd5b4a50dd8da9145"),
            (12390519, "0x79cc95d8ca2c80d3358fe17520f50b70467a6a28"),
            (12390573, "0xe3ee7ca56b5ae12876df64dc4017c873f91bdd6e"),
            (12390593, "0x6166b56a71b02b2136fe9b7f4ccef16cded73ff4"),
            (12390598, "0xfad57d2039c21811c8f2b5d5b65308aa99d31559"),
            (12390624, "0x73121e8141f3645cee435fbced27259f31fb2145"),
            (12390654, "0x3209c64bf470fafecb8b87db3d8ac1baa3ecf629"),
            (12390674, "0xf284aae87a0f10c6aa5eaf51eadcb2d736a448d9"),
            (12390773, "0xa32650483e4b5d51416c41a3870b0e4a53c7e09e"),
            (12390778, "0x306e12b60ad55cf2aea010a3b47b5a5d2df4b4cb"),
            (12390790, "0x88bbf71ac44c061afe891ef142854fe07f74710d"),
            (12390878, "0x2d7ba71dc3e391988bb7f356d07dacac92b03e5d"),
            (12390942, "0x6ad47733726c86d0622188ea2197945977be3f54"),
            (12390971, "0xeec50bcb168b66099ab00f6fdaa807ecb484c2d2"),
            (12391076, "0x903451f625b5234083278f233684880a8f139188"),
            (12391084, "0x89f0c691af9edc44cfe7c88cc6952841c57fb3d7"),
            (12391107, "0xce198ddd044c411ed26a67c90fa7bf62c9d34177"),
            (12391119, "0x27a9ff745cf1dd366d94267cb4ade2350588a187"),
            (12391188, "0xa558d7b6503ad84b1127c28921264bff1b5c23ab"),
            (12391260, "0x802e79ed746316e5ed7d2cfddafcd87ade96f7ab"),
            (12391367, "0x8ec83cb2ab2727ac16ab89d825535f7613c11e4e"),
            (12391411, "0x744159757cac173a7a3ecf5e97adb10d1a725377"),
            (12391542, "0xdee6db630156b24fc4b20379a03774eb156b9dde"),
            (12391701, "0x013037f7e497beb2903e9b73603c6ae30648dbf0"),
            (12391742, "0x88788f56a200a7c9dc1add13d10de60341d96f50"),
            (12391893, "0x479745658a6aec2de6318273944d9549457ba813"),
            (12392093, "0xf90321d0ecad58ab2b0c8c79db8aaeeefa023578"),
            (12392265, "0x80dbf454431ba9b64f0a8753f72a86afea6ee2f3"),
            (12392821, "0x9afc970774f4eda981978c94dedb9392a813ea5d"),
            (12392841, "0xcbba4ce5fd64a6b18b305eb7ffeec8d46257c9db"),
            (12392895, "0x1d8141fbdab8d3ee3517f31d0886b37875d9926c"),
            (12392951, "0x0e76293a9ebd178813fc61b87ed3ddbfbfc8d22c"),
            (12392955, "0xaf22155e1e5a57aea8b8a68172f789f4a227d6ec"),
            (12392961, "0x59a9cf0e11409d7cbf9379dee80d06b2aec74660"),
            (12392994, "0xeb0f69a396a5e04e6f232606ab7da6ae3b2876ac"),
            (12393320, "0x81ec0cbe58bc6df61aa632dc99beb6f87f0e2a17"),
            (12393338, "0x61c7f3ab62496d58fa1e7a568d88eb2f49805d88"),
            (12393416, "0xa53630cdea79afb08ee293b319ac39db4f61988c"),
            (12393679, "0x7ab6b2b49e1ce8cc10f06d2ee3a0ab0d1f4fa489"),
            (12393779, "0x230d6ef3fa58548c8d42e452440ceeb2d43b21ae"),
            (12393831, "0x3658820488e9ed3e4ef9433c5b539c06c6d24b07"),
            (12394133, "0x9d918fbf7dba01482f7431ed6b3522eac0e30ea3"),
            (12394436, "0xf3037437a8f5e5025a08af0a54424d029684b2f5"),
            (12394492, "0xde05e21fa6e9f04ddd5fd1876a523369949f07af"),
            (12394748, "0x9675f70c9b4bcc64b54656c57a2a8c8474a6847f"),
            (12394972, "0xa666a0f6a48b26a309a3e17c65eec509ad91c2d0"),
            (12395384, "0xd9a50099d63ea00ccd770cfc41075e7670bea6a4"),
            (12395386, "0x28e810d4b33ae24fe6f721a21e42230bcb019da1"),
            (12395545, "0xbbdf8c6f272a65fe0294ef775b463b43327190d9"),
            (12395597, "0xf152fc91f7bf2a32c16257e796e79fd8f318a523"),
            (12395606, "0xb1555d2c92987236941b269746b03525b6620822"),
            (12395613, "0xe07c687cecb3caf246ded3240d186181d5eae705"),
            (12395621, "0xe40a2eab69d4de66bccb0ac8e2517a230c6312e8"),
            (12395630, "0x5180545835bd68810fb7e11c7160bb7ea4ae8744"),
            (12396066, "0x93be491d835be1eadda96fed57177220d7b6cf54"),
            (12396665, "0xa1c129e8e093f935969be783790ccae472f8ba1a"),
            (12396690, "0x35548c4af3fcf16b7ccda9031f4df63b7c1bb1ac"),
            (12396912, "0x02d436dc483f445f63aac45b37db0ee661949842"),
            (12396969, "0xc0a9969939f0bcaf9efedbf9ac63b06fdbe04e66"),
            (12397037, "0x418eb2f342d8189d480506ed814bcdf8cac52254"),
            (12397369, "0x3215ed67831a260b2a1d1e0652de082fa85ef4d5"),
            (12397696, "0x1f81f2919306bbfe26d7b72776fa77984aaf4d5d"),
            (12398078, "0x9fbccf078dce4c8561e9990efa3f971a4c2fa2df"),
            (12398460, "0xf2c3bd0328bdb6106d34a3bd0df0ef744551cc82"),
            (12399309, "0xa51cb304c7fd3d0bc4d20d6c703b99bc5a806fef"),
            (12399335, "0x994bd5a0c8ded978beb9444da6210c79d372a06f"),
            (12399405, "0x5c5a53f215771742c711652fc375ef31d592dcab"),
            (12399543, "0x60024f3b6334cd490e84971ad5ac42b1346abd9b"),
            (12399631, "0x4e0924d3a751be199c426d52fb1f2337fa96f736"),
            (12399834, "0x1af92176843ac84e254ee9c5e839b73f8c7beedb"),
            (12399840, "0xd251dff33e31bb98d5587e5b1004ff01a5a41289"),
            (12400074, "0x8d4322d9a275e7050ecd25a59a28eaa50111bf2b"),
            (12400201, "0xd48e3d2bb8ab61b297a41d61fb13f8f487fc484e"),
            (12400240, "0x64d044a25c6d4b2c2ba3f868c6293fbf2f19d22a"),
            (12400768, "0x6582b944069f6c45e0ea05fb87b0bf71bfaacbfb"),
            (12400991, "0x83e32c25dc01946f4e14f49ac9e94f8956bb8658"),
            (12401131, "0xd01c8bbbcbc50cd16d7aa6ed7be995cd02def90c"),
            (12401137, "0xbce0f1bf144769288be1b2276c812d01c97f34f5"),
            (12401154, "0xe75b569cab0c82983536cf470c1663fca00a5f68"),
            (12401332, "0x3e16d0a4fbcd40fd5534c90ba94037175a091567"),
            (12401457, "0x3850467e46e0da25cd355956f32deb44b639cfc5"),
            (12401490, "0xa2fd4b2742be1f4501fc2bab457267654942180c"),
            (12401545, "0xf10c14b04b94cefa159845b19c2b70db5d868509"),
            (12401565, "0x481bdc1c54b9992052ad294f491dd7005c3253bc"),
            (12401595, "0x0710bb2d0110463af338e60755435762c73e514c"),
            (12401639, "0x167e9162b05f59e403b0d5022d6dfba4760879c9"),
            (12402040, "0x68df70cde0bdd82daa40edbd9b7973db3cdb9e64"),
            (12402372, "0xe9baec2ac8791a9c25f5abfbf79b2ca914c116e2"),
            (12402382, "0xa32a68920b9fba43cbecd834ff5b5c347f9d7536"),
            (12403006, "0x55922d884f673f4d2505ad8bba157003e097d601"),
            (12403348, "0x130430502cccc91854490e596d43b09ccff264c1"),
            (12403987, "0xe82153c957dca69e799035be304642347bec5007"),
            (12404042, "0xf5906bfd9406c45061dc67f774da440d169b84d0"),
            (12404066, "0x5d938b62a0847588ef6b35ee61efc8325351ff16"),
            (12404072, "0xd386b3793217d7ecc73b70de0551e9a181069a12"),
            (12404462, "0x549e45fece31434931d59d21570da0fbea8a1cb2"),
            (12404674, "0x8417a5e665b04b1590725c8bd5215d1e1cce8c0a"),
            (12404835, "0x73a6ab5d45ea3f1a9cee2def0d6eb8bccab584b0"),
            (12404846, "0xd80aa3d6df4dd9330d2d40fb7746b2b52d12aa53"),
            (12404979, "0x4c1d1c02fbb00757eb55f1827323cc9c8f1d43a5"),
            (12405095, "0xb8c05b7ca698f7cfd9b8a08f177e0ac5f2696bf9"),
            (12405159, "0xdce9cde0ccc3493eb6626466d490fa75aa29f23e"),
            (12405207, "0xc9a922f8ac952048f14eb18be1896804582d68f8"),
            (12405633, "0x3afdc5e6dfc0b0a507a8e023c9dce2cafc310316"),
            (12405905, "0xe1fc415f87465b024ee62f55ef33d8f822705b5b"),
            (12406139, "0xb0a1b367267f711f049d05c3eeecfc3e42d9f8a0"),
            (12406248, "0x70e61de63eb3229f2ad8668a8cbd87b7a87b5f8b"),
            (12406440, "0xdca18ced4f3b926d81046ba4217f289e4893ef13"),
            (12406477, "0xe8e98c99609e5fe9c65b0f187f05df2fa9965d7c"),
            (12406636, "0x6958686b6348c3d6d5f2dca3106a5c09c156873a"),
            (12407164, "0x629cd0665cc0a181ebf9bc7db87332a84158edf4"),
            (12407941, "0x68082ecc5bbad8fe77c2cb9d0e3403d9a00ccbc2"),
            (12408172, "0x65629a9fd808cd617543876711134012cf54062d"),
            (12408398, "0x4b1f895066058662b9fa885e87a4e4159be0798a"),
            (12408480, "0x796d261132ef5f278b377257142b8934e0cf36f8"),
            (12408782, "0x501fb4aace823a3deae46bdaa0c649cf5c51f244"),
            (12408910, "0x861d75271e2cde18d927c5170fbf22bf472a7d1a"),
            (12408995, "0x32bb7b72f67a93e18373c4b38ccc3b812e9e6285"),
            (12409513, "0xecfb71f5d7130fb38706be4a8144ee612063101a"),
            (12409538, "0xc49aff77e34cb0f1ae4450495d64209e41e8572f"),
            (12409549, "0x0a08bf867bddf555f4aab644dbf0dbdfbf510faf"),
            (12409597, "0x8a8ff27e80692444e2f7fcee6ef41e1917ff804e"),
            (12411082, "0xbd583b87880f85021d118b7ea1c85436148bcdc6"),
            (12411141, "0xd04e4752a3539a59c04dca93b9fb3d77d8c5caf0"),
            (12411707, "0x784479024251d7f3a6437346b5962c28c8e45061"),
            (12411995, "0x1d21624811e8c108ec8986c7c37f8cf5c99fd5b4"),
            (12412409, "0x6737b923283f7589efdd3c483e4c0a3646107f51"),
            (12412514, "0x4cd75e0f69cc0def47bce878ced15c24ea096e10"),
            (12412630, "0xb0a115d33a93b6ee11c8c0539e30ae648aa5753f"),
            (12412642, "0xb0cc75ed5aabb0acce7cbf0302531bb260d259c4"),
            (12414246, "0x775d0c18b291b889ab3d7f16338183bbfaf63f7a"),
            (12414448, "0x875194c57bd19407b0db8709607ebd06d02709ca"),
            (12414460, "0x10581399a549dbfffdbd9b070a0ba2f9f61620d2"),
            (12415201, "0x34f6783aa96d88496d1cd3f161a8a9d870cb9f85"),
            (12415305, "0x6a9850e46518231b23e50467c975fa94026be5d5"),
            (12415422, "0x399edf09888344743ea45a540e3c0543e437d153"),
            (12415424, "0x11a38dbd302a30e52c54bb348d8fe662307ff24c"),
            (12415528, "0xfbb358b2274388f8395c5cde478375cbeeb32abc"),
            (12415655, "0x2ab927dafa04e163530ff1aa91444279f89e24cd"),
            (12416012, "0xb88fd0e707d692ebaff2073058d15748d4082009"),
            (12416058, "0x62495596732c1a899e13f4389d09dc54fdc485e3"),
            (12416256, "0xde2fafca72f43103078435f707cd5c6b7c1aeef2"),
            (12416799, "0x2b3f164b890585e0191a852ac531e0f3d7cdb728"),
            (12416834, "0xf0a08a6814cd06a5f2e3adc867e113f1bbbe327f"),
            (12417053, "0xeb7a16bc831ca1dc20365597381f8289f99b09b6"),
            (12417299, "0xe086085b2cceec534d65fed8e59854c2dd66f741"),
            (12417484, "0xb6746400bec6d6bef46fa3d331c6c604dcd7cb64"),
            (12417559, "0xc75ba9831e713666e2ba1d11952c95011c961573"),
            (12417759, "0x3c1228f11ee2e716827eeaee844d50febf0fe775"),
            (12418047, "0xf25cb7f26b3bf82b559c6c998a7afd9cfbb831e9"),
            (12419213, "0xbb6642dda99c7fb1ecf310baa3477fa7ba3e7c82"),
            (12419366, "0x603c0e9a12baf6e96124bde1e3f440343c6eaaf1"),
            (12419580, "0xa343fdac58ac0f8bae2b917792ee5007ca50c767"),
            (12419766, "0xd966da62dec253128dbd16a4034de6e051e1d2d7"),
            (12419798, "0x92624da803a3e90f673c73195db8830f08219fc0"),
            (12419947, "0xda4fa998305e832cb9b9f17da54dbb06e0a9cf02"),
            (12420208, "0x4768f4dfb46b26889ffaab881945bd7d340505c2"),
            (12420371, "0x4a7156f612b9d883a368a8032bbc48cdf9dc325d"),
            (12420414, "0x235c74836bf329a8c4172af9d07173f38026a49c"),
            (12421823, "0xceee866d0893ea3c0cc7d1be290d53f8b8fe2596"),
            (12422006, "0xaadbd2237d00453e4201fe34930827bffd1da47b"),
            (12422176, "0x2ffc646b784732687f647e4f20a62db04cde6be8"),
            (12422503, "0x7245fb24c071f7d4a315239096395a466ac39e88"),
            (12423355, "0x4f2f3bdf115539a78a30769eb42489bd0f9d47da"),
            (12423478, "0x88f1c1c094884450efcd252f3a574d64e0b1290b"),
            (12423488, "0x923e9e9e890a5453c13ce4ae4c2bdb898eb0b6ba"),
            (12423527, "0xb45ab11ae9a63ff8a0ceeb48545f3639726d1422"),
            (12423639, "0x07ed78c6c91ce18811ad281d0533819cf848075b"),
            (12423684, "0xa9231b30443a2eb39a4a2d5ef096bf157f4dff61"),
            (12423937, "0x0ac0c1c8e854bba874da31f566251f757982e97b"),
            (12424185, "0xbff03d0e976d59db412ba44f57bdc6c299154611"),
            (12425018, "0x64a0d84d1c1b92fdc1149ffe3cee5e2fe0febd8e"),
            (12425096, "0xb702ead3b0c34fddfa9b4229c981d56a868a5164"),
            (12425211, "0x4b63963d9a331586ea94a993820f3bdaa474bd59"),
            (12425256, "0x366c35115289df55e25532d0c624a5e6a6cacc5d"),
            (12425439, "0xb34348fb650596b5e5784ae63dade893674a38eb"),
            (12425443, "0xd203036cb2fc9ed81e16d87e026269e68dc1e897"),
            (12425707, "0x9c729e49d1dfb269f3909dfd3fd2b6fcc8c800ef"),
            (12425721, "0xd0f3076b1f9384ad50d1c683376d7ac70d1e2995"),
            (12425960, "0x1c98562a2fab5af19d8fb3291a36ac3c618835d9"),
            (12426461, "0x5d475ce37b1d98127344132fadd80f5c5e1e1931"),
            (12426474, "0x6f093036a5eb623a7e07be17f21f72aee543bec1"),
            (12426622, "0x04ed15a29addad1e8189026d17879403997050a3"),
            (12427030, "0x3fdb6c0073fe81dc662c8a1a418aa3195f4a75cb"),
            (12427148, "0xaf585783337434fee47c9abfbd737156f9a84dd1"),
            (12427566, "0x058d79a4c6eb5b11d0248993ffa1faa168ddd3c0"),
            (12427624, "0xe09e394be1ffd85da23f00749f0090630b701ea3"),
            (12427659, "0xaafac42f553bbf80862fce24dac292eec5173f72"),
            (12427731, "0x664b92ece366db41042942c5ca41e3398078dd63"),
            (12427827, "0xb703259778a7045a1614990bc451bda48ca9d6a8"),
            (12427832, "0xc79bcda461456511e1c6a6916321a5851817256d"),
            (12427957, "0xa2022d57c52fa17d75c6707585206293d54304b1"),
            (12428017, "0x699c2e8e64624df5adc56599223692c7fa1b8630"),
            (12428401, "0x955da6a5a887439ac9f19d76010bd7534c312289"),
            (12428596, "0x80ba2de1ced9de57366514d9a797f3e10884ead5"),
            (12429078, "0x199621a88b58e96209bc0dab3ec8b8f3cf7b4626"),
            (12429139, "0x9ca37e40aef2686a6f44cb4e2708849231ef3878"),
            (12429204, "0x0cf7494c9de661467403abee8454b3bbf0179a84"),
            (12429220, "0x38265fd46cae892f03c65d287738f37940c71812"),
            (12429301, "0xff79e89bd6d2b043d949fbe9317a5a2d6d5c35fa"),
            (12429396, "0x42828ac0ef0591081478d4e6a06a4b92695f8017"),
            (12429573, "0xedcfece1060e568e10425e95e35b179d5b5a2b5f"),
            (12429593, "0x290e1d74eee046d85dcc789ec555196f2d06a1d7"),
            (12429726, "0xfa18f4ee096cd153e6caf601c8b63bdfc1a0e60b"),
            (12429787, "0xb1608e16609a7ff3ac5b0da49a0539bb0c3c3d9d"),
            (12429893, "0x389baf82665869cc1b70a1d33628ea01fa22a050"),
            (12430067, "0x82743c07bf3be4d55876f87bca6cce5f84429bd0"),
            (12431054, "0x7290a210537ce51caf4e8c2403b81bb571c46937"),
            (12431061, "0xe82a000e2478c882c38ff6c3e13fa454bc561dd3"),
            (12431216, "0x93bb1bed3e3f65372861dce8cc7a6a792c09d33a"),
            (12431516, "0x04e08fd3503c971d510674b2bc5c9a17d4760eaa"),
            (12431604, "0x16774c080ac2d2f376fc1e449cf6d98917760664"),
            (12431660, "0x5252febfc81f5c987ff23e446ee4275056732946"),
            (12431825, "0x4351825d4b6b92264c76ffa10b4981074fa01326"),
            (12431841, "0x2c72e4c8f2601f71769de5bbf7ca252be910e7a6"),
            (12432114, "0x575700a2191097f626b651de014487cd873c99ea"),
            (12432190, "0x09466eb0209370ecc80e1eb1d1c2d4905af9c09c"),
            (12432307, "0x1465988655dd229d3404b74a966a1b18dc0a12b6"),
            (12432345, "0xc66af8bb2be8722c6151f2215a94f19580b15090"),
            (12432629, "0xf1ffe180e7b5211ebe0e2b3b2e5bf2d38d996d79"),
            (12433068, "0x3d71021345aed9ffab1efd805e58aec9c857d525"),
            (12433519, "0xb10adb35ea2f2490eb338bdc11dc723ad40f128f"),
            (12433880, "0x87d1b1a3675ff4ff6101926c1cce971cd2d513ef"),
            (12434557, "0xd0af1981f52146a6939385451daea0726e13a484"),
            (12434565, "0x952a4d899d5a61ba4de81f8e2bae1a2562ec0b33"),
            (12434950, "0x7c911d97315eafe8ee487985e4d2f21bcd6a2a0c"),
            (12435094, "0x4693456599a8a4975862a0e720c5de7e1d09a1e4"),
            (12435245, "0x74dacd53fb4e285afc129edfa2e391a227310b19"),
            (12435579, "0xefba6ba69c82776cdd279f74fbfee1cf4e927aae"),
            (12435667, "0x2c73ab8ee6c3aebd692cc9937a69ca9811b027cf"),
            (12435815, "0x90318da7c4f9d25d974315ce06785b798d43e863"),
            (12435956, "0xc485b2757ca69e83998bf4543833409bc7a417ac"),
            (12436385, "0x5b5c8eb7142e5f484099853fefeaa74e56d0dda9"),
            (12436541, "0xba965e77582566967d0edebe2b590e4ed8306095"),
            (12436796, "0xed77bb9d173271889b5600ddfef5239a4fd8b4d8"),
            (12436811, "0xe381f103ec66e9c7f26fd1104d33f74960ef9798"),
            (12436891, "0xda827fe99adb2643d80fd30f750b8b96321d7726"),
            (12436985, "0x03a86da24f980cfeace0898883db181c74be9c13"),
            (12437052, "0xe74e836b761348f2782a7d3ceb7912c67361d89f"),
            (12437131, "0x0ce3e9e879b4623a902ee08a9921a4b9d8b27d71"),
            (12437198, "0x718961d3dd5abbef046ecb4407e38b7b4aaf9a3c"),
            (12437198, "0x9f769262273ed2c21caccc3ebe4e61f5b489d891"),
            (12437202, "0x083178e6adcadbda37c02c47b1101aa19878d404"),
            (12437325, "0xead9346aab5f82592138b8a7d0c6d27ff7bcd3c3"),
            (12437885, "0x2418c488bc4b0c3cf1edfc7f6b572847f12ed24f"),
            (12437910, "0x09818325afd92e866a58d3c170d19a3a1c5a1c53"),
            (12437981, "0x98e45940d0c76898f5659b8fc78895f35a39eb43"),
            (12438136, "0x8b6ed5fa776f10787f1171bdeea4f3c40974df6e"),
            (12438208, "0x3e16236decd430f4540a9cf4fe3086d7ced99c5b"),
            (12438358, "0xf9db11a26da162399bdba5ad2d4e0db3623d367e"),
            (12438389, "0x9c512f6f2e32dd8f008d57277275d541391f7818"),
            (12438592, "0xa9e2037cd3d894e8121866199fb094fafa7fed4e"),
            (12438625, "0xe729879d2a8618b8a63ba1035d881f28ad9e9e7a"),
            (12438646, "0xa8d68c9cdf743573dfec817e9c831e59d213ff8b"),
            (12438647, "0x460699d4b13161e8688d54cb074a22e164f8e444"),
            (12438743, "0x7289ba7c7b3d82faf5d8800ee9dd2fa7aba918c3"),
            (12438814, "0xc6ee20d4097bbbf84419f680953319e1f6f610a3"),
            (12438884, "0x4c7eef55745f937d1cf16fb595db1885bbacad45"),
            (12439097, "0xd81f0497def1666d95dbee04bdfb3986604d6db1"),
            (12439103, "0x973a67726227ce2747d5710eb44a53fb9abfd02a"),
            (12439227, "0xa788ad489a6825100e6484fcc3b7f9a8d6c9b3f9"),
            (12439349, "0x86891e9e1186dad61a8fc600c6055c8412ad5dea"),
            (12439467, "0x9e536d58315436a83176d02af6c564cd75c682d8"),
            (12439508, "0xa462c95042fc3aa96aedc89ca4daadb7ebd366d8"),
            (12439624, "0x95311bbcf02608c64ec3889515af9fe43aea9e3a"),
            (12439742, "0xa4a739639022750a5e672832ebc806b0ebee8c77"),
            (12439756, "0x145184ea99ab13148103e89c354f48981c5b2096"),
            (12440310, "0x408a3962d014d5bb9ee2e4ba97c2366d11751db8"),
            (12440626, "0x24c11c61192e65206656f01da8db83d03beabeaf"),
            (12440660, "0x2d3580c233e72762f208880fd12ffb97da0ea192"),
            (12440732, "0x9a48a62a271fcae163cf9000b3c236a1c70af3cf"),
            (12441135, "0xf51df2c10678d13c50768f2d4c7f1bc12a53f08b"),
            (12441634, "0x9f202546114e92ccdebcfb6f3ec82afd699c68d7"),
            (12441649, "0x9992c7e0ed14931ddd5bb68e5649c07b13267cb1"),
            (12441688, "0x0ff3164c4fcb2bebc21cc25ef4bd5743b893f464"),
            (12441809, "0x1d6db07fa413c6d06b448e1129450872e832d73e"),
            (12442102, "0xec6a368f33ef9a60bfc8aeb61ce24a44c58c42af"),
            (12442162, "0xbedb3cf3cf26561b5588c85ba0406028a8628570"),
            (12442270, "0x655517d8f0dad78641982e91bf4889f6869bfdf7"),
            (12442310, "0xdcfd909ff9aee6f4510f85bab79a1356d43ecfd4"),
            (12442323, "0xd442e7f7cb6b5dbc1c6bd6aa7421748985235060"),
            (12442467, "0x9bb035ad2cd158746bd61316d75aa5464171df02"),
            (12442493, "0xfbce8ea43a575359f4783b6801ebf7bcee5762f6"),
            (12442542, "0x7c7acba9e3474985c17f99430a6807c5ca29bb4d"),
            (12442634, "0x53614b950b201b061df191279055f54a9f79c341"),
            (12442663, "0x3bf97b3eef6113da780c037638bf49836d111510"),
            (12442892, "0xb4462a4f2ecef1638e564e2d923d0e46008836a1"),
            (12443053, "0xd65b707c8f52452efd045f6837ccc1fc9d18f001"),
            (12443326, "0x599a690e33de75ede4c57fbdf72762d3840e342e"),
            (12443612, "0x1d38109f42813aa5523b252d731a342053307e3b"),
            (12443614, "0x0e330653bcca4ee7e7a5bd0738efd0c2999cb312"),
            (12443616, "0x2cb0d23b8ba8025faa4229723968e6ca6d39a963"),
            (12444306, "0x9c2dc06da074919537ebf0bd1e85fc815dea797c"),
            (12444357, "0x3f9e5ff155a04554e674f9e2f468dfd65142f672"),
            (12444506, "0x2b2e3f44d3c2a4f3ddef3a802edfa891f8111444"),
            (12444700, "0xb8fce31f3dd2f0de6823575f1d35da4a306a5777"),
            (12444815, "0xa49e0bfbba7dcf10724d5b3d9c46e63d4a787098"),
            (12445084, "0x64f701f93cfbe2cc5d4886f1a514eb1ac02543eb"),
            (12445176, "0x5f7f0693540262141c37addd480da18937ac8a23"),
            (12445377, "0x4cd7e6fda4bd2abb28ab05102809593540e8a9ca"),
            (12445433, "0xf0be3301296bd4a90a3b8cef9124cf40ca56e5b0"),
            (12445638, "0x402d4865f9bf101e4d06e0bd565a8e85118520e2"),
            (12446176, "0x732ff409632c700dbbdb8843660e57856790a4d0"),
            (12446577, "0x9daafeccd7bc0cf075ef5e01346bb73e9c1d39ac"),
            (12447058, "0x0fb5c07b7b261bd64083620fe4cac7fe91765983"),
            (12447083, "0x39131f6ef397828bc3c855fb5cd50ed92e86eaa2"),
            (12447152, "0x618004783d422dfb792d07d742549d5a24648df2"),
            (12447181, "0x6ea257205a75924944549ffee6f78198907d1603"),
            (12447255, "0xc3d688ff921e73dd7082a9d4915c9f9600c135ae"),
            (12447339, "0x2ab8631f0dfd59b7f245ff9fd95c0df9cf6649f1"),
            (12448334, "0xa8db34b9cb685474e4195c96736d293b3192b1e8"),
            (12448393, "0x2266d95f215f2dcc0a0b07a78fef6f16af3677d1"),
            (12449001, "0x9d7e5647ce3c7c2d835f2f5e82c8fdb36b0bb0fe"),
            (12449046, "0x2a4e8fe56038e05b1421924190a2555ecdd4180d"),
            (12449086, "0x2f233a1e291ea4e84933a43304714028fca7d5ca"),
            (12449622, "0x612054873fa437dd5f2bfa79133ce59b82d7b66c"),
            (12449888, "0xc687ba428c844cf91e4e53a32e0dc5d5658d2436"),
            (12450337, "0xa2fe103e557db1bd4e963dfb3dc50df981986970"),
            (12450517, "0xf25b7a9ba9321f9bb587571ff266330327bf30d9"),
            (12450582, "0x1f007c775945bb5e95e07ddc4abe2152fe404ec4"),
            (12450970, "0x943c66d78875ddeb3aede2ac081625d0a947bbad"),
            (12451156, "0x0c6e72fa46a48264800469d07d6e74ac28bef380"),
            (12451322, "0xd8351444792237572b2e5f22e4283208aca6f77e"),
            (12451346, "0x3289c15810e20ace49ad16b56a0db8d78bd10117"),
            (12451353, "0x2767a2c9014ae4b344029b346106434e9e4be66b"),
            (12451380, "0xfad9395ca492e9df6ef0d4f5184bb50d1b382335"),
            (12451414, "0xf7849d0852fc588210b9c0d8b26f43c0c9bc1470"),
            (12451611, "0xb4545adc429e628969736a460073317254a83fed"),
            (12451639, "0xf573c056da20163042ed8590dac0147a134c8a42"),
            (12451959, "0xde9e8cc0b43ef606036125e13e0f1b83415bcca1"),
            (12452318, "0x18a6cdafc1ad9872f6642d676135b2f93e81b30c"),
            (12452427, "0x429e06d1d2e640d649f1480fc82ddfb1fc047627"),
            (12452685, "0x81b5d5721cdc2bc4d90c9837c411f210bc68a188"),
            (12452837, "0xc7d6c8cc0feba14277b9367cd4b25ac587d0003e"),
            (12453413, "0x34373d19ea114f842d745628b9e7cf0b3fc14704"),
            (12453482, "0x773ee2b83e6f5427bd20befa247143551f2f64f2"),
            (12453676, "0x7de8ecba49a038f763d30b05268f2262ae61cc3d"),
            (12453690, "0x138080a0036e8c2c4c79d21e2a2c535fe0887d68"),
            (12453790, "0xecca6db8efe28b93af5abc69fa7d8094cbefe139"),
            (12453941, "0x5c6f96bfe18b75d5982a66fb27d4c34381216089"),
            (12454065, "0xa173bcfeb3f29800ec0bbd706053a5d679512cd6"),
            (12454107, "0x024e90965c2dd92ed3bdd98738803b55c73beef4"),
            (12454500, "0x5628950419c53d429e99c127807c43a4f8b5a57e"),
            (12454625, "0x1d5632c41afaae0554b1a8a8e614ecf2a4d46d14"),
            (12454676, "0xd92de63661d2e298350307a63fea2c3f1731ff9a"),
            (12454814, "0xd82cd876c967f1aee2664c1db64ef3c797650ae4"),
            (12454868, "0x01fced9420b37f9b5dee28af7e692c283d987bc5"),
            (12454984, "0xa16495105b1da7738d9aac1494459bd7982c05e4"),
            (12455514, "0xddfd3bab33c48ade88c8dbea44a2e49d9ab55fca"),
            (12455535, "0x1abeb17ac29526287824c338424e41968e24c889"),
            (12455538, "0xdf1adcc682e598e543d7709c72ab27c33c51fa33"),
            (12455542, "0x7ada233945c2e8fddeac58e87066e6a3b6204eca"),
            (12455546, "0xebfbb88116f4cca37f45b08c6502113740c5bdf5"),
            (12455575, "0xdb8256e895c3182359edbc8aa35565198e8ff4d8"),
            (12455651, "0x882e9891752d374177f192713fd591afe18b688b"),
            (12456166, "0x4947d72ba0fa09194cc1b920d30acd244158895b"),
            (12456658, "0x4b3bb4ddc44732eaf0cdadce3903bc331cd6886c"),
            (12456733, "0xe2500e970ac77a8520fd1443ba07960264ba54f8"),
            (12456758, "0xa8d0517ebcb1ecbb4745a4298d75e0592c463396"),
            (12456965, "0x2aec4724f94c0ec15869a9dc33b6929bb08751d1"),
            (12457015, "0x0f07c7163228359c7bd35aab954f2ae696c1fc85"),
            (12457202, "0xb797bbfadc3f32c6bd1f74c4806ca663a00c0141"),
            (12457269, "0xfa97d2c20571474ae1ef6783789ef7cf42d6cb22"),
            (12457343, "0x612279ae8b1daebb5c208d0589d4fb6160500fcc"),
            (12457476, "0x89bb6aca49f39805285e2246f7bed823e0fa2545"),
            (12457493, "0x4332dae6db245f91969ad7001045ef400b0f26f5"),
            (12457518, "0xd2ae10f3e847010b3f3bd1725b2ec92c747950f8"),
            (12457925, "0xe77fa8a57aa5cbf1d60778866d5bc164ab913c1c"),
            (12457973, "0x2289fdc39012fc5cd3ecf9ea0e1bf321b954dd3b"),
            (12458922, "0x1e5143bb68abfadc401360a55e8aca2fd292b673"),
            (12458979, "0x1ce30058f53b7a6cc8655c6a8e8069f69ae640b3"),
            (12459123, "0x325365ed8275f6a74cac98917b7f6face8da533b"),
            (12459143, "0x269598874ad6cb71d8550622b4134832c8d94a48"),
            (12459376, "0x7c3cae980d9cb9d1326c65bba7fc1ed0198f408d"),
            (12459388, "0xf31dbb5b43196def80e16a9355110441b65ce111"),
            (12459512, "0xc252ba16b881aad6e34a90a9e263151cec243780"),
            (12460601, "0x4efc9e2e3e77732ce2f9612b8f050082c01688bd"),
            (12461144, "0xfa7d7a0858a45c1b3b7238522a0c0d123900c118"),
            (12461255, "0xcb1d8b40eed1e1bd5b897586bd0a12575d73546a"),
            (12461498, "0x37fbebbc3db4763dc4066681ecc9390b825e94fd"),
            (12461527, "0x72981da7b90e1705bda0734da4e3ea60043c7aa7"),
            (12461918, "0x8a336a5b52cd1785ad32babc1ab62957fc8315e2"),
            (12462597, "0x035193e068e068e478f46bd703e87f580a03bca2"),
            (12463850, "0xd119ee880fcf4eff4cba49dd2233966eb217c887"),
            (12463942, "0x2b255d3785da10e1753dd4bff781c04ca3c87686"),
            (12466017, "0x50ee48cc4f3c4539970e2c54c3ea7c8fe0a19d89"),
            (12466447, "0xb6d002fcd44c024a788077a59987d8394d4434f2"),
            (12466905, "0xfc8eb3545aec5fd376d739a27811e25c15133764"),
            (12467748, "0x3ee301ed8f42d106def4f9d9730ea3367880b771"),
            (12468732, "0xd32188974f4e55e53dc8e898b068f21c3784cddd"),
            (12469123, "0x8c954d86678ee71f824a02367b4041ccc0d09527"),
            (12469469, "0x528bee00dde5d1507011ccd4d43a9835ca513777"),
            (12469488, "0x3de1a0f57840c9fdd4d53fc5a01178ac2c8f2edd"),
            (12469799, "0xeb30e8ffcbd02fd4a14a52e28cd300698d00e2f7"),
            (12469836, "0xe6f31409282902aa39bcfa06763edd46c0a995a3"),
            (12470053, "0x6241541105dcd8291bf2cc43ad19c0c2fb78e301"),
            (12470448, "0x95d3032852f7ea7d896330d2c73f6a7944c5f951"),
            (12470718, "0x3e2cd79db408061bbd6ea42483d333d7d44d229b"),
            (12470867, "0x5555b1b916e02f8d3d1a17f1bc71877349f7e486"),
            (12470944, "0xddc9e703595afc08fd5ad049c4c129804a003177"),
            (12471390, "0x07a6e955ba4345bae83ac2a6faa771fddd8a2011"),
            (12471436, "0xacf35a1ab6ede9c8e06a7001499c8e1e02073022"),
            (12471936, "0x74723340c2e12e77b78e1a13bc31a8f43ac99f0d"),
            (12472085, "0x46d3c903149a3bff0c7c7dbdb0b6c6d885d51ab3"),
            (12472086, "0xdce6e7d5d400773c876689f10a5dca6a438ccf88"),
            (12472099, "0xd0039f24788dec1386e7b2b78de3374adb721343"),
            (12472829, "0xec152a4868132e9b7e71446e6b82f61d4209c943"),
            (12472866, "0xe4b82df044511a7162bea1f11119f6d1283a43a8"),
            (12472938, "0x1f14600e71bf9ade32bf97e60a526266eb437ec8"),
            (12473200, "0x201e6fc98cda33649102915340723ab2144a552f"),
            (12473247, "0x2aadfc5d4957a4eb4b6a143adf4ec9244be70eb0"),
            (12473407, "0x58c2ae94565cc87000a62783c7915179951eb28e"),
            (12473549, "0x552de0b5d94e4187e8d0cff4fedec1aec8cf8d56"),
            (12473639, "0xd7c13ee6699833b6641d3c5a4d842a4548030a82"),
            (12473704, "0xced364623800f1ec98d8c19f9ab6f412148e5733"),
            (12474062, "0xfb3b5a0e03ae12c41492d70bd210e65d25cc01dc"),
            (12474196, "0xacbcbfc1f3febb092d9f01d2cafcbe216ebf8119"),
            (12474364, "0x0254a309f5140d457c0699e2cd0457a692a69cc4"),
            (12474744, "0xa89ae75f2bcd5b59566bde15bff02a40e9014d51"),
            (12474915, "0xf720e786b92dd659c30d9b57b626e39d92935eeb"),
            (12474952, "0xb3bb26f55f58a46a2d5887b83b978ff88067e1de"),
            (12474979, "0x058e136bc597082589d2cf5d0d021e22b6556a9c"),
            (12475156, "0x7f4c9a9fd4ac78d0c8061fb69be5de189ce98c46"),
            (12475433, "0xe15347de0c10df8da0ec12be10e1dbaea556a3df"),
            (12475459, "0x951e678454cdd1c279362683a975e0482ef23fba"),
            (12475751, "0xd3be770278dbb8f0ec34aa8508febdad5df10022"),
            (12475801, "0xad76da68a204fe301cc9b1c60f7166db73d35e76"),
            (12475827, "0x82790b21ea1fc6c84dc46abb533abe9ed99757e9"),
            (12475889, "0xf8f49097fab6b14bb1ccd424c43079ea3fa82446"),
            (12475925, "0xc9a90fd43b224c94b5fd56231231ef6b209f8b78"),
            (12476056, "0x6a2259ac6714c6dd825f7fd615fb7ecde05eb09f"),
            (12476075, "0xf2ed0fb5b032f0106b169179878dde197d08dcc5"),
            (12476116, "0x93d1df74317d18550e197c146ed29f178d027265"),
            (12476267, "0x6130a21cb799b1567b7b0e6eb0205ebdc167c655"),
            (12476286, "0x7c6c590a9d766665b97f3f26966e74f768177c6e"),
            (12476312, "0x68add60da802f739b84505aafdb6145be4d41162"),
            (12476329, "0x2f9be30384cef5d17f22a43869a32c19a59ed585"),
            (12476467, "0x7f1864750e943c4cf187c6abe3ef6c1f004b6e94"),
            (12476499, "0x83606a6ed800bec388bec1299b49d32c1f92e616"),
            (12476600, "0xccce35f1eacf0acc67e2fe5bb0d608192ccc36f8"),
            (12476742, "0x095963d48c8676946118ab9ea17fc58c445ffcfc"),
            (12476748, "0x829e48efcff5e879a8e1a789ac173449decb591f"),
            (12476761, "0xe619727a5fa81fa95964ee72f762c1ec35f2b475"),
            (12476774, "0x4439a8002234616e673d3a8768bb16e7b4e96fc6"),
            (12476905, "0xb74734c0fedfabe4dad2cfb63428dcbeaef57791"),
            (12477231, "0xc7df4f4077b956b262e012bc2d09070828352a64"),
            (12477265, "0x7565027a30fe580dfb38f61ac641444cf6bf8b50"),
            (12477395, "0x19a573b228468f3bf917389f4e2d4f2997610f71"),
            (12477601, "0x8649b95e8eb0ba864178dec435569b0417236ea8"),
            (12477691, "0xb7c90598ccdaebcd14f43fa6635c422c3c5cecca"),
            (12477712, "0xcabff00366798a59680e34c2374294a18646a3e0"),
            (12477715, "0x83d5513123e5fadee0d6f6ccf5c7609f3e433015"),
            (12477878, "0xe0ecf49e570da407d054e29626e73b1f86983b5e"),
            (12478107, "0xbcc2adccd4de0b2656179f729c98c39dcd68c84d"),
            (12478580, "0xf4020783b38f3cdb34710dc0467e5383485318e3"),
            (12478980, "0x125ee18bc63f63aceb510662a77fc122dda8b13f"),
            (12479179, "0xaf99ab16e03cf6ac85a9e3cc74f6f5820858f38c"),
            (12479403, "0xb43218b383fdc6de7c1bade113f03af4585300aa"),
            (12479609, "0xd8b55a29d29ff8b06f1e3808e059d6554c2d558b"),
            (12480641, "0xff1e17af5aaa12424de7cfcb5d6622d14c768652"),
            (12480789, "0x798f5c12c9bdece7dc9694ce244231c22a0f702b"),
            (12480908, "0x1bc393777e4088f84d961a7280ae4a4728b883e8"),
            (12481011, "0xaaadbb0d64f6d25731306f11e9ee94b486d53e43"),
            (12481254, "0xfd5434192af9e9c9074e5d7afb3a057777be3a02"),
            (12481281, "0x326d251d1e12c9431f63f148d6a0706cf0262fe4"),
            (12481308, "0x548838a45d922230a99eec8a3710ff0f8d203f18"),
            (12481458, "0x54706374f88e9908ac2c518302da322aebd46473"),
            (12482566, "0xc4c73b403c474f7bb4f0d5ccc05cf29e7fd43681"),
            (12482636, "0x16a8073d0439475f57f0abd8dc5383b1f9d1015f"),
            (12482651, "0xcd59c83b7f7e25440af205edbca76133a8fba5f7"),
            (12482871, "0x53c7c1f4646cd6e7c3913af985da4def81d0e568"),
            (12483335, "0x260cc60a32d59636671ab1d0dd83da28c89cc690"),
            (12483586, "0xd40a6add2833fff233db70bac5e7387e40108ad5"),
            (12483732, "0xb1340fe2978a4ef7d32917491b11fbd40cd0a364"),
            (12483749, "0xbedf43fe5df23e163e5a844c3f6fbbe4d77e5113"),
            (12483913, "0x2905d7276f142a7b913c237f92a937632b2088fb"),
            (12484032, "0xfe9fa36487c8fa145bb215d49e68e1a70253af4f"),
            (12484057, "0xe258f08fed6f3a46de5746454b62ea937dd5b9b2"),
            (12484064, "0xe8b977aa5a9303fa94818441d78575e0f697ae72"),
            (12484317, "0xe8c2030686fc3b0161ee1def0e8d01dfe4fac0ac"),
            (12484706, "0x5620f7b870cb69dd3e21def2ebc57fc78425cd94"),
            (12484746, "0x07d1f222360e36ce692e687907fd5d0994fe206d"),
            (12484777, "0x3a73beb6fef4bb6fc58b8d1e74d25132e1853b55"),
            (12484949, "0x21ca348fef9f09fdb79b155a75efa7f02f82733a"),
            (12484993, "0xca52b52f1ff30b75f8b5aa8c64cf9d78d30560ca"),
            (12485115, "0x3b549e2ccb3d058b46c547af9585174a73cf221c"),
            (12485144, "0xcc8bd8212c4b0f913910cd5b7ff20b53d3ceb659"),
            (12485258, "0x8fe632ead7c30f3fd6eb53b3610922e4bbf248a2"),
            (12485279, "0xc04d960757b1444bfca6dea218864d1322a33cf7"),
            (12485477, "0x6351017b912177a4bc37459521f4bdc859e846b2"),
            (12485491, "0x373ab69dcec0d8067d17805b17a01ffe4efed44a"),
            (12485502, "0x97a86cff15d1af9eebd67274341291fa73d0f204"),
            (12485726, "0xb99e2596af1a6bb67e367111f27125d8d7759cd3"),
            (12485877, "0xae85c4bf1ee72c03fca90b693c24633606dd1796"),
            (12485899, "0xb74f0303ebfe9d5c08b6f89d9baf121117cd9dc9"),
            (12486179, "0x05a075a0aa0a49f4b2bc8e7bdd1f56e4681f083b"),
            (12486448, "0xd954107608d4ab5ea1b3f6cdcd44a4da25ac57af"),
            (12486662, "0xb45504b6a5f9f734ef9414f45532aed9d2979d24"),
            (12486742, "0x08b48722792cb5d7dd81d687d922acfe4fbaf6ab"),
            (12487090, "0xc772a65917d5da983b7fc3c9cfbfb53ef01aef7e"),
            (12487110, "0x25b1c0c2a174dec7d8072b23d0a9d731774f385f"),
            (12487131, "0xf649a64f3e1c171752e2135df4b4f20649b1a6b2"),
            (12487168, "0x1ee9c46c745f06e9e4bf9630ab138fe3fbf12f6d"),
            (12487238, "0x4e30dbec2e22be07f235d2311f9b691046370d18"),
            (12487470, "0xa47db191f46a502a8294ab2bcdd0599e93ae816d"),
            (12487571, "0xf700b3083519c6e7b32910eb9ca5cb3bbfcbee1f"),
            (12487672, "0x1ded587b37852c7b8e2ec114abdf64023986666c"),
            (12487684, "0x4a19ed380b4d1714d84289a47ddb9ed716cbd2b0"),
            (12488424, "0x565a792e51302b0a399e01c7bb6e3fe0ed055853"),
            (12488673, "0xbe7cb49dfd1c1dee74c6ec1144117e1041826baf"),
            (12488676, "0xc49e7a107c875072d2125cc45171aa603ecd958e"),
            (12488723, "0x4cdd5c0780a07987b71e9e29ae2e770d8820971e"),
            (12489032, "0xada30691d6a72a0c975d4a182846095885a8ef7d"),
            (12489075, "0xe3b919d83de6332b20293faa82a3e1748913b290"),
            (12489076, "0x73f64f12a5ca29952cc31db4291a9f8dd43de4f3"),
            (12490380, "0x956c9e68e2b2f1fdb7d4eec78cabcaf832172889"),
            (12491323, "0x087a2050f5179130e5687b410a2d41633278717a"),
            (12491420, "0x8e5778ded8a7dd4000561a119b65f973158c277f"),
            (12492014, "0xec47e6bbc03579fc636d7030aba455be137dfd45"),
            (12492133, "0xee249c933d023e36dcb57e9e6addb761b09cec2e"),
            (12492277, "0x03da747c440717420d9a8b21c61805dd9d99071e"),
            (12492469, "0x4611cd31e4411f98534c0ca50ba3218ea311b5f4"),
            (12492824, "0xe24442fbdf9dfb155f76821d1bb2c3794eddd873"),
            (12493602, "0xcf3f297704585d2d946619b6a7549c7d16580cb2"),
            (12493677, "0xbf431beb8c9d6bb3f270be20c1e312c35eb18ce0"),
            (12493680, "0x5ae5f85ae12556a98bc2b0f0459e76018673f105"),
            (12493689, "0x506af98c933d1d8ebef24af7a4b80956a5b716ea"),
            (12493816, "0x447adafd849336d3ff5dba50b1d9f4f8308e82bc"),
            (12494051, "0x0fa33fa904b70a5e1c62d878838000342b8477ac"),
            (12494262, "0x569c2d4c0df09561f71655e97ede14d4ab0fd8cd"),
            (12494272, "0x733a37b3774e3c079aa39a83c93b142ebd0d7e78"),
            (12494290, "0xe55e68925809784c8234dfcf6f8fa42c3a48b2c3"),
            (12494368, "0x0adee264d34a2c2da8113c318bf823504fca4a31"),
            (12494404, "0x1bab3f9f356c00dc9fa1d2d33f953aecce529ebf"),
            (12494543, "0x1c91ba1de1573f025fb9f6f34d544cd396bc2ff5"),
            (12494753, "0x68a1b3dbdfaa0529371ac8e9d87bec40fac254cb"),
            (12494758, "0x3b3f288f1f5db1b3318103a2e8bebff464e06ca0"),
            (12494907, "0x9c33bf61f110e53b5f163116e80fd99d7a6eae6f"),
            (12495002, "0x10cb35baab0a8d6c3612083a9cf0f57df0b16e60"),
            (12495136, "0x1b52af2741bf9ff4f387e81dcc55b628a1320b8e"),
            (12495289, "0xd286c3022744f9ca8145b676dcce14dbaca7fe54"),
            (12495336, "0xfbd8d5c0a0bce6cffc85cb305dd0ab410e59523c"),
            (12495339, "0xa002b56364cd8ae52c7e1a176c088d019e54f108"),
            (12495364, "0x425493ab40625ce677141edebb63a2b61f96c454"),
            (12495370, "0x09aa844c57278a2a53821f90a4cbeffcc540e7e3"),
            (12495397, "0x1fc94aac9d7afc36a3c9dfc6a612dbe59dc28f52"),
            (12495428, "0x40fe3d7cf1771311dbffec07c6c17d5b2aff190f"),
            (12495499, "0x5b97b125cf8af96834f2d08c8f1291bd47724939"),
            (12495507, "0x9039093a644c466e43c28d06c303d751cdc07cc2"),
            (12495530, "0x11939e41dd9c5fdb319c1d8eebb83afb886a1c39"),
            (12495535, "0x8c54aa2a32a779e6f6fbea568ad85a19e0109c26"),
            (12495835, "0x683d6a62fc628d9b1ff7662dde2a17856a05e4a1"),
            (12496190, "0x7a3c548416bbe8e09ac4fd6151af2dc6957a75ca"),
            (12496303, "0x2f1dec6c83ff85030825a1d19f7697cc5e3ddefa"),
            (12496322, "0x675e2f47052f6b496f9f2f8d4b8bcd2a6c1c805c"),
            (12496402, "0x46086a4e45916e6b4ccce0c4667e4c5fc84b8e85"),
            (12496422, "0x8020eab645e44ae808ebaf93b3035f52bc633f9b"),
            (12496597, "0xabb670b47096728571672408057ac258176c3809"),
            (12496692, "0x8bdff9b816a316031d3bdf060d0dd7be80de1233"),
            (12496831, "0x67f324ae38deac3e692d5549d9e5650b37af96a2"),
            (12496880, "0x02edf020dcce4f1d7335f7ebaa46687f457c68ca"),
            (12496933, "0x714b8443d0ada18ece1fce5702567e313bfa8f29"),
            (12497038, "0xcf56b49b435f4d326467788f2c8543cf9a99660f"),
            (12497060, "0x5d65ecbe922c0f93905121ec6806c185f1ebe268"),
            (12497246, "0x971e404f2dd3a1ed6cdb966224a78ff91896ecd6"),
            (12497544, "0xb8786f9180b9fac2ef95ff88c46dcc79f0bca1a1"),
            (12497742, "0x6d24e69f24267b559a5ca93547c073a4ecc05124"),
            (12497904, "0x56166d2190026a30bfbe789fbb261c41a9fa699e"),
            (12498848, "0xc486ad2764d55c7dc033487d634195d6e4a6917e"),
            (12498887, "0x00cef0386ed94d738c8f8a74e8bfd0376926d24c"),
            (12498918, "0xa884e01d1da92fd16b972e5c2ade7c595391b5ee"),
            (12499309, "0x85eb26333831268350aae79152d56ed1cf0b83a2"),
            (12499413, "0x1a7d2432ebeb4e15099f96f78f6f9a8686af961e"),
            (12499516, "0xe9f58e9d3d3977bb74f80e71fa23d6c40b9a44fc"),
            (12499516, "0x2d2afaa7e5a1d10557cf2adea9fa6afca81a61ab"),
            (12500060, "0x7fd626e58ef2de3be03113b8e7b154e8edf90eeb"),
            (12500379, "0x6261c2dd8f9b771552228f7511c6a3977bcde2da"),
            (12501080, "0xa20ad630cee74bc834aa9b8fcb432c5c02710479"),
            (12501109, "0xfba2fe5c233db3f25795aaf5e56c0a2633b59a6c"),
            (12501126, "0xe191ec484dc78f38b2bd733900288ff0cf2ba8a5"),
            (12501198, "0x5d156f6c9f241cbdf770d138d91d044c7a067fde"),
            (12501207, "0x7853c8a111cd65f8ccc6bf9e075519f5c51e3bd6"),
            (12501296, "0x1f0a97a0b7c4783a471d883cff5e85d13fe2ef3e"),
            (12501341, "0x135422b7527dfd9746e3bf7a613a7e437547d122"),
            (12501383, "0x0f9d9d1cce530c91f075455efef2d9386375df3d"),
            (12501537, "0x38a209e1cbd71a02b4922eb4e4e5f507499e9899"),
            (12501625, "0xd73070db67ac6d8ba1c31fdf365c84eb964e93e3"),
            (12501650, "0xb07e0d9913b2dcbfa56e0b0e29ec0c61b23b9547"),
            (12501756, "0x2f79643f9fe52dcbfb90cc03f59678a8a16073bc"),
            (12501810, "0xa96dbd00206610e82645dfd1c5529591bda9282c"),
            (12501824, "0x8549d10acebaee90f2962a377067ab66b95a6ad1"),
            (12501833, "0x0068bb604413dfee5c453907bb150d0312a0f257"),
            (12502026, "0xc05bdd0c4b0d37b84207522bcc998eedc9deecee"),
            (12502047, "0x53a95f5ad1f9f4596266522934ad98fa5f6396c2"),
            (12502405, "0x9127c5c988917b879b3b9bc87d11ee2c9bdeda73"),
            (12502703, "0xc8bf304b32cc514a19e748a2905a6df1f1de5c25"),
            (12502710, "0x8780b9783cb8055acd7a86b1106ef218e749e4de"),
            (12502752, "0x39524328e8e4403785196540ef9099cb1e742e3a"),
            (12503773, "0xd5035227226a3d2dcb349374123a16308ad452fd"),
            (12503870, "0xd36dcec4b62a507baf71082bd865f88bef5e3efd"),
            (12503942, "0xd2e0add90e86079b9961fa232178049503e06401"),
            (12503957, "0x59c38b6775ded821f010dbd30ecabdcf84e04756"),
            (12504321, "0x3b8ec94042af00f99051ab2575f76e49552b5b7d"),
            (12504333, "0xe85821560b76109fd8648d1121cad678a25c7e19"),
            (12504498, "0x1ad96c413367ebb4f9c8961a751cf144c9278592"),
            (12504615, "0xd017617f6f0fd22796e137a8240cc38f52a147b2"),
            (12504698, "0x0eedddcf6e26495d2bb6fc21fd565bf20818ec81"),
            (12504752, "0x537de9477a639ad1d23bc2dcfeb8cc3032bc114c"),
            (12504756, "0x60c51a68d3c75b62f723e6a2da29c6f1fbe4b7c5"),
            (12504780, "0x06b1655b9d560de112759b4f0bf57d6f005e72fe"),
            (12504782, "0x2c0ed2a13a6ec400118552e934c8b0e3a1ba5950"),
            (12504792, "0xdcc531a543799e06138af64b20d919f1bba4e805"),
            (12504839, "0xa5273840b68110031a5f83617cd992a965683a41"),
            (12504940, "0xb203c4cc2e0d7e086d6a62d4f801137541efb469"),
            (12505237, "0xf4d686c684b2a05cbd4fccb77121b503e6d00389"),
            (12505268, "0xe2c5d82523e0e767b83d78e2bfc6fcd74d1432ef"),
            (12505337, "0xb6b965a08f298b75ca16d473d812e6a23b1d753f"),
            (12505350, "0xc378df6833fdf7ae85d71256dd189e7830a70826"),
            (12505489, "0xabd57587522f6fa7d6d77aaab8a22afbfafa015a"),
            (12505584, "0x06f171de64205e4dd881c49d57e6e4ada7c37726"),
            (12505639, "0x2e8e606b346abc508cef020a9f98ff89657b629c"),
            (12505827, "0x145265236efb291a96cc5ee69b50072f8fd36b62"),
            (12505882, "0x1c4a33aef406b7ab7f902323c989085f3f153c5e"),
            (12505945, "0x2f6c2da592ba0eac2b4536bcb670d60c5c71fd12"),
            (12506209, "0x8a1e628b7181b1c0d76a873413ccb769a6129734"),
            (12506266, "0xbe58afb4e61a13cf2ea6b6052e2e05eafe562aa8"),
            (12506298, "0xa46466ad5507be77ff5abdc27df9dfeda9bd7aee"),
            (12506544, "0x0e1284d71ff5c210d28723ab27a5d3b0b1ca1eb2"),
            (12506595, "0x099bde3538bd8c597ea627fbebec9653b1dd0af0"),
            (12506703, "0xfe7ddcf268873ef040657fb7fd3e8c09db8d8b9e"),
            (12506737, "0x2a372c76aff0b6393c12520566a626fa6810f4c0"),
            (12506799, "0x17ab5860167cf2a7d671f579cfebd651162f7cc0"),
            (12506821, "0xf74a97cc57f1691b4df364264df742e81b4c7754"),
            (12506934, "0xf4ad61db72f114be877e87d62dc5e7bd52df4d9b"),
            (12506961, "0x888a0559ba50b87df7238341e51c3ab122e68834"),
            (12507237, "0x67b62f0cb3039e2c1efc64e853db7317db4f3974"),
            (12507451, "0x5fa2aabd1401537b0f16b191558542eb1769bbe1"),
            (12507547, "0xe618d00b8ed613e3a551f2a17bb8018534d55418"),
            (12507614, "0xaac4f2193011aa80c4549e92feb68aaf647e82cc"),
            (12507726, "0x387a79b14ddab98f703ed8ef48bf59b08dffe92a"),
            (12507813, "0xb5fe2469f920986ac50d47764abccd513b14b805"),
            (12507931, "0x4fe4465943b6696d40f06b323a8a3256df452cca"),
            (12507933, "0x68f73e2180024db5b54e0e119d4f5128953f9417"),
            (12507951, "0x24924eb8c883e4e686ed6052d1a015089e6baf0e"),
            (12508042, "0xf0a1088219ad372e971e62adec376377ea875ad0"),
            (12508103, "0xc055a9cb9a0a7450a4f59b37c1e0a2f6683f98d7"),
            (12508199, "0x6fba9ef54b841dda97802b0707fa9e210cb48d01"),
            (12508253, "0xb9fc34bdce2aa7d115abe2e23fdeb3e6fc9e19b9"),
            (12508286, "0xe204a0f8d72b56ef129b913a4d737d6e800c0508"),
            (12508290, "0xb38ccd4311eda6529f5de99ea959b3884d09b955"),
            (12508303, "0xa497e3d00adedc8886f644ee6c03c3f2ee67d0e3"),
            (12508365, "0xc641bf5f45760644fd613aacdac34f3936ba4c41"),
            (12508505, "0x060cb8bc18d68f07738634d52cbc82696001fe3e"),
            (12508838, "0xec3bc0c942aac1c94e83fdc7786fcf06e95d1535"),
            (12508957, "0xb80946cd2b4b68bedd769a21ca2f096ead6e0ee8"),
            (12508984, "0xcbf3daf2e4a3be10fad16cb5a388dac56d5b3a02"),
            (12509053, "0x3fed392cde08fa170d6ef924eb886e7967fdf164"),
            (12509145, "0x0f8c3761c1fa9187de5bf1ba62db4c5976555e29"),
            (12509292, "0x2a0330c7e979a4d18e5b0c987b877da24dd37d04"),
            (12509487, "0x1eefc75cc4458e651480e80d74b263b77a93cb11"),
            (12509487, "0xdceb026bdbeabef09b823de8cb76aacb899cf576"),
            (12509617, "0x0db6a5bf8588afd9c84ea79591644e6e0474e63b"),
            (12509798, "0x77f2435e54e8e2ae492d697887f93d99d69c4668"),
            (12509872, "0x30c14f6d23cc63f57a2deb685fee466ac7a93ef9"),
            (12509888, "0xb35d0b9d64f06454aef8c6ca5237b22453afb439"),
            (12510013, "0x543842cbfef3b3f5614b2153c28936967218a0e6"),
            (12510020, "0x82b5f8fc2da06bac949180c2fa276439f9192fa5"),
            (12510201, "0x306c0cc99fac9afb2109abe42d17d0e10b36f95e"),
            (12510660, "0x3809adca4811b1ec74ae60c10df212ef85eb3f19"),
            (12510918, "0x4d64fdedc4321a23b4dff449af0e54ddd42e9925"),
            (12510955, "0x736818f46cc3f5d25f08a13dba8fde273d73e7f2"),
            (12511249, "0x0d03eebe59cacb5dd521bdeed94f7ff4ce9e939a"),
            (12511518, "0xbda1200eed6aa819c781f3b66d1312a3b703eb2e"),
            (12511967, "0x24f6d68a2524befb5d5d3aff938e6d668b8eae3a"),
            (12511989, "0xe744f5e2edfdcb9fdb43b288ecb8b21c8487e888"),
            (12511992, "0xa2499a5af023bf570e158946ca74b662dfaf31a5"),
            (12512111, "0x61b629846819860d3d0acf047f3ea741fe2af2cf"),
            (12512163, "0xa3f558aebaecaf0e11ca4b2199cc5ed341edfd74"),
            (12512225, "0xb67e4f949a8864a1dbcd374a09957efd226376df"),
            (12512293, "0x35fa1ac87b9bc3baf1ca6f0ce5f8aa560c63336b"),
            (12512339, "0xb395f092e21101a2bb8fae3892d2eef149aa2067"),
            (12512418, "0x88fa6bea2960f6edf3fdb279658f5c032c574cb2"),
            (12512460, "0xd7826bfc84252d81e173d1d63a7f37060d4cafc3"),
            (12512461, "0x507c54914ca6cf2d4d833307d0357b8c2d148a0d"),
            (12512466, "0x5f2c024d6137e888236f7ef21a8d51d2c06db224"),
            (12512493, "0xf76f1022075b757d1455b32a491b2ea27423f8b1"),
            (12512562, "0x8147aeb1cb347a781d97644af6c284003f11db74"),
            (12512762, "0x6a3eb6fe80270e761aab614c48f776066e7d6ad1"),
            (12512902, "0xdade25f6394e12f2f9cc0844efc32f2f662188e6"),
            (12512930, "0x406113a76bdedb4a0333ff3d0b80c46b3035b655"),
            (12513093, "0x3befe57059987c182b8393607418f05bd01e666d"),
            (12513148, "0x35b01400051a2c08bd90e4bee5e05b33519208c2"),
            (12513167, "0x35c349a2cadbbcdc4b3e5ca6d6e8e32e53df0c86"),
            (12513209, "0x23c4855ff903ad733c364780a27b5fadcdad2e17"),
            (12513283, "0x29ef3e403fa6ab93e0895e07914108425d099314"),
            (12513724, "0x43d9b213f37a589bd81d27f3a143dc13257869c7"),
            (12513796, "0x98138e4c348389fb315b3e5a7f3c1bf0142af9fa"),
            (12514048, "0x9c602088932d4f665ef894cab9a2c5e67bb8ced1"),
            (12514049, "0xadc211037ad0beff379329fad4569f128b6956ab"),
            (12514098, "0x54a0f2d8bd71b793d008266005dfae136500b204"),
            (12514101, "0xa31a285832edbffef40d271a4ff1180bdd30237f"),
            (12514111, "0x31d4878048c6af8efb6435323323c5af1c1ed778"),
            (12514443, "0x5d70c9d540437c09ea961c83fad69cfdc761e6f9"),
            (12514792, "0xe0fc0c4c232af34b59df65df9ba69ed9a136c614"),
            (12514818, "0xe4e969fad63afe4a425d1038dbee644b97e8b891"),
            (12514868, "0x24fac6710329aa548ebb11bd0494859905a9c7bd"),
            (12514876, "0x391db4e0559ec5c95d96638201ad2da8764f7970"),
            (12514999, "0xdd005650ac6805457f4fa6ee1816813ce815e914"),
            (12515147, "0xbc6b80422f89b0ff3bf2af01011730abaeee490a"),
            (12515272, "0xebc783ea1ffa5c602e22f31a4cbea75c50058627"),
            (12515366, "0xfebf38b1d34818d4827034f97b7d6d77c79d4997"),
            (12515407, "0x86a93da3013aa3a40e7751cd3c68f824b6512756"),
            (12515657, "0x8b92fe7e087dd51691495001fa47f2afeb30d043"),
            (12515690, "0x11b7a6bc0259ed6cf9db8f499988f9ecc7167bf5"),
            (12515902, "0x34577196fe8e22957a9be399c3c83d002dcc1e03"),
            (12515929, "0x8c13148228765ba9e84eaf940b0416a5e349a5e7"),
            (12516247, "0x62aab930cd5f3c1ab1b230f22a96f740a91eafe1"),
            (12516350, "0x6e9eeee4936d7d73933ea6409103c3df9713562e"),
            (12516426, "0x47cf431f539d6b69758887cb99cc4dd453976c97"),
            (12516657, "0xb03168f432208bceb53422e8139d8880875cadd1"),
            (12516665, "0x1cd2071c6cb5b2f87667b3d3309a0c0925b1373c"),
            (12516723, "0xed9e1a15e7c638e552e8d390567cebfe7d278178"),
            (12516731, "0xe9544a7b3f4c4b8ce5876901b88583eb7b1bcb09"),
            (12516788, "0x5ffa7c184478edb68b192c8640f603416fe22eff"),
            (12516953, "0x152ffa2ebb9c86c409b700f5368906ad0972e989"),
            (12517336, "0xac5a2c404ebba22a869998089ac7893ff4e1f0a7"),
            (12517349, "0x05aaa0053fa5c28e8c558d4c648cc129bea45018"),
            (12517386, "0x3bfe5971b11196e197ca887f3feff676e0fd3762"),
            (12517679, "0x4858c96a9024fbd7efbfe78aa5a8d62900952420"),
            (12517766, "0x62cff1c39d789fa9cb8dcc81616161f651b4bac0"),
            (12517877, "0x946dd0c21dbeefde35d8ff074090f519ae3a7579"),
            (12517903, "0xc2e9213fe0aaf5ee55e4bbe665935c2df94af13d"),
            (12517996, "0x10f97d4b92590410d7c3951f0888f132ca6d157e"),
            (12518018, "0x98a0c138957cf1781485c67857257291e99dcb9b"),
            (12518043, "0xcf7c26b97b2450d21bc1ac02f41698bff6b9926c"),
            (12518054, "0xe189220d42b3adf01508d1bd3aeddbcebcb6cc01"),
            (12518239, "0x500dcb12218a66b4aca4cc246af1e4003d02e042"),
            (12518432, "0xeb05ab65ae6b24e51ddaca5b7cc4ef63a9914637"),
            (12518436, "0xddf05c1b96c4b8fa9cd6c5abefdc137805b02569"),
            (12518476, "0x595cd204464e3ddab4d72aca79d4e72fcf2412b0"),
            (12518537, "0xa8322f6d07707fb57bd111f6e98dc158b4186761"),
            (12518696, "0x0668e89f625e81c7c6c13f9aede576ffdd6b2e98"),
            (12518772, "0xd1b9cadba2c28ac454f2253e4ad67b6672c7c4f3"),
            (12518820, "0xaa36dbf3fd5bfcf1350cb8d830708f28de37ae53"),
            (12518936, "0x93d6da5d307e68a770ab9566ac521d197f1a7123"),
            (12519127, "0x2c4777a8004bcecde4272448c5b9c04a395a7f86"),
            (12519242, "0x79b7bef14533340cb2ae7d543d7e46a0a64646f1"),
            (12519261, "0xcf94e970c45cb64d224f2b2fb05bb73866a15759"),
            (12519538, "0x8ad8774a68aabc6ab28fee2d15ac437a36538016"),
            (12519785, "0x45a2b413ecc24e779a68250deb7d9776849ef6b5"),
            (12519827, "0x8cad1fd164c5e350ae9e27602b91e07833a93db9"),
            (12520366, "0x70ac2e9f374cddaa3adb19aa0ffa953f9c00e83f"),
            (12520393, "0x69377ca9354bf15c087a7aa833c3c90dfa03b1b9"),
            (12520637, "0x17fe4a722f794f9daf321b770a240ea7ae7e2ccf"),
            (12520754, "0xff9704a23d4c4f57c69d86e1113c1e9204cd804e"),
            (12520770, "0x3ab6d1cd2f9f34030b97bf022a10808f38f06d7c"),
            (12520851, "0xf03e648f2acd75e060e4a7054ee6eebcbca07b86"),
            (12521246, "0x05dbd4649043abfdbde5d145b80d0937e515228f"),
            (12521681, "0xf5d0ba0ad8b76e64c5930430b1ba6655bf7978fa"),
            (12521686, "0x01949723055a451229c7ba3a817937c966748f76"),
            (12521826, "0x17fb6eb41518ec57f5ed410b8ac027c43e476cac"),
            (12522462, "0x05b8ecd08a9a838c0f42ada5fea8a124e866085e"),
            (12522490, "0x437136095551671a7b1d326f0967b14fa24e07c8"),
            (12522759, "0x0ad1da8bd54d657d48684a32db00276138e7cff9"),
            (12523300, "0x679a26814bbdee092367f41e8aefd73695fb9c6b"),
            (12523557, "0x5c4602867f5b4c3ffa8d170b86fe109b7f986311"),
            (12523601, "0x2d0a00df187cbc77b108680373a6dc61db4abfa0"),
            (12523889, "0x97e7d56a0408570ba1a7852de36350f7713906ec"),
            (12523988, "0x99d619572c50a67d6fe1a4847c04a6ef0dae2539"),
            (12524177, "0xef955a87ea16b7faa7524fbf5245c84c2f083821"),
            (12524200, "0x91b0cd426f57e854df9acd00e4fd54f7ecac4ae5"),
            (12524283, "0xeeed5713cdbbad1339cc80e1d60f36db27c8fd1d"),
            (12524383, "0xdfdaa2b0674c0515fd9ea34aaa1395d438cdb802"),
            (12524408, "0xe7a05d7f38c62871102ddb30387d7a5935a554b8"),
            (12524597, "0xfe7f3dfdf485d100c0845c9ad92906605fea3891"),
            (12524605, "0x784602c0cb1a476d1d2ec7ade08fdd27e228177c"),
            (12524607, "0x4d63c8ec8e10478dc27d9f0c3478dc72947e00ca"),
            (12524616, "0x83668f6bdfcc57fe4af4bcc61448480c350f91a2"),
            (12524656, "0x0826f2aae7c902843684ec4e756b891f6f43cd2f"),
            (12524714, "0xd7197c3323191b3dfa9c6f212efb6d114b8abc84"),
            (12524949, "0x1108aed88ca6d7089b1f89825249f9d0d654bc16"),
            (12525735, "0xc7fe2343cedffac84dbc5bf908e6910c107b8fa0"),
            (12525866, "0xeb41030fbb8e54023a6891c514a8858b5c3f319c"),
            (12525895, "0xc5bccf768e4242a0b518608b4843988ec5fb24d9"),
            (12525920, "0xd124c5808a0d89738e0aef655c0aa19ec6175811"),
            (12525939, "0x4af7475a8d51ee117b1454832454b84cbba1eecf"),
            (12526170, "0xf784ce42e5d95a9f047bb6dec0d20729a30ceb77"),
            (12526351, "0x8d8f4f7a6fabf006cf4b63c2ba785082e3264574"),
            (12526736, "0x6630b6e5e970478c574f3ffc381f5ac62f78a4f6"),
            (12526876, "0xb3ab53d13a281057a31868cb575870c85662b79a"),
            (12526909, "0x26cef6b9a65985dcdcf98a4f4124a901a515e5ef"),
            (12527834, "0x8b5b549755a3f1b75b36d6aeb31ca7d09671cc5d"),
            (12527972, "0xf45ef5f968032403c6af0f13b93727c27bc663ba"),
            (12528056, "0x484fe88becdce966173bd9c6e75365d0ca5159fd"),
            (12528463, "0x9e8883ed3c25c6293f6feb530ce54cc972d815d8"),
            (12528732, "0x08947a42235dbc4be7326ba73a50e7c29b180689"),
            (12528736, "0x69eb684c52ff50819e458bf7554a175ed9563052"),
            (12528829, "0x8fd62622191c772066763a397731445b106995e1"),
            (12528943, "0xbaa2cb09abc5578e0d2b84a256dd5cd08fd65e4f"),
            (12528975, "0xf8d31860d443707687f3dc69a2abc83b89ea67db"),
            (12529057, "0x973e4af32dd14a4ccd1a3048c20ef373bb84e458"),
            (12529076, "0x489505426fa38327ad10e15c23e24376faf6d2d5"),
            (12529202, "0x866dfba0a5f07effcac524839cd35adc0edda3c0"),
            (12529740, "0xab2044f105c43c25b1de3ee27504f0b889ce5953"),
            (12529878, "0x283939eba95491b1a65541ecf5584c525e2ad063"),
            (12530463, "0x596d7bcfd45577abe581a71f810fe72bc10a5420"),
            (12530476, "0xca4e435c58347c33527c2d7b101c6e1746ddb056"),
            (12530568, "0x1e7e7054540e776522bbfccfd60f18f724eff0aa"),
            (12530570, "0x3512b97bf978d8a960ed9a77f87f8692315a547f"),
            (12530824, "0xcdc0f4092086452d8f980aceffd6bc077c7e656b"),
            (12531595, "0xfc1e511fd4d4412db47c6441dd44e2fa1bceb963"),
            (12531649, "0x93b1cb56b6ceb4424e0649e728161c185dd61ebd"),
            (12531654, "0x184c33b7b1089747440057e46b4e2bb61f09bc8d"),
            (12531660, "0x01df18e8132e2a1f34d02653bb51020b6cd52461"),
            (12531778, "0x1da15b24e051ecdb1f18d0637797152c19699b72"),
            (12531830, "0x90e2c4e67c3b595b551459bf847fb19ab17c41ce"),
            (12532155, "0x877307f6334d4ca98dfd132f1b8ea2071154da2b"),
            (12532233, "0x471e3a82fca1e017978f8d0e98346109b42c6652"),
            (12532264, "0xe11ee9c18d03b43d6a7fc53e51aedda8451e837a"),
            (12532331, "0x3ccfd9640182b576e26119cda149416a5de280f2"),
            (12532519, "0x5f0c055ab5d3861c6677ecf7c0be3a69d39ebff8"),
            (12532524, "0x52414c8b908e9a75c3c8704b2ae5e39ab7156c05"),
            (12532588, "0xc7daff610fb309914a9f2c1d96d3a95c9b54fc3f"),
            (12532607, "0xbdc5d268e02d183ca1dc2af588f354dc65e50844"),
            (12532704, "0xe28e21be61cb3fd438c8fbb3669be32c326391aa"),
            (12532849, "0x9b7122814b52fcb945b9f6a6357fd2f35a56f297"),
            (12533124, "0x351fe122b0ccc605ef9695f8010f9b2c12c4dbe0"),
            (12533231, "0x83e083ca231fc6b1a4d23847cc515d3da4c52ba2"),
            (12533371, "0x42497ea76a66caea529b818a48e20d2323a76f10"),
            (12533782, "0x2701926922e7b2bd6b484c1516d0e158088c8a68"),
            (12533790, "0x7ba32fde57c6940846ba16d27fb34f55c7442644"),
            (12533989, "0xc33dcd77650ae2382665be0dda21eb6d4da37cda"),
            (12534122, "0xf1ac68fff3aebc741b7c15acaaf41277a3fc16e9"),
            (12534364, "0x6ffa491daa26cc9efb800e1ea107d9956da1c316"),
            (12535136, "0xe67c5b70c5ae362fbff75c12a24f5b8f21be5348"),
            (12535195, "0xc49a31e73d8a2175df3de1a6701233580500d799"),
            (12535268, "0xff934eadcf87accd8fb3855f1ea5c7d0a1f5a281"),
            (12535653, "0xe83bad50c0b3f58ded74ecd67f001337e319075f"),
            (12535698, "0xdbfd41536a5a220e75c587ddcefaffd643ea3778"),
            (12535707, "0xee746572fa6414bca20b6ea6e53f542ff2f2b93b"),
            (12535793, "0x89c14db4179a44e8487ad6617806fd5e805b4e54"),
            (12535810, "0x749369f1dcb977b9fd1479a0c4c6604b1208d30a"),
            (12535826, "0x10786d64d2296c1042f7674570af62e57567f9b7"),
            (12535926, "0xfad25176c366957ed4c592d21b21eec176d70630"),
            (12536143, "0x303774361983436195ce35af23dd84d484fc7cbf"),
            (12536214, "0x5d0cdb2dae4c13c569578e85d81fd2f41d82cd9f"),
            (12536424, "0xa87998484c19d68807debdc280e18424d55743a9"),
            (12536483, "0x4545f0bc0053c887f1ffd673ea99ba55eaba967e"),
            (12536581, "0x46b9046269b008e435c3205437675c394d0da19b"),
            (12536690, "0x42670595766c9b44a2a4936b1c0e62f0e8167ace"),
            (12536713, "0x68cfee5c451befdf760909a1f3721e3db9af4910"),
            (12536827, "0xcbeb7da1ec121fc37dde2bc9010f3a4001e1ebcb"),
            (12536904, "0x30f4317758469e76d3609cfca1812e216f105c19"),
            (12536907, "0x38c6a386e6376738945b2cc52cd49b32893ee741"),
            (12537402, "0x9ee1b8c4bc66d1ff61ca991748c3d48f4a28a6d2"),
            (12537526, "0x22e19234bf61a4f850d0a79ed70cd1932b41ef26"),
            (12537634, "0xec4c606ff23a8de70f867c5eda209b165b248e1b"),
            (12537668, "0x9b1eb0e66c0173a3b9de0d4bd7754f07e47cb8da"),
            (12537953, "0x0cf4773f2ecf75e8d995e424c7c120af409760f8"),
            (12538030, "0x07201a08626c5cf6c31834751d3c18f50ad933eb"),
            (12538059, "0x58de5d2cb6c1e7196b1399c1ccb396413fdd53b1"),
            (12538126, "0x8a02e33a02cf133f0f98a0fcd07e2e8507cad2e0"),
            (12538279, "0x947cda728e25d8fe3651a33895ead8b7ff28918e"),
            (12538339, "0xbe9c426a89923760d28d9c77cb390ef4176c6086"),
            (12538466, "0xc9ad18e738960abb759f1f7a8f5d9a4e9986346d"),
            (12538557, "0xc445b6ff9e841e493a09be521196fc44f90953cd"),
            (12538735, "0x34b16c0162e76bd6f8a46d0d48e9eb42dba95d08"),
            (12538811, "0x9ac51a56e31cc9b75e9fa616709defe6c347484d"),
            (12538928, "0x04a959e3b14bcb1e5c35c611ba5d4a1bf29ad26d"),
            (12539076, "0x99132b53ab44694eeb372e87bced3929e4ab8456"),
            (12539163, "0x680a41207bb7f7112f0d07a8bed861b4a8860c1f"),
            (12539218, "0x5debcd8b540685309657bdff88dca748c01d3072"),
            (12539419, "0x5bd88ca801feb4eca729312c7f94cfadeab66a76"),
            (12539578, "0x7995ed2d49f112fe4abdd0543e2998fa0b3f70c9"),
            (12539607, "0x1d78dfbbe57ef0a6b7e07cc7ea9406577ca0726d"),
            (12539810, "0x1be80b8de4cd080ede46256e4777e4b863412f30"),
            (12539914, "0x33d0ca5c8dc2a3ac8f0f49596a57662c062df2dd"),
            (12540249, "0x8c3a9acda062d36b912a1eb60391d0c8e27311ad"),
            (12540284, "0xda65e4681dad37b3ebba045c7c64199a6f338df8"),
            (12540368, "0x05262f9dafe3eadb4238e6f2df527d75e465207f"),
            (12540533, "0xf744f1c51a93b406565573f74561d34481a10c86"),
            (12540978, "0x0273d4aac5b0f0c86bbd6ad58cf89710b2ece234"),
            (12540986, "0x789a175cf0d0910216d2effc5d51318015edeeb3"),
            (12540990, "0x6f42b1514ab4f951573b1f2edc52465c795c82f3"),
            (12541065, "0x16b2e589bf479b2200a3a81e0df7c2549fa15101"),
            (12541088, "0x2e5b5d5729529ff80d31c3cf2d3bbce6bc9d0988"),
            (12541247, "0x01a08178ba3d0ddcea35f714e795982cf39abb28"),
            (12541308, "0x8dfcb387b4641480ef9c9e639c1af7bfc0766d4d"),
            (12541537, "0xda14993eee56d3fb77f23c19b98281deb385e87a"),
            (12541943, "0x6b9ebb10290a6ef7889e09269332b0a74334d640"),
            (12542175, "0xbbe97f14a6acb173ddd3ff8dda669a443a77b7df"),
            (12542194, "0x94c725938c083adb4bdd55edbc3c2f7dfa85d805"),
            (12542783, "0x6379af7eb99c4fd2f4c7239910bf5d1fc2d41164"),
            (12543260, "0x7852e0a431d72e75a27b71ba109e5fcb04e0fcd0"),
            (12543428, "0xe3a379971c9ffa73e8c74a73c4240640393a92b4"),
            (12543507, "0x29bada2257424fd07b78e6992a45694873434bfc"),
            (12543568, "0xb6f4512e042e66b95ad9554d295d05b3f04ba14e"),
            (12543946, "0x53c0d552ea40055aa0311ad7bbe12152b65e8f41"),
            (12544014, "0x43798dd7b5c79b767618922ead81ba5dcfbb7527"),
            (12544168, "0xb7241e382709f1bc8f83fd4792fc0f1ec4658ef9"),
            (12544466, "0x37e8a5d7868e20e59d0abe573906fa71934d42a4"),
            (12544791, "0xdd960b52ea78a2602bf56e2a0f1563740210c57c"),
            (12544884, "0xb0efb2316d3313fa8efcffa994c8dca5af9252e2"),
            (12545045, "0x713985189ff756cdaa3212d68cd6fa22942e1181"),
            (12545299, "0xb3624c96ac07f9833ddd0c373443231f92c76ecf"),
            (12545412, "0x185edf28eb82906e4305ad03c190b8dc6f9f498f"),
            (12545456, "0x41f9c75bc28c0e224b6ce0594c48008998c2d864"),
            (12545507, "0xdb6f825a1350a93c6b8261e6ee8e5b63b24614ed"),
            (12545750, "0x6bd0f4335504314d62325fbb3ea4155f15f578c5"),
            (12545931, "0xe4b91705b50e7d540da0dc09f95474cdb168a096"),
            (12545981, "0x119d7fd1fe299b276c3ed87d572f138ae0b3feb3"),
            (12546250, "0x4da56307a38aa15ebb28c81766415b9179984036"),
            (12546473, "0xf6136a5c6a2b92f06cd4226aa6a72d3229fddb1b"),
            (12546576, "0xf95c108588c287d2c17e156ed62a8be0236e868a"),
            (12546702, "0xe5763dc0c41b531cc4edbb2aa8f17e35fbbf24c6"),
            (12546721, "0x6562247c3eb2145b36f630be77ae9c7afc1bba69"),
            (12546784, "0x5859ebe6fd3bbc6bd646b73a5dbb09a5d7b6e7b7"),
            (12546862, "0x0221d724c1a37b8c54dd99fefddae2b903d193d6"),
            (12546874, "0x419bff8ef6bec27af00b263fcc968af30afaf58a"),
            (12546960, "0x3bdc16ca2febe604372e0a973d31037f9709c7b3"),
            (12547226, "0xa14afc841a2742cbd52587b705f00f322309580e"),
            (12547262, "0x885f318ed1e03409a85339f05ea87e65901774c6"),
            (12547328, "0x8dde0a1481b4a14bc1015a5a8b260ef059e9fd89"),
            (12547380, "0xf62b5084ca88cc3cbca6b4ebe68db5fc6f27ee76"),
            (12547489, "0x3b91601db7b7db6a61cc13770c69ea41f4a46689"),
            (12547531, "0x9db16afa6db60e68805647e76bcc672baa2a5860"),
            (12547608, "0xd0dcd34d7e504cffdf5d200dac684dbb6a91ebb5"),
            (12547756, "0x1af72094bb34d8c15db8f8a7fd1bc24fc57cbea9"),
            (12547996, "0xb3b0225a69def8ba8055174d25a719daeb8a2615"),
            (12548054, "0x5b9a9f4e34d6cb372b67ae770bc905d5177f0c6c"),
            (12548153, "0x5a332599613c9460efd8a71a4b5de1aa5a005cfd"),
            (12548391, "0x7ad0557336abec0a006422bc50c25232ce89485e"),
            (12548520, "0x5b792308d5d167effa254cd1a0893b177643c6a5"),
            (12548531, "0xc0aac957ec80728f841f73cc331b95880212d703"),
            (12548537, "0x9727abf97e2f775f25025d14f4bcc79aaec9e330"),
            (12548595, "0x0025ade782cc2b2415d1e841a8d52ff5dce33dfe"),
            (12548599, "0x0fcb15266a1fa2c0c9f7b7f1e6c086edaa4caad6"),
            (12548611, "0xd504e996a18be8f2ea9f6980468065fa3e2bd436"),
            (12548648, "0x7f2b2d6fae29947a39df41c0718fc5b88747e1f2"),
            (12548878, "0x84d5d9b7cca43984a9e0ef7f85db8ab884e3e161"),
            (12548889, "0x1a205d1ef7fb2b247fea813934cf961dc98bc70b"),
            (12549088, "0x65f2f494d718f2e74c5674979a4ac7799a0e1ca8"),
            (12549352, "0x42ccbabc3f2e7e1ede19127fd07025ff53154645"),
            (12549541, "0x18d2d102eb71726f62dee878bf248625d27b69a7"),
            (12549631, "0xe0fd4c0f10be3211a1b25dab6cb0078a248c08bf"),
            (12549824, "0x5b916b5b83ce8432498730dbbd39f2eaa3d44b57"),
            (12549942, "0x1c90ef8cd8f4e880f15dacd566b3ce6e45fb8ebb"),
            (12550040, "0xadddab5f35baee3c7923485e390e92e0df82f1c1"),
            (12550116, "0x2e4784446a0a06df3d1a040b03e1680ee266c35a"),
            (12550236, "0x402dfea3c033a7f2d57a13787458029f470ec90a"),
            (12550334, "0x5fb2d6115d133f2442742c51e6244607cbd408af"),
            (12550596, "0x31bde882faa478fd2c6f0bb3b53d6434b70d99dc"),
            (12550616, "0xb88d057bbba5db7098b0ca336adeb81f2ac3c505"),
            (12551212, "0x1d31e2ccf779be5a7277c5098edce04eaeb76dc4"),
            (12551318, "0xd775471453a1d923d5fb3b45c9eb5595c853ddda"),
            (12551477, "0x75c80ce8fddfc61641bed16cd90c9123f0d9a020"),
            (12551482, "0x9507bf397588371f7b30cb232d8935a374c8120d"),
            (12551853, "0x5864dea5f1750d1f8887f9fb7f3a50f15789514e"),
            (12552304, "0x816f2118e9dcd22470d2128b73b1eccc88112ea1"),
            (12552323, "0xdc253c1da38c6644b2ebf2430ec007c7e16b9dc2"),
            (12552373, "0x7f63306a62c345365881e0fff85cb2c8baaa13d5"),
            (12552382, "0x6138f683c6eefd0fedccd98801a8bf4d73bb16df"),
            (12552621, "0x751680323dbc5a18e332f6a352c1c95d29e8571d"),
            (12552693, "0x98a19d4954b433bd315335a05d7d6371d812a492"),
            (12552708, "0xc5b47c0f9353490c17687abadb491fe776c9edf0"),
            (12552721, "0x08fadce9f97ea7f6096f3d41980e7478441272ee"),
            (12552865, "0xd1ede4862697fa88e8948fd83317e4d7888910f5"),
            (12552938, "0x3f2da03c22165da462fca9e99a2e34034493441a"),
            (12553029, "0x881b8d0b1ad9d1b1db918342b064d10afb9eaa69"),
            (12553044, "0x83a1ccda053b7a9c696c532eb3555ecd7cca8a18"),
            (12553474, "0x1ad3fb103d0fccc4754c7d54bb37e7328805f9e2"),
            (12553503, "0xcb804a3a1facec60fdbf39b59e874faa3db21a9f"),
            (12553506, "0x61d8d4324e06408386251dcf6bf77f0ebccbf591"),
            (12553615, "0x9db3311d68144996706e90892bf15958402d7670"),
            (12553617, "0x8670445a416757642ad97fe594d1545d1a280984"),
            (12553620, "0x4d27eff6ca4e7321739210dd0aa7c66b8dbb029c"),
            (12553663, "0x30f1cbf5e9fd99091b1acb6e96aeec96f7b5a2fa"),
            (12553664, "0xeb656e61866a94b05af97c20370ce45d5db139c3"),
            (12553766, "0x1763a4ab2b56fe1cd286b9f1a5acd0068a37e6a8"),
            (12553778, "0xfe73d4565b60fc309769ea32dc6e0755ce2f5a62"),
            (12553844, "0x43ac1cc76503a597498c3fb696690e023021cf53"),
            (12553868, "0x3a1762e23ef3bc1c7d27043ee7625c8e4e0d8261"),
            (12554139, "0x29a9accc2e0fcf638b519050eb52ff90be8d4ef2"),
            (12554239, "0x8378728f0673106ec64dafe4cb51dd6c4a9d0105"),
            (12554777, "0x3e7baf0837091511f980b0d16b44265ae44f40c9"),
            (12554913, "0x4c6bd5c32bedd36a92feac511197838c87c9bf82"),
            (12555053, "0x2a44192c9df3eca519ac0e1df5810fdc6843427f"),
            (12555096, "0x689c55be02534aa192f4bbe916c45318ca99798d"),
            (12555105, "0x62773f00cd3e0df3d70beca5aee342b0151fe78a"),
            (12555552, "0xd947e549bdbf0bcfebd5b11f8a629c88173fac97"),
            (12555756, "0x91278a6c8c022002110ceb27160b58f7cd5cdd29"),
            (12555796, "0xf845cba831be88268b1415d976db0afc7b8c6357"),
            (12555827, "0xd7265695e838aa2a9c26ca06164a4ce031f70608"),
            (12556325, "0x5665c8ef10051a0797cdafe04c6b90abec067e35"),
            (12556566, "0x1c6fe962f25336b2e531fe766aa890a20e1525bd"),
            (12556581, "0xf30f0c0cba4508f4443cb0084a7d376d537effed"),
            (12556650, "0x5e85bc4101d860dee7f807c2c5cb1d7ce7fc63ed"),
            (12556681, "0xee0273fb5a7ce399e54ff1daec354f17b8a4ab20"),
            (12557123, "0x57be500bbfd759462b93b60d1cc3273750aa6e80"),
            (12557332, "0xce5c3a992f5089d659cc37b243517b2b0c3446f5"),
            (12557346, "0xae5679aa4e945060eabe845aedcd98f9035cb48e"),
            (12557366, "0x9d98d228f446c3921339e296cc6971822e3a4d7d"),
            (12557482, "0xe7b23e4f2cca36409ca6438026794ad73d7d9eba"),
            (12557688, "0x2de73907b8491f00670f1c1bb5e7828c49f2494d"),
            (12557768, "0x67c110bf5ffc3b19280194e44cd8ab70478775ab"),
            (12557813, "0xf62e729ff705728a19eb3e25331d04e71986fe1a"),
            (12557828, "0x6ae34e9cabaad597516c512a21c107e8701e2f33"),
            (12558136, "0x79c9946b4cb90bd21fbe330110f5658f9d35cbef"),
            (12558224, "0xdb1f195e7f74921b55055831380c030b9dc39bf8"),
            (12558332, "0xf109067c13601f0d04e1bcba8bd1a4bad77363c0"),
            (12558349, "0x0268ba14a0e35d237c9c6a76f1adc5b3775d1b79"),
            (12558604, "0x44f9469d0d5393d3a01a0d4fa14fe7713c1ad1f7"),
            (12558772, "0xf9a4c8d53fbf23969c10e2400997bc481920f930"),
            (12558823, "0xfc9bc741db8694079709007a550de22ad4f37963"),
            (12558902, "0x9fce1bf5e578f4245fbabc6ca9d4d90586062854"),
            (12558903, "0x2094c9bcb053fdd1bb7cdb0831d7c900f511e5a8"),
            (12559013, "0x02178ec58822a93b29921448f209383bbecc90ea"),
            (12559100, "0x82629890bdbd697800ec537058120d3d82145964"),
            (12559134, "0x1d1c15051f930da9a169174f11901d481ce2864c"),
            (12559259, "0xe611cad166ea1b1f8d46fa574e5e14464821fbce"),
            (12559454, "0x7d05efe7c67de78dc2692f2613c0d4513a2d8d4c"),
            (12559649, "0xa27f652784426ba19533a69ae13977f36a624200"),
            (12560067, "0xcfecabe399f08a4cbd3c1abb6783b5678d526834"),
            (12560159, "0x654735d3779392ccbda31eb7be536a0717d7b3c3"),
            (12560182, "0x0d47fedc9f84ceb8c95719bb6b684b50645cf2f0"),
            (12560363, "0x4a3bb087387f6b2e7c4af146678fe2c017678e87"),
            (12560377, "0x1d57a4d549cd5d1eae256e9a22b9436d426d1c49"),
            (12560922, "0xeb43b045ed09397f2d8cce95f02d8c6a766a1058"),
            (12560959, "0xeefd6bfd97c2b49d739140104d2d0f53f741b989"),
            (12561079, "0x4004d94a4e6a6ca2b745b9b2e9fd262f3c3bdd98"),
            (12561277, "0x81fb80ae2b28ce03f0f793101ad31f98188eec8d"),
            (12561309, "0x8ce09b147ebf9871be0d7b3dc4adce0c865d3143"),
            (12561345, "0x203b6a797364eb095d414029d0fe7ac8a15a003f"),
            (12561357, "0x22886726fb80ed0b1b592cb03964555c6fc7ad88"),
            (12561466, "0x119582f5201cbbfec75394ad921ad02d3f57e9fc"),
            (12561481, "0x88c660dfa5072732718085f648e03e9dd360a1e0"),
            (12561500, "0xda434d7b379f770d32601fa571532c0c75a528ee"),
            (12561607, "0x9a772018fbd77fcd2d25657e5c547baff3fd7d16"),
            (12561731, "0x67ff8ac4e30e59505b27c4c204d9e463bb7d605f"),
            (12561900, "0x383effbb9257703da0caf187943fbf02dc1d9905"),
            (12562001, "0xb28a112661b2adbca615bc321c859ce8525d6d04"),
            (12562005, "0x12f0ec2c8edc8fa9a349754855a55a003c134bd5"),
            (12562242, "0x04b8dc41aa9cb9181430ff189be170fbba2f2255"),
            (12562248, "0x45f7875e0ecd0a6ee8dbcf5198f926353d156e13"),
            (12562373, "0xcf9be41ca83567a72dfcf6583a98f880b8004dff"),
            (12562532, "0x770beb3d9c7e38894738a4b4609171b208bfd08d"),
            (12562630, "0xfb1833894e74ebe68b8ccb02ae2623b838b618af"),
            (12562871, "0xd82faa2d94c18351ede546caa6b797dfe5fdfa98"),
            (12562942, "0xbabcc4debab3bbfc1ad27431fa61148a9960b1be"),
            (12563082, "0x6394aeab6ac8993dc62f5e18e46762b75eb890d8"),
            (12563115, "0xe64725f47557ae35bc3854decd189a5cc820c7b7"),
            (12563722, "0xafd5012d37d73f237f5cbea12bb8132b3b984a7e"),
            (12563765, "0x9a41e60e819323b3094403c3d3548c6a25582ade"),
            (12563780, "0x1b4b20a49afe9a1a8439ff8668c63df68e1f04cd"),
            (12563894, "0x80683f37d3cb36d61cd41dab5f9776955dc60afb"),
            (12563994, "0xb4d09ae66b024152128818f7586ea63b40b90562"),
            (12564026, "0xb2875e30aaa57a2e34c617f77f6d6b6b24a499f7"),
            (12564069, "0x540c2574ce5c768f64108884d3d126910eae83de"),
            (12564204, "0x620cd19eae24fb8a02df908bb71b81b6e3aa1ccc"),
            (12564402, "0x451eec2ef2ffc46f21267b96eefcb28bf6a538f8"),
            (12564605, "0xfe439605633f9afe8ec3e0e075bc12e2a2204c1f"),
            (12564687, "0xab14c2c38dc9dd7081820269dff088ddf0b72ff6"),
            (12564689, "0x0cfbed8f2248d2735203f602be0cae5a3131ec68"),
            (12564812, "0xaac051635fba070e0c18aeb1e867be614c14066e"),
            (12564825, "0x938efdf215c7e69d4d87a760d2157687b913ec55"),
            (12564832, "0xf1ebeed4969bc853fc5892f516364c61cdb35c24"),
            (12564889, "0x44b0901fbef7d9329516ad820196998a9f8adbcb"),
            (12565123, "0xebdab3e7787501cad4159d87561aee0687a852ff"),
            (12565150, "0x216084e4a9d94c867e065d709905051a8f22f92d"),
            (12565254, "0xe0c1d1d226c43a49b57902f443bef67df46e281f"),
            (12565293, "0x413ee1efd065befc766d0b0f61ca0a74b2cd2bd7"),
            (12565640, "0x56a990c0197d50a13d8a9537fe5a0d7cdf69da3b"),
            (12565899, "0xa4bd201c839755948e4eaec754214f2b8a6e5955"),
            (12565963, "0xcc260dbebf6912812a4dbab305bbd9c09e52efa1"),
            (12565965, "0x9da1d1c9353b32c9e15adf11fadbe9f0860fccfa"),
            (12566001, "0xeb22cab10d66f9c66568e348d2f51c94e5a20d8e"),
            (12566226, "0xb9a43110663ae6a42a0eafe9134753e20e5fee83"),
            (12566267, "0x8a44e0bf8ae224488fe3770238f8e5026080be10"),
            (12566788, "0x98c8be139d73ac27bd96a0be97f0abf17efcad45"),
            (12566987, "0xeb2f3eceb693c633006041d07ea5c7dbf4da10e5"),
            (12567143, "0x917c52869df752d784ec73fa0881898f9bfd0fd8"),
            (12567195, "0xa89e82847b0cc1cd3fbd735320bc237211ff11f1"),
            (12567696, "0xcd5ef33a18e063b81eb993fb5a0784cd1c28f357"),
            (12567930, "0xb7f027ae8f02377da3b3bce172d07758b1ce7d9e"),
            (12568114, "0x4747517a4c36d23937bb96aab841a6bab5be3f6a"),
            (12568339, "0x74c857f7432254ab45ad9cb9379759c3c760b45e"),
            (12568591, "0x29a02fa544b31df5aa8538c01290508823f48145"),
            (12568944, "0x494b9c24804668872f7c76a65461b61fdeebb0fb"),
            (12568967, "0x4b304f9cff8fc3e673025b21747d9ec67cbc2b64"),
            (12568981, "0xd578ae5106d8a0ce3ba6dcc7e70acf44ab8e791f"),
            (12569033, "0x1fce5789347527a3ec05a745851a591b2df40d48"),
            (12569048, "0xe22c44bb5ce8253e32579c947cf98bb17d7fcdec"),
            (12569070, "0x7bdf4fd5ee9ca73492e4b0591b310ce27758872f"),
            (12569077, "0x0478137a422d61835764f2427947a7684f841056"),
            (12569134, "0xfa84ef015331d1bd83321e344d92489e3de0ca9d"),
            (12569238, "0x40817acc99bf2090fc1088aa1dc2043e38450622"),
            (12569312, "0x6176c323b86e2753c8be14c6843398725990c777"),
            (12569412, "0xcbe4e6f3352a4d3fa64b9ced6d8c2612e38129b7"),
            (12569515, "0x7a54a9e1edc1ec5b8a1fe492e72cccf9c3116a37"),
            (12569564, "0x0c06992fa6a6b3f5b3f8416f46d8c64cfc9131a2"),
            (12569590, "0xbf190af824a0f79838ae79df738e4086a084a057"),
            (12569876, "0x385caf27ab46c9488f9f2e66260aedc876c2ddfb"),
            (12570040, "0xf03744a09b37ee59c0d1a38b1f358f77cad53357"),
            (12570139, "0x46ae701f0a42fe9833d7cfa4605ee0e61e73b216"),
            (12570179, "0x2180aafaaa40f6fa61bd7cd4f76eef4763395877"),
            (12570788, "0x0c1887e602da2aa96aa0642a37a32dbc2a142213"),
            (12570811, "0xaf07806094d8d2456e11e84f97fc050331d5aeb9"),
            (12571155, "0xa75841dc610296d01b783caabb931e10ead28c9d"),
            (12571288, "0x8dbc71ee0572c8b7a7a77fc13deefa2693452e83"),
            (12571476, "0xf1c95b7ec2a013c8cd4fb10e038133f84180f919"),
            (12571570, "0x012a47d45f077796282f22220d3844c4d72504aa"),
            (12572263, "0x7de84ce7b87bda2a73f8b06c6549bfba99910a40"),
            (12572273, "0x304de4a5b6865d1fa1d5a2e69bd997f0088b3704"),
            (12572367, "0xe103f2668a68538f71c90f701091d18da2244e9f"),
            (12572444, "0x0cbfcd3426594d08d7a9dee64cc03c51b121992b"),
            (12572578, "0x3bc810483e4e2344f0e114a57af13cedc44cf717"),
            (12572757, "0xea8e54d1fe65b14752d9e6036d40e454e5a1c11e"),
            (12572842, "0x50086790e2a984ee9406199cd56c50c9acaeb6a4"),
            (12572944, "0x05966826d5d3afcdaf4ac25746dbf1a2cdde60f0"),
            (12572945, "0x6572ab2843069aeb3e7246ce6f9b67f747b86ad8"),
            (12572997, "0x3019d4e366576a88d28b623afaf3ecb9ec9d9580"),
            (12573026, "0x5c28b5f471d97f53fcf132f16f9f3c0c888c1a01"),
            (12573059, "0xe0ead2c803264f7eb538846f08e49cbc5cd15730"),
            (12573141, "0xac613105f750f69ed2219d09f6591b0ebe74e78a"),
            (12573318, "0x043a957a1121c25698a5423a62b980c76299638e"),
            (12573319, "0x81f3f8374ae800ea7addcb1f195aa28a7a543a7e"),
            (12573382, "0x50a6c4e0859229e720e2193f34a3a7cfba6f3044"),
            (12573547, "0x1748610994c32547620a2fd8786511a31543356c"),
            (12573723, "0xf4ef17c48834e2ca617971b5f746e4a6c0a07b63"),
            (12573844, "0xc2bcdcf6025db89aa2bae237d3f4acb33e010664"),
            (12573972, "0x86ab2bbb17c8bf48fa7c41397210bcf760cdfe59"),
            (12574013, "0xd794c838ced02a7d903857f632ba38f1f5058d31"),
            (12574204, "0x3173c6c004874e9a536488b16eaf46231a3567dc"),
            (12574217, "0x85e073b9ade32476139a404474e249cc2590ef01"),
            (12574530, "0x67e887913b13e280538c169f13d169a659a203de"),
            (12574646, "0x58a9c1aaaca5dbee0c64de61c6b011b38391cea1"),
            (12574886, "0x99a6880748ceb61c3d9b6d4cd49edb59f79f405d"),
            (12575005, "0xb10bb5a0616f680cd9ca4ea79b2895f7fc385526"),
            (12575388, "0x0e4620563098fe12dc3956f8d16076f9c81b1824"),
            (12575395, "0xe49e8059921a6cdfe1727968072b9d059643715b"),
            (12575430, "0x343fa3c8d17d5f7ed95f5b619490be3d7eb4f4be"),
            (12575462, "0x382c113acca7c2722d7d7b8295a805efd196c8ee"),
            (12575556, "0x39b3839486a6ba153f5377b54b1fcca0ed53404e"),
            (12575653, "0x8ab99cdf6607f54113d323f529c242fce26fa7ad"),
            (12575674, "0x95209160ca66bfe2876877429c3ab89d9b1abab5"),
            (12575908, "0x14f863fffa91bb9f2c1430c4d2d2c20ac1fe8f85"),
            (12575922, "0x1f2d174d4e948d59375a425465e6240c196bbdcf"),
            (12576003, "0x2f71eebe4ca17a3c753778075dc6e08158f15c4c"),
            (12576197, "0x78ef1677e1d3d565629e455a9defc2e33e863c25"),
            (12576788, "0xb07a93abd0ffeadfc1c2a6d56b664ca686331369"),
            (12576861, "0xb3fe3bad93b4f2e35fea390f6d607b35550baf97"),
            (12577003, "0xa7a69bb56eec7d4409257046a8dbde4c8b1cec9e"),
            (12577027, "0xdf033b588347fef1aed80f05b4a0941a45eb9cf8"),
            (12577100, "0xc2bba1aba4af5d66f9bcc4ef0f0613fb8923cc97"),
            (12577281, "0x63e5bdd389cbdab0f35f004024a8c63ee0f6bf5e"),
            (12577492, "0xd6366e26e22b32afd98e3d7eb8aaedfa559f953f"),
            (12577601, "0x23ddebd30b69e77bf86932ac96eae59e925a0ba0"),
            (12577701, "0x860caf97d5da97145004cb45a4bf458686fd90e5"),
            (12577774, "0x628b8447b23ebcb4d940c4bad08c64f89b2cc01b"),
            (12578184, "0xcb754607df7b8fec29d9cf5862e367573528c975"),
            (12578278, "0xfa8a5082174c53eff6866a96e3253b9d5403cb12"),
            (12578347, "0x77c97f04265039f31a47322adc2fe87795805de5"),
            (12578446, "0xdd77c2b43f61f96cdb3e523bcc30a041c218b8e2"),
            (12578550, "0xe0f0e02a16b45f949b98856b61175e63ca5f6293"),
            (12578587, "0x2078ef8fdcb716f32a275e51781c9655d0a7396f"),
            (12578917, "0xf972cea53189d593f476d868ccc49038e8dcf3e0"),
            (12579163, "0xeb510de12fc7ac3b491295f39a07621ff28299ff"),
            (12579207, "0x7d221e3019bff0c27c7bbfff76b3c3e7b4ca31d5"),
            (12579379, "0xf6a42a1963b34ad95bc82c8afe1cadf27b0abf2d"),
            (12579409, "0x9272ab2d57a1cfe044328f0a7f09766593b8cf26"),
            (12579482, "0xdf47636bf318f9d5cc9aafe38f7e7bf893d58fac"),
            (12579499, "0xc2b4faf2c5eb717f06913cb6276ce6f901e64748"),
            (12579503, "0x0bbb7c3ef37b37330125547b76a992aeb5a83119"),
            (12579605, "0x70c6bd8fc6c48dc57f95c9b8adff4ab9f0e19f5a"),
            (12579658, "0x8453ec5dae7715489e9d60b205cadb429beefa90"),
            (12579669, "0xc936dd27cd8ae678be9ffcabaa301a6b530f691b"),
            (12579737, "0xe90fbb3a6ab3839e5910984e30076b296ac93afd"),
            (12579900, "0x18452fd36bbd92cb46366d143e7fef8ac165a0f7"),
            (12580173, "0x544176512a92e703df13313977ad43b4cf64ebc7"),
            (12580471, "0xfdf5fec06c980d3f1b66f949ce8e87037c8b5e0e"),
            (12580530, "0x00cc4f4fdce4207b37ea129251d1afb2cd45efcd"),
            (12581064, "0x0791a44269822b883c276debec51d6ddaafafae6"),
            (12581085, "0x39ef04dcfbc3800c8eadf6f7f2ca9dcbfd829b64"),
            (12581216, "0xf88ee8100758352939649b41cebb4e5088415d85"),
            (12581219, "0xf359492d26764481002ed88bd2acae83ca50b5c9"),
            (12581282, "0x6c4b41b1aec1e39f660ca6ad2392135e8f947847"),
            (12581369, "0x557d3e1ee5e8d870ef70624ac148c1253672d40d"),
            (12581381, "0x951e0e67d620850c07ad94bbd887a79e9f2689f7"),
            (12581382, "0xf9be360b26984afeddda8a4760c725b1a19e0d85"),
            (12581437, "0x7563201511e9e89d2919de06c30250dc97af927b"),
            (12581519, "0x3db6453d6d937c6e37bd5ceffea8c930444b00f7"),
            (12581780, "0x33ad4de14a71292a901be302bb0446eedfef022b"),
            (12581781, "0x16ae7d78cfd8772c4b2f1bffd127166a45bfc36d"),
            (12581845, "0xa1948040d48368826f52f1db4dbb71d90f244026"),
            (12581849, "0x5c2b3edbe845764b99eaebe87377f1f9d27d2a7e"),
            (12582431, "0x904ea1f62860eb5bc50bfc215dafc76a5771a614"),
            (12582453, "0x41635ef999feebbcef4c3d30afbc7a1af5abe2e6"),
            (12582562, "0x7d84c663ad4d597b69a540d1cd6a2636994962fa"),
            (12582590, "0x9d2713fa2f387ed1284a4176e7841253b4da2a71"),
            (12582946, "0x3d2ded4c5dc386c7fc92fb9462ca6abae00378e4"),
            (12583058, "0xd9193ff362da337e94e37ac66a5f2b8f86cb3422"),
            (12583105, "0xb48af592f3b53cc8af9721b34988204402749111"),
            (12583193, "0x6c063a6e8cd45869b5eb75291e65a3de298f3aa8"),
            (12583352, "0x8f9a0935b26097a1fc15d4919e0b9e466edc1c57"),
            (12583461, "0x6f6d9127bb2785a874cc34e7bc61264e0c3cb201"),
            (12583487, "0xb26db7e6a4daebd4d76477f321fb978e9159da46"),
            (12583591, "0xfcc25e1e772adfd473124a8b35354d54ea3af24d"),
            (12583617, "0x1fd8723a1dfdd4299a86f987af7ec85834666c3f"),
            (12583677, "0xd2f6a8c03e940e72cc0df54a1f954500e3012849"),
            (12583753, "0x9902affdd3b8ef60304958c60377110c6d6ab1df"),
            (12583832, "0xb0d17ca307ff03c1c3399393c35085ab68bfed2c"),
            (12584004, "0x757dd9c177d31e4ff48cc678b938252e19cc2af2"),
            (12584240, "0xf2c37a93c83f027907d7d067e2c10419e9e9a736"),
            (12584246, "0x3ed81407cdfa7ae026fb82e05f4e0bba8b1ac0ed"),
            (12584366, "0xf2dd826c7d50fe89f0eac6ce1f5678d3bcc80942"),
            (12584420, "0x0b82aaacf8609e3dfa74e216976ccc3d29fef346"),
            (12584512, "0xb065edc84f289ce4965f74622193a6c7d7ce719d"),
            (12584645, "0x7cded09ed9eb8c323f1bdc21c935891dfb5a5a1d"),
            (12584766, "0xefd33216064454898c53740767253a4b97f01b68"),
            (12585206, "0x7aa39bb7405e2b6d157831554236287a6620dbfe"),
            (12585450, "0xc674146d14ed82e8ca61452dfb7d9402376f1981"),
            (12585662, "0xd429325f1a16da532a002a1aced57c55df2ee141"),
            (12585703, "0x2d8e7d3e24c2149688a157482102e51a923e9db1"),
            (12585853, "0x0819678a6a4a6f41cb495c92aeaac4dbf2999733"),
            (12585914, "0x06ada8f74d99c6c200672b02e5c3341866ca3bfb"),
            (12585984, "0xacabefc959a714b6d2061112a4e3f4fcad00d52f"),
            (12586163, "0xd28dfd2d0a18cf3e474ae523ab0022ab1420ef39"),
            (12586357, "0x889e126e30835835ed1b94ff68ff481276e1e01d"),
            (12586498, "0xdf805192f23f7b6e0bc554a4e0e4dee92ed81d67"),
            (12586920, "0x2323e0f049386da5e1d4ae4d1530f3782fc090a7"),
            (12586926, "0x482a2fa61ef29625069e07c62b84a4dc33809146"),
            (12587024, "0x307c4d0a83931c3eebe501f8f0c0b4c249bcf206"),
            (12587191, "0x7d2e2fef85c67ab930268541d065be7c97f953e2"),
            (12587283, "0x9fde8ab055fff961503046460732e623e4b33e35"),
            (12587294, "0x4c92b84392b15fdabbdfa6cfb74d339fd3770f9b"),
            (12587331, "0xa5e5bca9efae79a446207bc3fea48341394934ad"),
            (12587524, "0x228eee2630cbe8df079ed8a9a3fbc6e9266e07fa"),
            (12587577, "0x28272270a21c286561a377564c04550139ee2c27"),
            (12587598, "0x07dd38ad816fe84b8b37b3350c4ea127171d5f2f"),
            (12587601, "0xf6aeeac7b85095e934ce36cd64a2e54f260d7c4a"),
            (12587637, "0x709eb4199bb2961772cd247e54a767eb6076a11d"),
            (12587787, "0xd9f6605f6ad367d072c342dd290bbd8ffe0a7151"),
            (12587854, "0xa8ed39954466fb6716a88b050f06f63a7aa93a30"),
            (12587870, "0xc5d2ef5f26bc79b9abd7550ee4b4448f0bef2644"),
            (12587888, "0xeee4879380fa03c7e439e8921cf342c1fcfb2378"),
            (12587925, "0xfaa20aa2d303569c19ed6a6c0a308f48bd3c6746"),
            (12588047, "0xba6f08adb52badc392d803e70d48c8071734131e"),
            (12588530, "0x7721c9ea663dd0db40cb9a7f3695f83795e6d66c"),
            (12588595, "0x2f6a0ad19c5d30f8451831e5e7000af24737790c"),
            (12588988, "0xf9427a4646423806bca24bacfd207f82f9fff8e7"),
            (12589242, "0x35901a9f1dc789b76fae5e771a3d402aa9f1881c"),
            (12589250, "0x7f3d44e0e4497d3de3d598d556d6392c2b7b4f9b"),
            (12589593, "0xc340d73690b636bbe6e3326310f59f430891b8ab"),
            (12590293, "0xc490f67d9173c6853771e8c764a2cc519fc69707"),
            (12590446, "0xac6ccc2365ad727a8385438b72ee9f51aac81ccb"),
            (12590462, "0xe00e39f2d1774dac1afd41d7ba80af3cbe461941"),
            (12590495, "0x9fffd5f57ad20b16e0ac6404c62d8fec993bdb37"),
            (12590496, "0x932aed405eb88d3033ae875a4a96e94e39675ae8"),
            (12590557, "0xe302beaaef6afd66c9b7b89d647b3e756dc53ca6"),
            (12590683, "0x41b536722c014a577f06a4bb0dfa08bf0b8f5e87"),
            (12591112, "0x4702abebad5281b53ddc6bf474f271e5b38033f4"),
            (12591140, "0x5daa89fe664dc6537450a678b323aff2ca7b6a15"),
            (12591147, "0xf18c97261e4152c173779f749bbebc9e856d1838"),
            (12591151, "0x76d8938c96e8abfb44db042091131b7e9a63189c"),
            (12591222, "0xebf690c9581523875c001ecd8221d9624e2c1e8e"),
            (12591408, "0x4be80b00b85c301fb91152aa445ee1f6567e043b"),
            (12591777, "0xe67f7b51b57be3d6feee6ec5a2058ac24b38e12f"),
            (12591971, "0x97f24e80db03df79d230c4dd15411c82ee63ef6b"),
            (12592237, "0x000f0c0b0b791e855dcc5ad6501c7529dea882e0"),
            (12592389, "0x32ca9253085dd0a0a7be7efe5b79047b18cb8962"),
            (12592841, "0xfaabf84d73ccd71c55cea6fe2252ddec058b0ed7"),
            (12592927, "0xdaa30b38a4261871e949e93ff372413878e3f7d5"),
            (12592985, "0x8f149216bec58eadee8a66269d36fd64afa2bf72"),
            (12593020, "0xa74ec2ada6169ef93b8ef1a16a0942dfad973d38"),
            (12593041, "0x935a0a9c2c40e3c6e9413c00617a801cc5e99bce"),
            (12593055, "0xcdff6ddfc9e4807c9927fd58708c2ef3484cc305"),
            (12593389, "0xdea651334f4e45f9e1575d1b7bb81d03a6fd886b"),
            (12593406, "0xa867765b8dfb682fbb5e30499cbbaa0436f134c4"),
            (12594166, "0xff6231309f9f7f418ed7b55fa6f4693bfe5cfeea"),
            (12594204, "0x8e3587044bddf25715f3fc7f131e250c65296022"),
            (12594212, "0xc8df1e00ec7b726a3a0b6739a3534ecd6d89bb31"),
            (12594243, "0x9b2a4e447132bf5060a2b6311a4bb315fd28a26e"),
            (12594402, "0x06d1e5441698b9f76ab6acc0665cff164cb51297"),
            (12594627, "0x5f340c0de925bed8ea7c950c08c5d8083066c64f"),
            (12595309, "0xadc2f3749bccb1e55df107ed910f16fe4641c9f3"),
            (12595571, "0xc271b9b62ca001f31d7543fe93703fce1288ac81"),
            (12595686, "0xe2b03ad65001ec94ae4ea378432605e84f6973b2"),
            (12595747, "0x59d2916f6c4e8a0ac167d228cb2f5f6698cedd65"),
            (12595781, "0x8374a425b03957957cddc4e3271c990304ba4479"),
            (12596208, "0x2d84abe9ca1c7a7bbe85a74aeb532f86b1a7c916"),
            (12596262, "0x16d19dd56d2cf11d2d898f15d3033afb8b1adafd"),
            (12596332, "0xf9d29c7c0691ca27f076828b4c1de6f6b14c0bda"),
            (12596430, "0xfde9cf682cb281be875019d2c5949f9113c9c7b9"),
            (12596510, "0xdf9e1d57f54cc401ae6e3dbd287deed5f191bf29"),
            (12596677, "0x277a3296b30a07d63930a831ae2de5ad0b3e8ab1"),
            (12596769, "0x41aa55d2dcd42760ef0b5da303730e7ac734a475"),
            (12597110, "0xc29271e3a68a7647fd1399298ef18feca3879f59"),
            (12597233, "0x558aae785a77205f801cab3193a4caa911b63afb"),
            (12597341, "0x5613dd4974a6e6908374767f0f8bd6b5a05ddbcd"),
            (12597601, "0xd4acb4b634990f95a794cc123fa86176408f3dd2"),
            (12597661, "0x5c09448abfdcb638ea992ce3e45bbc1d7ec7c4cf"),
            (12597969, "0x795bfaa385bd7c84457f49a2487a016cfca35fc4"),
            (12598015, "0x612eeeca9652bad93eb93da1187846669ca77eb7"),
            (12598023, "0xfd021c60ab493c4dc19c7df2d79243759d884561"),
            (12598165, "0x0929bce643df935a46bfb449459196b4fceedcf6"),
            (12598347, "0x6213c5cd6e68d7eafede939ee81f4ca9152fde84"),
            (12598379, "0x22d244621b7bb749a8be109d6af79d3b4782f0c8"),
            (12598464, "0xf8c3a39986835c5de4dc9d549a187d0d0722c883"),
            (12598560, "0xe53fde6b1270822bff916d1d941d7192eb50e34a"),
            (12598759, "0xb31a38f4e228ca8d0873ddb3530a89ac80aafdf4"),
            (12598829, "0xb9249c20c3d8cd3a94839151273efd81ad01be6d"),
            (12598836, "0x3b685307c8611afb2a9e83ebc8743dc20480716e"),
            (12598877, "0x6db2cbffb8d38cbf42c6da28731699e3a8848c20"),
            (12598896, "0xc80267adacc5f9341401e6b08a1b57711076e341"),
            (12599036, "0x6279653c28f138c8b31b8a0f6f8cd2c58e8c1705"),
            (12599680, "0xe328f3c581aee060afed7b8c043915e4e7a6486e"),
            (12600204, "0x0c71f7a4790e228870f647e5cf8f20f5a1d0d93a"),
            (12600282, "0x737a43adf2b9d1366d59ac790347fa3403df3214"),
            (12600284, "0x40302bd08e3d0ae97f0792651e775f9c0065a10e"),
            (12600292, "0x1af432ed9c8b573de073c4c90a9d7b3d4e08f26d"),
            (12600299, "0xcf6c2693540feddf552051d73e9d987d01838461"),
            (12600329, "0x617adfd75e1f06027ef243e56556b53b517c9ab1"),
            (12600817, "0x2e571c1479e9b5415057655e454680a61820ce65"),
            (12601276, "0xa8db73fa70e1f06f0da6719e1883bb5ed7828e3c"),
            (12601287, "0xe4a66e9442890713d3a7c9e2759f96a1482cdb3a"),
            (12601332, "0x6d18bee592d645b148f65126cd3a4406277da219"),
            (12601655, "0x351afcb4d0748f1537d4da39ca322c916adcaa2a"),
            (12601886, "0x56534741cd8b152df6d48adf7ac51f75169a83b2"),
            (12601886, "0x5a59e4e647a3acc42b01715f3a1d271c1f7e7aeb"),
            (12601896, "0x3e327a672734310d08f42da393bb1e386bbf28b7"),
            (12601908, "0xcbfb0745b8489973bf7b334d54fdbd573df7ef3c"),
            (12602097, "0x7261bb346ccc02911e4b07f933ccd69dc51ee3e1"),
            (12602244, "0x7748d489fb6f5b016ef7b3ac1375518dc41132a2"),
            (12602445, "0x81f389a287990ae02a431ab4aa88c9804aaecfe4"),
            (12602451, "0xafe56a692fa8964e95ed0e7810c2247d3ba20b2d"),
            (12602476, "0x16e520ece9b86721e1866bba2f48dac0a7933178"),
            (12602663, "0xa58262270521d7732fccbbdcdf9fcd1fc70d47e5"),
            (12602714, "0x405127050fc7e45d861e84cb96368b6e45015bb9"),
            (12602778, "0x0574714ae050a8d4bace7180fdf7561d6b4f7b5a"),
            (12602872, "0x59139a6cf5baabd298728b5b07b97eece2c17ae9"),
            (12602988, "0x07f3d316630719f4fc69c152f397c150f0831071"),
            (12603023, "0xbb0bec7a33693722674ced149d1e6a473a6bf758"),
            (12603028, "0xb203fdf3a9c860ed3c39052eb41bb0db87aa7a48"),
            (12603132, "0x7e284dc78893ef132bfca1782e41af3386b03668"),
            (12603274, "0x3b725b629b7c547eb9b30f34604bfdd4e958076e"),
            (12603283, "0x00eac8324053b72a43fbf65aeee36a9eb16258ac"),
            (12603361, "0xea4ccedc8391e307092d121b693699ffb50951aa"),
            (12603484, "0xabff02e569322cda51eb799db7bf8247d759dfc5"),
            (12603543, "0xe9cc1f03a53b199aaceadebef4cf70d5a23690c9"),
            (12603775, "0xf507e003278d3ac9a0fb37a565f4671dc56aff20"),
            (12603841, "0x676b1c74bc703767628302c9077b3a21cdb7f7cc"),
            (12603844, "0x064f22cb324979c24b4632e203c92bdec3d6cda8"),
            (12604104, "0xe1a5b16db3aa1e2fc90a27567838a0536903a941"),
            (12604357, "0x46dbe096eec13630d049903b3b1505edb76218aa"),
            (12604703, "0x4531ec2b7f49b9cac3b689eaec7e779af3f65ee2"),
            (12604720, "0x9b92337f221ba14257613aeca567210cb88d4df1"),
            (12604997, "0xcd8506cebeef573ff69d12d228b8f62c832d0ffc"),
            (12604999, "0x19efba7feca888ab6bc411103a657aa4e852d024"),
            (12605026, "0x5b1a0e6339a445082d8c94b8340cdcc9618f279b"),
            (12605084, "0xb00d9e09933ac047cb3c1bcb78519b92ee414e62"),
            (12605286, "0xf3cd9a264ba12ab0ac3240e0786dcd40cdccf745"),
            (12605330, "0xdbfd99a28787a31ee06dec5f00deb090ff04d956"),
            (12605449, "0x460d507154b41f9f4134ff64057b7139675fa852"),
            (12605473, "0x49dc430619d241378ed57ea02a0d5db97357de8c"),
            (12605573, "0xb0f4a77bde7fee134265307c5cc19abff0ba409b"),
            (12605681, "0x62d06043f46a7201a835759aaaaf7f7deaf60315"),
            (12605880, "0xed49728e65246d81fa65f832fd5197467abbbc40"),
            (12605933, "0x005b584315d7c47bb5fca504ac0d8df56aea40f9"),
            (12606104, "0x50072faea4450f2ab683d5101b52b03bc6dbb703"),
            (12606319, "0xeee1b38286d23b0c22a475542aa18f2ef7cade82"),
            (12606633, "0xc02d0fa00c0bec48186026c71da2e54ebf680139"),
            (12606698, "0xdc4a6863e9e049b2a28a03f4c743475bf185a246"),
            (12606719, "0xd11adcaa3d44da3c94d7aff9fcc99772a2c577cb"),
            (12606864, "0x65fbfe10a9a32eb9f5010c4461c6c016384b811a"),
            (12607072, "0x77370b278dcb180b1c9381badaadbdeff32f6317"),
            (12607466, "0xfc68fe34db695533e605fd89d599eccf0a5594f8"),
            (12607479, "0x8fc91697ee952161edb5934a2d2bbab16ebc6d2a"),
            (12608212, "0x5720eb958685deeeb5aa0b34f677861ce3a8c7f5"),
            (12608409, "0x610a94f64d1d149623369e5bac9576065d23893b"),
            (12608578, "0xcb1e3716b4408dc22e95b17cd3a7caf1a84dec39"),
            (12608648, "0x5a8bebacec4d1bd5c2a7f4c8ca4ea4c962adfa59"),
            (12608702, "0x4664a70f12f09e5cbecb55bfd95e80b1661418ea"),
            (12608759, "0x3ed55de227079a1a7245a178f8815f6d14bbd900"),
            (12608920, "0xab4056a23d45bfd8c69703ae178b346a224d8b0f"),
            (12608971, "0x48b87a41799caad62b89655e5cbe1f31e73d6019"),
            (12609248, "0x7055a791aef081869d8ded195d86b7382b283d0c"),
            (12609297, "0xa93eb5b410b651514a18724872306f5ce9928dde"),
            (12609297, "0x649caaf37f36e67d1129c0fd6c6539d390ca2b82"),
            (12609312, "0xa1f15215cb18ffbae85e63d38a6784b514820aa0"),
            (12609375, "0xf830cec53677970f71e431617cdff470b2b872b6"),
            (12609607, "0xbbb210ea3c86892960b8c2915d2347da48a0ff97"),
            (12609805, "0xeda1203d6c685cd7ddb48a645ffb1488ba2bb9a4"),
            (12609809, "0x1827a0f8d912afafcf60a13364ede49d699df84f"),
            (12609857, "0x304c5c17a34da25ab08523546fd178d96a5251c9"),
            (12609857, "0x3c4323f83d91b500b0f52cb19f7086813595f4c9"),
            (12610038, "0x3193dd5bb8ef01c0546e1dc6f46443c3ceb01525"),
            (12610363, "0xf8aea0c8bff29c58f11f8612a80d31e737ac0303"),
            (12610454, "0xb48778964f4b9dd7c2ebeaa587d21f4ef92e0964"),
            (12610494, "0xf26aabbaec5e6431fc72903e43bbcf3245e2ba19"),
            (12610545, "0x9b58ad98f7cb3d6800a9b7c4a3621a45299d0c14"),
            (12610582, "0xe19f9c714bee7fce2b7551661a7b0025e19a3ae6"),
            (12610616, "0x2eb8d2855a71a0803f69c64038092ffd5f1cfea0"),
            (12610640, "0x2999114029d896991ba89200d7b0b3306715e8be"),
            (12610658, "0x0eeb1dccfdb649b3ef82a42d4ea13d35eb72c44c"),
            (12610730, "0xa6c2009e98b1e7780c14e7aee541f67e29863a80"),
            (12610866, "0x542ebce8481c433165a09e1466c1248ea6c81039"),
            (12610912, "0x42eb481a3338563e1247d461477f4cbc97c9a444"),
            (12610939, "0x681abe96a0c8f045825b7467c37ed94bad410b81"),
            (12611227, "0x1d707314b8070d585c103ca9abc0efc81ba95392"),
            (12611268, "0x266cbd2cb95c6e8807ede9422508861b5553ab56"),
            (12611287, "0x56b46686a72642e1510faa42cc6bc1e74258f6e9"),
            (12611595, "0x4fb9d4caa9d1eeada31c6f04532649ec24fa37ed"),
            (12611691, "0x2f201fd67922745797a5986bdf5e631ba13dd296"),
            (12611869, "0x274e120bcde210313f71ec373569cfa673d202d9"),
            (12611910, "0xdbf3d18fd009771f515fd7d155a147712eebcab7"),
            (12611988, "0x98a189ce8ba7ea4caa03e17f41575a9e56396dd4"),
            (12612211, "0x7d6bb60269e219b4fefcfd6f9d577a9c0ad55ff6"),
            (12612268, "0x4650fc0af2503e6490004c89a2c6ce003f6f2f2c"),
            (12612408, "0xbcea9307d1759623a78636641ae037dd3462ff4c"),
            (12612668, "0xfba31f01058db09573a383f26a088f23774d4e5d"),
            (12612671, "0xe3a4f7959f4e4aac08ae3029d3a707ef4ec6da95"),
            (12612840, "0x89af743328a3382b7d67cd919ea92aee9d747388"),
            (12613017, "0x72d9172cb42019c0cc661ec6eb0fca38385079d6"),
            (12613153, "0x21d343f4a2f630e3573c23d738f4c47b582986f7"),
            (12613268, "0x8f8ef111b67c04eb1641f5ff19ee54cda062f163"),
            (12613290, "0xf72c85db72de296825a9dc6bd3ad444d3aaef01b"),
            (12613320, "0x098e0b9de8ed064006d9dda99b4e3d1604801cea"),
            (12613357, "0x135f5d693d7c29778e2164030777b97ac3818c90"),
            (12613378, "0xcb5db0600677afec3d30ce2b21a6430b9e57356d"),
            (12613561, "0x5abf2cf7b37e64bd8a85a59a3cb8a0fff5934084"),
            (12613567, "0x76e7e8d66331e19156badbd132b074051bfb5bb0"),
            (12613753, "0xd7cb275790ecde9879ef23a3240d251c94aee20a"),
            (12613790, "0x1e33923faade7aad8b095dc3298520799c11d95f"),
            (12613962, "0xd4485bf8e93618b627ce560866f75012742dff35"),
            (12613964, "0x80209165ed17286f3e3deef700f96c59870ecfca"),
            (12614271, "0x632f8512166ec65c90a40fd85b8e0d76b2acdd89"),
            (12614275, "0x65dcc26a8a9fa22f792379b83f4a644536b43a53"),
            (12614452, "0x5c60f5434dde9327ea66e5e07e0ed4b5760d2c07"),
            (12614703, "0xdaa106aeeb62be83463f9469caceab56f13b1210"),
            (12614836, "0xf0e0006e5b4db5ed92045fa2035e1db6b9120c9e"),
            (12614849, "0x240abba6ba456e1ed5717da4abe3a4e1ecc06581"),
            (12615002, "0xfe0b65c9233145a5b980c7a8751256b63f0d557e"),
            (12615090, "0xf4b64108726807571fb7f12b7fc06097e0f6f760"),
            (12615445, "0x9208aad8c2e4d1fe7f04aedd5976243692626ee9"),
            (12616124, "0x61f6cfa82ae17259fdee34a69ac34702b51b9c00"),
            (12616331, "0x1a818b27ac1e360ccf059f62a7184c37b5a642b1"),
            (12616415, "0x9410cf01dcdccdb9b56e285b39cea375e5e90883"),
            (12616430, "0x8b1ca9d59467214c2c3a4656de1ccc8e56cd2de3"),
            (12616480, "0xbd3edd3c3ab6edbc7fda76d44e5a0145f9aec14b"),
            (12616907, "0x81cda16a11acfda4be99aed7b28d5a147548ea67"),
            (12617575, "0x5721bbbc78ef5c3c2f03910d79b5e1705494a31a"),
            (12617584, "0x372b9bbbc1486569ad3faed2b08130954dabfbb3"),
            (12617599, "0x3e80118300af563aab1a5c2a3557b8c099ddcd56"),
            (12617952, "0xd2d31517b245c55a1ad8c0a826086321b8e4ac67"),
            (12618002, "0x3d9c69cd03800012f05579230cb69e013689a453"),
            (12618108, "0xa2dffb0da7250b3449b54087ccad5803c416b94c"),
            (12618128, "0x56027930bdff39faf295a7cf6bb94b9bb9f6a718"),
            (12618269, "0xc59511889997707c4e859668fd509311ec49f85f"),
            (12618626, "0x35fe7b8055ee17f46227621b1f63ebbaea86c6ae"),
            (12618679, "0xac5959f244158151f6896f6fc3aa4d1607b33a6e"),
            (12618757, "0xed5dd2141361d874ed9a7c1851ff5c37f5ea41d8"),
            (12618781, "0xf600e4e3db2fc606f2badf1802a073d3c68038cd"),
            (12618904, "0xa3488f9e7f7392dfe3d531201410ba2fc896e759"),
            (12618930, "0x279bac9421e812ec64996d662a4f02d428aff598"),
            (12619185, "0xd80cf5334d85bccb5998dad7ca228c44fcf53ca2"),
            (12619235, "0xc2fe79c010401784d2896f60350bc6150487b43f"),
            (12619285, "0x460214f8de29ab0d862efab42872faf9c875b77b"),
            (12619450, "0x38b7bedfefba07908de66601b2ca82ed1dfef0bd"),
            (12619542, "0x25bc79771b680ac4db5bf6f9186586439136edc2"),
            (12619556, "0x18552d64fcd7e77a56c932d100a887a969934e0c"),
            (12619576, "0x32f04d6c66e2ba4b10c2fda705b961878e2b3c9b"),
            (12619872, "0xeab396eac78ff7622d30eb72c767cc14fb53c132"),
            (12619930, "0xf5ce0293c24fd0990e0a5758e53f66a36ca0118f"),
            (12620002, "0xd1b27271208595dc112ecb482b497c0257b524f9"),
            (12620077, "0x5575ba3d05026cbd53a4862492f8eda6fec8288e"),
            (12620494, "0xaab06ce78ee45c7e2b2b43245f300dbe7680f3cf"),
            (12620718, "0x5dd0c62eb867cc18d965d3e2c13ad479207a42c8"),
            (12620723, "0x4a7ed055fc1bba682c4bc91b4d1be8b7177ba315"),
            (12620886, "0x4ebc76bba018abc76b18afc61c7345ea0af0a037"),
            (12620897, "0xeba7a872781a2ad87533a9374b587e24b1b15a23"),
            (12620964, "0x27a3ca0db804dc38387343ecb3461c1e0185b11b"),
            (12620982, "0x16546a81590a5df707d439c3de8f52f66fb1d4d7"),
            (12621017, "0xf2c055823dd744234694815c6c42f2956191ce1f"),
            (12621450, "0xaf42109724bf4ba4199da94d7ead6b0d4e57af37"),
            (12621516, "0xb0d348d1d67e0f07af199327739259b69c0af2ce"),
            (12621667, "0x119f33c3266dd5ed81a9db08d5033d4052e1e145"),
            (12621825, "0x4aafe5354625189fc8f10e423b57f59cfe11cafe"),
            (12621868, "0x5c2fb3ea0dedc8c34512d13f8125a5f93e7be7d7"),
            (12621881, "0xad260212fe1264f42e478981d937e5c1be9c1c74"),
            (12622621, "0x0c6795dea754d28cc941563ff4ccb73dc47e56a8"),
            (12622760, "0xc6a65109edabf0f2c97a0c6f0c8c511882afbac7"),
            (12623065, "0x13200ec00b0532c579fac40ffa0a045954fb9bc7"),
            (12623100, "0xe60cc75418f595c5a45475f842f535bf27c396ca"),
            (12623139, "0x1431cca1e548f7267798d02f21e7a66f52e55d81"),
            (12623164, "0x1b170eebbaf9caa401263a556728e4ab0d6c1d62"),
            (12623457, "0x6517223d90277e98b7f8be8805d55b0b308d50c7"),
            (12623548, "0x47207677854018bfc63691bff41c6666c911ae3a"),
            (12623837, "0xba28dc7668e00dc3c525e30080ed09b578fec212"),
            (12624136, "0x7b58408927263298237c93d9c1061e90d498fee0"),
            (12624188, "0xdd51b862f5b3e23f5625a303329078fa695eb558"),
            (12624489, "0x9359597abd32f9aa4032eedc81e4030ee20e760b"),
            (12624539, "0xde1cb14bcabf3d50722a1c7a94d041f2ebebc0e0"),
            (12624965, "0x6984be45cf755e93f01397de283442d18403a7fe"),
            (12625140, "0xe3df1cf82757aa29664289992e9ec7e50ec48d13"),
            (12625284, "0xb5667233194805e6972b16e815047c9c086dae4c"),
            (12625286, "0xc7ec0dfee680c9fd6586b00cf739fbc54e9563e4"),
            (12625836, "0xd8b2051e69804e565adad30f3b7c6f986bb9f209"),
            (12625855, "0x9da93d5f3b174a4ff81a923b56c550261d2413d8"),
            (12625966, "0xcba27c8e7115b4eb50aa14999bc0866674a96ecb"),
            (12626159, "0x3f1a6182af41665e6d261efc895335f70e49b443"),
            (12626170, "0x97a231fb21bc68ce00fc2ef641a1553edf8439dc"),
            (12626566, "0x76cd6f1517ed517d6ae0cfef3747fb1ddc593da2"),
            (12626644, "0x5cef3aed38eb937f3dc0864307ac6c9a9694abfa"),
            (12626704, "0x0c7c0e119fc57782cad703b5be9824abfd4ada73"),
            (12626716, "0xbc39cf8218bb4466def33c067eaabaa16c65db7a"),
            (12626768, "0x1353fe67fff8f376762b7034dc9066f0be15a723"),
            (12626830, "0xfdd29aca90c1f7350bebc291ad309f3ce53e7634"),
            (12627283, "0xf41ad162cce26e32a950ef4c5caf87a1069426d1"),
            (12627393, "0xcc60a990451d8180907522e804ba23d79caa7e1b"),
            (12627592, "0xdbe0b9c5006cf03e2f1ae9a536b116750499ed29"),
            (12627632, "0xccd1c0823d32123975d8ae2ad6cdf2ddca4673ee"),
            (12628041, "0x5a54a656b9a29f48cb84241e89104ad6e0f88a0a"),
            (12628054, "0x11c4d3b9cd07807f455371d56b3899bbae662788"),
            (12628162, "0x0981a5c0742323ccb85a8a881d2ad8bc8fc1e471"),
            (12628853, "0xa2ea4d31e02d954aec68345b922bbe06d8f9ba6c"),
            (12629223, "0x0ef368bf1d04bd9c52b3f57fa0a0a56222435781"),
            (12629384, "0x6d8b386880426e6478f34cdc828ebb674cbd1398"),
            (12629515, "0xf2a6566bf81f6875081716ef62033b0a614f271f"),
            (12629611, "0x1b49313c4301b31bf36c3d57ed6c56521fa21724"),
            (12629885, "0x5b4cf5d968ba123fb5d1385c1d792d8a7a019876"),
            (12629912, "0x583cd84b6afabafdf0b6907b29d7f9aee5f2fcf1"),
            (12629916, "0x8fb8aff46cecaec4674deafc209a29ed84c605ac"),
            (12630019, "0xcba33a3311f760fda671f767fec04993fef89880"),
            (12630238, "0x756b3eb01c57cb1dfeddd05863d031b552bd3d97"),
            (12630247, "0xa5dd6f4e3d5bbb5c6ac693409880fb6a322a51c7"),
            (12630280, "0x3c09a1c701e4e2ed701b47202c9e0cf716bfc1d1"),
            (12630368, "0x4f2efa12cd4893db5317b3d00733571bd88b21cd"),
            (12630489, "0x781035bc95c01d3916f62db94dc08318ef8c9906"),
            (12630534, "0xa3f02d35f2a29ea782575ad24477b6a048bf50da"),
            (12630687, "0xcd8a4a877c6a48469fb6869cf63463a613728f31"),
            (12630772, "0xa08e8a43db4e6b05daee70088f43e9df338ff82f"),
            (12630872, "0xd4fe6fb3b725d3bb2d4b5d5a7a00ac7095b48d5c"),
            (12631018, "0x146fd3e41d6c0c1adae2422cd0f1715d51d14c33"),
            (12631056, "0x2949b81ae8a26cb38e43510a1d9889d23943ecd6"),
            (12631428, "0xa71921c2801a58f0a8754d6e6ac353216ebd0a73"),
            (12631460, "0xcadff63f2c7b15b85625e8327b89671d4da86563"),
            (12631463, "0xbed423ecbb85c2abe112b26f751b79085fc9aabc"),
            (12631496, "0x08865164afbd75f1fac3904dc7cab544cab4565a"),
            (12631726, "0xe47a217eed596a06be34b7087fd9b68239d03080"),
            (12631826, "0x4d4fc7e105b2f225dd8df65377edbf42d6d91e29"),
            (12631902, "0x42fcdc36869e0b7b8372014b06a408584473a40c"),
            (12631909, "0xf23d8f9625389fbb2b5b07b06c08a52eb633e97a"),
            (12631959, "0xedfcb689d660ddbe3c9fecbdd58ac58e1478a47f"),
            (12631977, "0xf45b56d95795c900dcbe058b9212d3ae56d2ffc3"),
            (12632017, "0x0adc12baa7366d628c19df0f3c6eec534d3201e1"),
            (12632608, "0x197bd330f8496f48f70bd2e8c2380c0ddf15c9be"),
            (12632643, "0xc8f797e893d9f08db03759213b8d09f0ca6ac9c2"),
            (12632665, "0x309d54007b48a76139152b211b3a6c847943a617"),
            (12632733, "0xe3baa96ad46457d9e6cdd4e32abc11e2c124ec49"),
            (12632777, "0x644567a282cef1fe2626ddcc067d35ecb5c76344"),
            (12633291, "0x3d656dad255ca189d497a7b0f2c4b06ffe5cb9b0"),
            (12633461, "0xaeac4a9ba7df813b5e6b24c0a76b7390a80a28b1"),
            (12634145, "0x6003c66b0c28f6cb0e9eb19f971609b1b5a848be"),
            (12634173, "0xbc6992760dfcf63cca27e881a51f762244569176"),
            (12634423, "0xe0695fa67c7ec7b842906847df7af5c3f370f905"),
            (12634667, "0x24099552f5dadd07c4c3e32c52fd7109e7c72727"),
            (12634671, "0x7dd7342ac24b85c8a3fc5bc9f25182d820cff6c6"),
            (12635149, "0xea8bac5d463df2fef22d97538880c1a45ae7aae6"),
            (12635848, "0x15f198c4de3a8cddbc58175003d8491ab03cf95e"),
            (12635889, "0x393a525799658cc5ae8a3a293489f471d36403e7"),
            (12636375, "0x1e29da73c5410a39085580618045fe81f7d63650"),
            (12636454, "0x3b0baf30ce0c1b8f8be5374f0ed9feb3194197d0"),
            (12636678, "0xb3eebe219b67114bf9b3edac18496dda4402fb39"),
            (12636769, "0xf86a418d0cbcb95a1286422b29aae22c7b7280e9"),
            (12636812, "0xe62113d7568b34c9dfa764b1045424d55c508c7f"),
            (12637068, "0x8dd240195b2cd7c0a118166cba02512f52e9e360"),
            (12637271, "0x39c2c2ab2a9197b99bf5933c1943363713f68e3b"),
            (12637326, "0x682d6e1b608ac293632155b8ea96b679c3663fbc"),
            (12637342, "0xaf215c9b1239f02d1271e0e2796e167ab8300c02"),
            (12637416, "0x686739297d913afa7f1698d4423be0a2cd558eb6"),
            (12637511, "0x1e9934db2730e14d89a1516c534612b6806d55b1"),
            (12637556, "0x4bf528f0e1f4d4c25a9553da0d5d606a715c80f7"),
            (12637600, "0x74c4a3df0d2057b925eb8270b40e1e50e8cfcbf4"),
            (12637657, "0x9608c0637939eed386f5a481d456ac311838fc2b"),
            (12637897, "0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27"),
            (12637902, "0xdf387e8df7ec9ba09fc3c4960fbcfa16197bf381"),
            (12638139, "0x4f94b5942256a8b2ffb2fbeb9670cf962311b4eb"),
            (12638214, "0x9880035e446e57eb3ba902c73d6e7bf3ba6ca46c"),
            (12638868, "0x7c93dc53690f536a91d4878efca7c868aa3f8573"),
            (12638958, "0xcae4dbcf7e8bd4c0a5f931d70d0d2c497ea4e6a2"),
            (12639131, "0x0ed5551eb14288f011b61c13adaf719abff568f8"),
            (12639289, "0xfca3dbe10f380b52df09d61f79bf8c67f5d3d6ad"),
            (12639348, "0x1c15551c7f44bc1fa5210f4b9de6c2d5fb88c2b8"),
            (12639421, "0x31170d84f4cf69c06a719d8fda8632c7fdee31ff"),
            (12639475, "0x37affcb065bdb8da5d53a45a36a94b153ebb3d22"),
            (12639491, "0x300849c3c0c72100cec5123f27d0bc1678014a47"),
            (12639752, "0x97693242c5703900fc7651c9164a334ee5ddfa2d"),
            (12639760, "0x08008a915911271e2b09c08e1cae1ad1c48b933f"),
            (12639838, "0x113ecd438bff3e63a95b0b9d18c38bbf066db5a0"),
            (12639875, "0x38b5c995a73077c9c23dff7bcda73e51c6bd928f"),
            (12639902, "0x46631c701eae6c6f1880cdd6898675bbf20886bb"),
            (12640094, "0x7bbd7053f2b4299c33b2dda276a37fd04398e6ec"),
            (12640140, "0xf7a716e2df2bde4d0ba7656c131b06b1af68513c"),
            (12640182, "0x1e1b11d235eec50737c25cbe4fc680e766f1c9ca"),
            (12640228, "0x9b810932bf7ade6d09f26b878952a49ad22d6a68"),
            (12640295, "0x263ca8f8e139bede4b65a3363c10712509081edf"),
            (12640305, "0x9b114580a694a61ce8df3ecc2e83c8fdad85db52"),
            (12640322, "0x2fbef3dde9d4372c736ca04d48458ba8b2d26b6f"),
            (12640478, "0xefe7b6aa87e79c6125702b4c32e64f2d0d04c779"),
            (12640959, "0x11b4b114637425d0f6828d4e5f6bd616cbc5d6df"),
            (12641573, "0x9a3038904aade45e5dcef9208576fdeb03fd7bc9"),
            (12641609, "0x13895e06dbe15bbcce08a5d22eb59037c57006ab"),
            (12641905, "0xc3917092866025511a167a36b8a6b8afd8d3506c"),
            (12641960, "0xf1c5c6f21a3e129f8e33b27cab2ae7f14c8b77be"),
            (12642172, "0xa9a70603fac1ba9e19f54287b1cc66d5862af529"),
            (12642174, "0xe9131a276fd07af729b5537a429346e8affc67e0"),
            (12642382, "0x2be0d03bdd3efc3c06e8a36a13bcd31ce051d947"),
            (12642649, "0x2c85e17d75c4ad7e8d419c50700d5b7e3deac3b3"),
            (12642681, "0x973b14379d96e9a14a413a23444e181e715ad8d3"),
            (12642775, "0xb53c4a690596d3b8e36cfdbb82f8f38b5cc31da4"),
            (12643269, "0x755d6b149c243dfa5ebb7d66f8c9c611df70d4f6"),
            (12643275, "0xb9f5da4085862402d2725d7b3f14e3823007363a"),
            (12643388, "0x70885952f174fb5396deebb66ce3b4b2adfcef8a"),
            (12643562, "0x3a7694231450f280d3018bd23382c91214048bf5"),
            (12643772, "0x8a49455efd17523e84e83c89404f96f2a91cdc8e"),
            (12643961, "0x1cbfb08eee9c3de4fc62d94b0cacc9b78c7403e6"),
            (12644471, "0x7b3d5316b3df5ee6ee228a3a7ab426e7b9d85429"),
            (12644496, "0x656f699b50d83da37560fa1c26a267db827911ed"),
            (12644658, "0x4fdfc5f65fbc22668323d1f2157fa89ad6d88b6d"),
            (12644668, "0x1549a879d5568e354fd9abddf46eef3701532829"),
            (12644673, "0x38b6e47a97f4680a983eadc8e510c37d73967c29"),
            (12644700, "0xdbe8316dd61a8db59f1e8fe081a616d757581a34"),
            (12644702, "0xf1ac32109bd57866f66bf0e9e94d956bbd4da97c"),
            (12644830, "0x8be72d6ee948bdfef3a584d7541947a1a68c83e7"),
            (12644863, "0x58046ac111c4ed35122a062278ac3eea677b8af5"),
            (12644918, "0x8480d8040bcfd25011b24a2862ff199fadca3bc6"),
            (12644942, "0xb69e7e45d3a195287a5f7bdfdef940c2c470503a"),
            (12644966, "0xaadca3d04b7c039328833eedc6a14dcf18a8e8fc"),
            (12645032, "0xf4fddf973e8267989bc52ba4faba35660e04cb59"),
            (12645349, "0xf129f1224d8317e6ddfeaa70897fda1e467ba0ad"),
            (12645672, "0x2ca3b5143148ef81fe79897be77aaa49d27ac7ad"),
            (12645684, "0x3c47fd8a121b0c7d208871a4e2fa1b8db13b9a0d"),
            (12645690, "0xee7337aafc70a0af093a6745f0235a1e72967470"),
            (12645707, "0xeb121eda08b6fa7e8e1d242bcc970fd6227734b8"),
            (12645761, "0x8146e9a7713df23164f35025a91b96bd3e12a750"),
            (12646213, "0x158dff1d8cc6ce0b3b066ead4b17fd635f1dac30"),
            (12646327, "0x5cc4a57b79f5467442a8e271efaec5a6f412ac68"),
            (12646368, "0xbba38b5bdd5a04ab5a9f52db87e64efc299b6bd5"),
            (12646456, "0x20215cd3949edf87771c529ed41f5d8cce652f65"),
            (12646712, "0x9407e6118ce7d63cca7cdd19f4988c1e0d91a442"),
            (12646762, "0x99f30082206aa6b8ad2a4778cd2c52eaf08f6589"),
            (12646790, "0x29b31183cc5d11348a9244d7963052733505f88e"),
            (12646858, "0x20586c43f0b2f35b25016e65d386252ec7aa13c6"),
            (12647189, "0xc115b3d61aa0d3261fb6c3f32c957b8103968129"),
            (12647395, "0x7194e6561619de4c46ed2b75b997a82c9a80423e"),
            (12647586, "0x0b361e116e8d86e14e86e40cbc9a4e62b7afcacd"),
            (12647987, "0xc1cd3d0913f4633b43fcddbcd7342bc9b71c676f"),
            (12648493, "0x8460563edb328c42c8bcfee1e224a7ad8db02f7f"),
            (12648578, "0x5fb8d1c668db00ed4b96d2742071bcb61194cafd"),
            (12648935, "0x7f90a11261e428db4507b7142bc6203710b0831d"),
            (12649201, "0x895eb0398e103d4cd6b4904081c0d3043e4d0b68"),
            (12649570, "0x3a77624e1a89a0181dbcbdaa3e90e60216632383"),
            (12649619, "0x2b6e15548638748a4858f82e0fc6b6654acdda6a"),
            (12649731, "0xca3d47267bb5c81c86e17d0141535c606d7ca572"),
            (12649797, "0xb60203517b2f8bf5d644667ddd25f1e510074fe7"),
            (12650002, "0xbd11021c0ac901bcd04b5a796744cb8f22fb2b41"),
            (12650128, "0xbe038488942511af642f9be2ae3eada00c4d6fcb"),
            (12650418, "0x727ca1dcd2f5f76af9f4bbf805d041ff4e128167"),
            (12650499, "0x07ae40cbc3c4f2c9d86f101bbb83e26b5dbb9a09"),
            (12651031, "0xb6c05fb8d5a242d92e72ce63c58ec94d93d11060"),
            (12651119, "0x99e870d01eef8cf8ca1f7560f0973768b3c98ca9"),
            (12651256, "0x3baa9468ababcba9e5f960fcdd03463199aaad70"),
            (12651368, "0x0813ab99b5975737b575daa519702ce113f4e24d"),
            (12651734, "0xcd702df8e49b822d9ae32c06d99ea007f7a9d910"),
            (12652370, "0x2cac23c8c913de4ba1f602462e7f1ff5adecb15e"),
            (12652743, "0xf390cd1bee0d5d5a7e7aa60e27be1eb03702b17b"),
            (12652936, "0xedc2930c22455b3f62da42400720567b7645a885"),
            (12653063, "0x01f30d94091a3dd206968b53743bd2abe9cec799"),
            (12653290, "0x727092c368dc04708d0db121350af351a363e53a"),
            (12653319, "0x6dc567b75b36bbecfa895277273f5be5170ae261"),
            (12653343, "0xa8509cbee6710f212dd9534d3e2e0914e6af727c"),
            (12653451, "0x5480e51bcfe56467b0f3c38484162c744d3746de"),
            (12654045, "0x7c291c6120b226f56656f41a889822167d3e6d4f"),
            (12654508, "0xa3eef0db156eeecfa55682fa4b4aee5a3330dd11"),
            (12654509, "0x7cf82e7b1ee2a8a5d629f21a720740ece2f8b7cd"),
            (12654526, "0x95aaee48c5a7f949372c80af8baaf4834bda67e3"),
            (12654666, "0x61ea73f0622e3ee3953e64a6c481f8af4b37d156"),
            (12654697, "0x5b04ab838bf4183565226d4d0858cc6513645b78"),
            (12654708, "0x19cddc17601266cae735c7ca6987c61ca7ed52cb"),
            (12654949, "0x1e9fc75ef5d0065f07578c94160f6a5f2393ad06"),
            (12655210, "0x5c1b75954e938d90eaafbcd16fc9a50423198a0b"),
            (12655601, "0x5860ce2c5a116138d2eda0bdbe333d45b82a7866"),
            (12655605, "0x82cc46f01ac0f2f274a8ca169845514f33fbb6a5"),
            (12655983, "0x83daf1bfc9098c53a47168dfdad755e2fbad273a"),
            (12656209, "0xef6ce1a5a04234871227b09a41db7787f8a4a020"),
            (12656794, "0x94b4ba66da4faa4fe09e17c0a8810d2afee70163"),
            (12656910, "0x63be6a085ccc43786d877b19d0017367636bdd24"),
            (12656912, "0x6bede352f8f9d96b917c67ee1cbcc8a08f26f357"),
            (12656948, "0x890870fad85e3e268b08b64072b4239a6865b19f"),
            (12657081, "0x7e0640c52a35f966cd2cbc4fad578ba07b98f7ac"),
            (12657116, "0x327e3ec8927987180a0132ed64acc0935f65ff99"),
            (12657245, "0x12014971036f0279d08705d1824c30f631c4b7fb"),
            (12657249, "0xec3e950b263f7fbcd7f1c347fc5295c1440bafd6"),
            (12657282, "0x9149dcc71fc252691a388afcd7278bdf8e976050"),
            (12657419, "0x40442495d3e03492eab8aaadb3aa3c238c83094d"),
            (12657522, "0xdddc653d5fcb0904b811f647f063cf3f04593ee1"),
            (12657744, "0xa98cd23d5c0783faaf44ffaaee90355eb9a04165"),
            (12658186, "0x23eb9590ec0e04f9159f4b0beaf3f2c428fdec1f"),
            (12658273, "0xf9e0ac8959a92e0c91d8c1068fc23851f73a13b3"),
            (12658412, "0x75682a6f58a36eb33a7811d5d0d69593a4983194"),
            (12658487, "0xf296ae9160ea3ab2fe86dd4e3221ff8801def15e"),
            (12658704, "0x3a7744f1ccf92ba8b2eb9aa3cb0183c4608246cb"),
            (12659316, "0x864ad3830b8b721c17419b135a4709c3776fb9be"),
            (12659596, "0x9ea6b6a9171e4fc5c795d8046f1e49e664a8e7e1"),
            (12659638, "0x05a77b89bf4ed97c79a64284f6745c1aa358631f"),
            (12659674, "0x7b6f1f1f6296aea7f38fc884a2d6bd118ac095ec"),
            (12659812, "0xf58cd9ce59997780719b86c4a1fe6c5c4c247856"),
            (12659820, "0x5b1a85a10f83a007062f5a579c7cc712a9018e90"),
            (12659822, "0x5179d9c1e8cc980b1f2b46f1ffd59ec2bcc48b78"),
            (12659829, "0x62da6e3d1eb812a6c49f73246429f2188d50b534"),
            (12659884, "0x2a2724027223371a774a4c2edba12262f1b27888"),
            (12659979, "0x7d2aa651d9cffa01d757f6e361fe9b6d23d9d9d2"),
            (12660137, "0xe2fbd728c92bf9889c5407bce744f6bfe53bfc3b"),
            (12660269, "0x569f8402cfaedd0a011c22f4eb19ff2979dc12a8"),
            (12660467, "0x7a849eab28f05f145fcbc284c0bd3dcb3840e895"),
            (12660471, "0x1b67c76bf292f2ae76da227e364fdf4a959896e7"),
            (12660735, "0x8ea48337c6c6677b0973437f2685d17717c0408e"),
            (12660922, "0xc728fa877be8de86b36ae02d35eb180246f4ec1d"),
            (12660981, "0x7aced978a3ad3111d9567f2189143fb3cf980db4"),
            (12662079, "0xfced9e32abb5f71280b801ee36405f03b73e8000"),
            (12662494, "0x0c853ae5f5cf723dc1909032e4cb608d6382d435"),
            (12662696, "0x1314ae4cf2a9440303fef6ae0fdc9ea5ad7a2a37"),
            (12662829, "0x76b524aebe79c540284cf17fb5912713f374fa94"),
            (12662855, "0x6bfe36d9a664289ce04f32e4f83f1566c4712f96"),
            (12663322, "0x56a4bc84cfa563f0caa758fae329036ad33812c4"),
            (12663546, "0x351d935be0e5285fa4271e14f35accf0e488b14e"),
            (12663564, "0xa6e6fecb33b36b0396649caf94660887dbd250d0"),
            (12663585, "0x3d8c22e4838e729cee3400b2530661d14d200e51"),
            (12664431, "0xc0dddb9e67d6fa1a7e604836ca817421f6de99ca"),
            (12664640, "0x3e87d4c24fb56c52a0dbd089a6219b7086d577d8"),
            (12664646, "0x103a061a439cc7fe943e705b54aa25004feae741"),
            (12664764, "0xde77450d0887994364b62a93e14a3a76d2db9162"),
            (12665330, "0x2866d63fc16ff8b3fc77a0b23a6d280fd15a3c52"),
            (12665557, "0x09efbea2789d67d724282b0e15afe5b9e48a13be"),
            (12665863, "0xb9f530b8f0abe33bf16c5926ca9002e49d6181e8"),
            (12665914, "0x020ba87daca38562021260216831f2e98b6d8e61"),
            (12665929, "0x4e697af96911d10548177f8074acf3d0546dc43c"),
            (12666228, "0xed9a6db5f13a3223c832ebf4b3be40b159319575"),
            (12666245, "0x397c344e972413d7be18505b0734df648dfc3182"),
            (12666388, "0x2c38980824169c156ea277fd68a4b430238a4db7"),
            (12666569, "0xa15cc73e881c06d8db06b50b7a3688b763c18350"),
            (12667237, "0xaa9858e89f769587baff519d0ed37ef0a131fc8a"),
            (12667358, "0x820e5ab3d952901165f858703ae968e5ea67eb31"),
            (12667416, "0x664fe12b02743a85bfde62af0ecd7a5bbdbcfd9e"),
            (12667470, "0x586e6636fe041e1b48a0b69273b8c936d1e4a73c"),
            (12667475, "0x63be38b14908c616dc42aa8c0d688c9f7b376d69"),
            (12667500, "0xd753fff9db97c46a89968a85a4809f27530e949a"),
            (12667832, "0x930faa79f651c4409bc6aaa62f89cf58be523fc3"),
            (12667940, "0x2c88707e26a5bf2003f343185fe9c24ae35f3688"),
            (12667959, "0xee94496e0cd495e38157407a88376e531afc9ebc"),
            (12668052, "0x7181bcaf9626abca2e5dd75053e5c60685c2a77e"),
            (12668073, "0x92f9d546556d35dedd85e8ff736f85fb1121f7d7"),
            (12668150, "0xfd2a82bd07178c73c516c249a9045befe12cf146"),
            (12668357, "0xa6da2376d488887965f710f13900bd655ba1ca3b"),
            (12668889, "0x02469e30dd450de2e9de00d49d679f1d1d774ada"),
            (12669100, "0xc686f0bfee4f61161a3af55757a7483002ac8839"),
            (12669114, "0x0cbe2f86e2fd90040ebb557b99f83400bf8f3717"),
            (12669132, "0x972f43bb94b76b9e2d036553d818879860b6a114"),
            (12669150, "0x36f7273afb18a3f2fdd07e3ac1c28e65d7ea8f07"),
            (12669161, "0x55ec9256077a311256b2daf81f70c0992d9fbd66"),
            (12669171, "0x8c620e8de0dc14321212df1ff78ec924a663bbca"),
            (12669324, "0x397f886d3320030271173cf3d7e4fa98fb635613"),
            (12669356, "0xf7fe4f38d00e8a80c80761f51473d20efe8fe621"),
            (12669424, "0xaa4359c5bc7312b637f2c68297492c91756e8e4b"),
            (12669500, "0x61a07c552b982f09e9fcc0e5207ba4ce48f74f9d"),
            (12669612, "0xf3d0d3e351a33c311fd7154e87a68ddf2cc9525f"),
            (12670237, "0x06b2afb23ca733b5cd4bd0de65a7dd91db6fa5e9"),
            (12670804, "0x3412308d836e6612600064c39e72f1dfb3125b1d"),
            (12670851, "0xe204b380ba75642fe5955b77c3069684e2edd223"),
            (12670936, "0x01ceb1bd07d19d2c67e06e293be98b25a71a9cda"),
            (12671510, "0x5e40e5d3dbedecdd8516d1791be295b2c7912a18"),
            (12671733, "0x330c461cee0c94beafb1e4e852056d61a78f012a"),
            (12671909, "0x642dece2a0ee89fb2e00a2d195aca4e8eb542b48"),
            (12671977, "0x83d3f1de3069f83257ca3cd910101d11ddd555a4"),
            (12672167, "0x154f070cd3b002e18199dbde76040cc0ef23430c"),
            (12672235, "0x60f117d27ba5634ff8aa4f64a6b5ad378e732ca3"),
            (12672337, "0x6dff3e46bfff8fa458c9657d0b5bd7bf71d72adb"),
            (12672599, "0xd5639fe7be49cf00d3b95538095173fd24b3e9ae"),
            (12672733, "0xce3cbe911dbcb8e5149abfe53d7e2390086bd576"),
            (12672738, "0x1b4e2ebfdd354c22d745c2c202f143b3e727e529"),
            (12672768, "0xc1fada11c4276611a413ea8e6e0a125e7b2936ce"),
            (12672785, "0xf4fdebf150ac5d13e7284038ef43ede95841727d"),
            (12672809, "0x542cbac773d188a32970664b5521f38e69ef9dab"),
            (12673434, "0xde1c437bf9c49ff9ce4231b093a3f413ccbf03c9"),
            (12673723, "0x5025f83c0cc0ffab3626561d5a9ee56a5f106bfb"),
            (12673724, "0x82142adc481941d59cd81c7bff52bdd0000ac805"),
            (12673737, "0x7379e81228514a1d2a6cf7559203998e20598346"),
            (12673796, "0xa9ffb27d36901f87f1d0f20773f7072e38c5bfba"),
            (12673810, "0x5908fd9946ef820a932082f6df233afe38184a92"),
            (12673938, "0x1ef1d8866987a3a8f03717a8f34ed59a8e58e8f0"),
            (12674609, "0x40706db5d75c533f306fcb4f2bbe8f10d11c1ee8"),
            (12674751, "0xec13130ddc44d7f330b637abc315710afcbe4049"),
            (12674954, "0xa6102d2cece8df0350fecd2261d2c09074182bc9"),
            (12675026, "0xd73ea444eef6faf5423b49be3448e94ed214f1ec"),
            (12675113, "0xf25b9f70f7e1d9a2968b42de00efda666efb2de3"),
            (12675600, "0x9262a24ebf46736a06610bf978d82d49eb4d61ce"),
            (12675720, "0x7169ff7e7b3836534f20404cc5c7af4769dd4ce4"),
            (12676072, "0xe63a3d5d02247e11c3cf99450bdd997e36ce6b4e"),
            (12676238, "0x05ce5c50d06c5e5b1899c17d80959342cf7c9842"),
            (12676399, "0x933d1aab4ea454a695c8be1fca69c1b380cf22ed"),
            (12676448, "0x8b550f58b714f15d18a970241d6db2563f0c5615"),
            (12677914, "0xf1666c2e04eeab81b839e77b5d2cc85bac380994"),
            (12678113, "0x97ae48ce13621caba175ba2da9725a05be6d5512"),
            (12678980, "0xf80071b5f13f7ba7504752908ddb449be7e21cc9"),
            (12679241, "0x314f6810701a9e6e4248e28b814ab4ffe1fe44cf"),
            (12679250, "0xd7fb75545a0816e0dce70925c8273a119b0bd351"),
            (12679377, "0xe05e653453f733786f2dabae0ffa1e96cfcc4b25"),
            (12679507, "0xee78277b40faba38b4a512faed45d6ab9feb7a46"),
            (12679517, "0x7636c795c72bb9e034b10fe40da413e36318b543"),
            (12679544, "0x3a8027b37cd119cde72c1086ab5d7de912f61b61"),
            (12679661, "0x665019a4db37eb93eac66a926e549e3adda620db"),
            (12681960, "0x9978353276843589e3534c2f527e7b559267eb27"),
            (12682035, "0xf7e79a61d2e11cea1fe732e6de1d922617a9bda8"),
            (12682106, "0x91550a8e20fb38757032fae1d7429e5398954628"),
            (12682140, "0xef2ec1b275f7efbc66fb096bd72fa982a1bdef12"),
            (12682466, "0x5696c2c2fcb7e304a5b9faaec9cd37d369c9d067"),
            (12682586, "0x8ca18da3875310a624387061821fe849c25731f7"),
            (12682674, "0x7f8a708bb89e600d604d5450dcd66456fca8a669"),
            (12682763, "0x817b2d1db390cfae6011f3841e34321ba3d6bf3d"),
            (12682818, "0xd91e569e4d909c6d5399107ca72de24192a0150f"),
            (12683702, "0x46aabcc9b2363e9b5b904d8e64d3ef002f2724b5"),
            (12683708, "0xc25a70151578fc52c3e3e5d379249488abc4ef71"),
            (12683802, "0x7997b878d29429ee4390a384f3137fc94b410258"),
            (12683929, "0x69a603ecbc8bfd5dc7f163e3805fb70020f1eea5"),
            (12685175, "0xf9ffac7c6ee0fff8bb2c38ed40f3d77213b75341"),
            (12685570, "0x9adc9a2b43407439c765a00e52cb38781d14f8b0"),
            (12685769, "0xdcdc549f197c46f46307f5d47092f90a6b0edaf3"),
            (12685951, "0xa845302082a876a2baf91c5cb82027f0057ffee9"),
            (12686040, "0x26a2ad2f1755110e89fed8a969d0cc988ceae3a9"),
            (12686092, "0xfedcd28244def13dd8633de332469a2064549d9e"),
            (12686102, "0xf20fd91626c84029bcff5865433a9455ef17f994"),
            (12686198, "0xbc72bcc4bf4463d3fd1b51709d264b42da9a0dcd"),
            (12686491, "0x596101125094f9d65a579b2cfc4c26059cb64e81"),
            (12686551, "0xc9c32c18b69012de59d6240c41eb1ff023d44540"),
            (12687061, "0x3b65a9779f74e47ff443a7df2bc08d7993bbfae1"),
            (12687254, "0xcba5a9496a9911d54dd3ce38f2ccaf457f9780b4"),
            (12687366, "0x9d7c36f6039cddbab178b74f74945f5e74cb77a9"),
            (12687726, "0xadb3138cbbbe11771c9d24d10cc2cf159b76d24d"),
            (12688091, "0xd34e4855146ac0c6d0e4a652bd5fb54830f91ba8"),
            (12688517, "0x477e1a178f308fb8c2967d3e56e157c4b8b6f5df"),
            (12688555, "0x6f59cadfd7aa2e0351bdb58b3dc89f7547bcb76a"),
            (12688604, "0xfaf1449dd1b32da60da8b6cf700fa31c26d4ff3d"),
            (12688709, "0xb66c491e2356bf32b7e3ea14af7f60b3ed171a22"),
            (12689021, "0xf61d2d408b9e6369f8e664b6dc8a33c23f4f6567"),
            (12689116, "0x4a4f47f818932a712552ef8035252dbcbf13c9e1"),
            (12689200, "0xe3e391fcf423f4b34a65a1f6867b95ec10d8efa3"),
            (12690227, "0x4b9d0663acb7a774c327b679d423993c2a11a263"),
            (12690650, "0xfb54d35cecc75c1fbe0e223bd9e5727df7e3c726"),
            (12690919, "0xd3dd65f0d1ebe69e727db34fa3a25b4fd5abf978"),
            (12690938, "0x049fc8cb0cb23e95836fc70d030f77e005e04ab2"),
            (12691014, "0x5fddcbf2320314e75ee254005302d52be3396906"),
            (12691348, "0x7dfd5ca02ac91c77625d4b343b517c99eafd60fc"),
            (12691381, "0xb18c5c844e4264a0f000aba44681c6e7955655ac"),
            (12691462, "0xcbcc3cbad991ec59204be2963b4a87951e4d292b"),
            (12691613, "0x9c73935fb72a606b14c03b9a10abb7f2f66b55b5"),
            (12691646, "0x3293033bf7fc0ae9c73bbda3f3b225ac918945af"),
            (12692058, "0xdbf82f4ff5cea23717a7d228e7e1a341cd0d5887"),
            (12692072, "0x4545bb90af63de63d6e1e3c991d9c2d52e112cde"),
            (12692129, "0x858e3fae94df672f4709dc1396ee2cdf965e4cad"),
            (12692159, "0x5d511024d91d7e274a640b0224c7fd547ca1b178"),
            (12692625, "0x64fbcf1ac0b3f7c780c1f7ff4bbfdfb2733434bc"),
            (12692994, "0xe4b3782b426cd3355d4fd193066096203f179362"),
            (12693086, "0xeade42d91bf765fc84d8331454da7171ad10eff7"),
            (12693194, "0x3758f5ef412d757123a390ebb3c74eee580f2da5"),
            (12693229, "0xcdb4e35b235845ae8d0c27f77cd8176ddfc26965"),
            (12693462, "0xb7c04cd22ae2041aafe0515b7f6a1e61cafea74b"),
            (12693816, "0xc5c0d1a0049b07fb3a240f67b8a825fe9e8959e7"),
            (12693926, "0x5e6470d84f0a82fcc98a10267f1aa217cda466e2"),
            (12694072, "0x720075e41f71ce4c50552c197ff9589a47f55eba"),
            (12694174, "0xc1409a2c5673299fb15da5f03c27eb1ac88f7d8c"),
            (12694286, "0x9a29ec6a58b6863d6c81aee3f1157dc0a336ca71"),
            (12694416, "0x2e553ca96a66b4ea875ca3ca99179b001ac47bd4"),
            (12694472, "0x39559179b2dd7336caa84bbf9768dea8e7c0a1e0"),
            (12694570, "0x08c213b4476d260d3c42fd2f34942356be0297ec"),
            (12694666, "0x929dc0f2f03da6db5f8b0344f31007c4336a2856"),
            (12694755, "0x6236f4a39048a2e0098309507e0b33325011aca2"),
            (12694920, "0xab658f68d381028518a1306a43eb20de67238bc2"),
            (12694953, "0x4dc5959f2f8785f729cbf1699ec6f8b2cb5e00c4"),
            (12695135, "0xb41f385b582b7a8424496ea0b28e181d55e106e2"),
            (12695217, "0xe9948da5e9f7ccc944d75c9b64da19f447c5f739"),
            (12695448, "0x295e8e08cbecf15514f3b966dd8464503755454c"),
            (12695577, "0xe00f52818856e9475e705aa1197a52524cd7f485"),
            (12695802, "0x0aad82cfe04ba15b046006355970fa70e9193741"),
            (12696191, "0xf82133f0f18c383321b7b910ebd77288dd0a87fe"),
            (12696393, "0x86750e031e2d47f10793b2c4894c70d8ac05a00a"),
            (12696478, "0xc8bf6f3d461a0b9304248071573ff434c719cf55"),
            (12696539, "0x9c9184047a84b367b94806962b7b640e6b1b8284"),
            (12696642, "0x5963f8e5b20ace84a297aa819c130a2b4308249c"),
            (12696809, "0xd95b8ba6ab4a3b40c11bba2a309c920fe5762174"),
            (12696948, "0x823a2794a7496827c84a2cb268571769c73a4368"),
            (12697088, "0x393537d399ab065e51b66d371ef47b7eae444019"),
            (12697198, "0x1132bc7f54d7e9fd9ff24bdbd06213d063777e6c"),
            (12697314, "0x40ed419e373adb0e985d1ec1defa80ffc667e8be"),
            (12697374, "0x8bb30decde214bab4b5dced3c93f74397ba10ffd"),
            (12697428, "0x8b52a568b6c653ccfb4e122dc609e3795761025c"),
            (12697868, "0x9081b403b02282acf28b59a950de98d091de940d"),
            (12698238, "0x58791b0445d3d37699c1b1ce03786a728a5b2708"),
            (12698347, "0x20be2f77c30339effb27bd4521f03c9d37005b35"),
            (12698391, "0x04d581ad0588125d21a5be0e76e4bcd617d04b90"),
            (12698599, "0xb99d1b5bc3341055384a4686e2616ce70ae178cb"),
            (12698777, "0xf41e3d2a9affdb98d5fde692aba35a7cc3111249"),
            (12699304, "0x62897ded963a215f5c03807e410dd6f27efb8131"),
            (12699653, "0x54fb732ea42b90d17fdc0c578d7edde12e429773"),
            (12699706, "0x10203c7f3407ef69277de78569b688accc930169"),
            (12699899, "0xbbb8a3f341c998c40553cb153508c3b0ca512493"),
            (12700108, "0xb3244ce4f4f84e7cc602243dd3452d25e18e308d"),
            (12700238, "0xe82425083be8e9f574d78ffeb1e2d94a6a27c99b"),
            (12700373, "0x9e7809c21ba130c1a51c112928ea6474d9a9ae3c"),
            (12700391, "0x8586df7a945290bcb06bfa432b2630df6d614e6b"),
            (12700770, "0xf32c8705ebac5658f9d003083d92d075d935559f"),
            (12700833, "0xdc06c08fa7bcb031d78fc2e406ee7cbf3db01c82"),
            (12700911, "0x141ba7991205b8e7636d64fb42cc053d03113ca2"),
            (12701036, "0x036dcf37daeb6d8b137413882c4742444a8ea65c"),
            (12701118, "0x6a78b2a503ce19f6b96615c33ea9f4eda1f4643c"),
            (12701209, "0x03dc256087553af7871090df00d0bed694567e8e"),
            (12701272, "0xf80efbfa06d0a3499f688026f83c77c99aa54f74"),
            (12701463, "0xdca19fac93729e44ccfb877fc570af7155f0f779"),
            (12701512, "0xff25255890c30673cb8a194ad0de4c9c816cb445"),
            (12701934, "0x2195d6b4e2c561dfc7393d1f141df83967ab07b9"),
            (12702217, "0x6681ea692945920110290e6bdc261ba17060b078"),
            (12702641, "0x5034f6228ed3377cf5305e8655792efafce1e890"),
            (12702753, "0x7a5c1e09b7ef986c7442314f265a1f1cbf2cff5f"),
            (12702861, "0x6895219628ad5badfb1611df38337b8911adb1cf"),
            (12703101, "0x9ea1da773545d27217cf5e732306cb2b5fc100bb"),
            (12703102, "0x7b23fb229dd50d2e998e0c95bedfbe588fc96443"),
            (12703133, "0x06474ae21b7eaec0916900a6a839aa2e0719d2fc"),
            (12703176, "0x094a28b22e1b4218d590ea6fa916b3c5e670ba55"),
            (12703231, "0xa3991f0fe775b00d577c41f3d5dac488b9b23dd8"),
            (12703242, "0x6d06243c6ddbd5e651048e28107e7afa1110d1d7"),
            (12703275, "0x5d1eb44f49cb9837ca6aa3f76a1916900bee1d13"),
            (12704073, "0x5226b3bb100e2fb8ccafe5e189a3779a2315af88"),
            (12704173, "0xf766436b551d2acb09b73d126fd49869541dfa26"),
            (12704836, "0x9445bd19767f73dcae6f2de90e6cd31192f62589"),
            (12705334, "0x5cc644472ed7d3198eeb23353bd9236ca578a895"),
            (12705451, "0x520049bcde9029db35628704ccbb44ace1d1dbac"),
            (12705479, "0x5706884f6e43f66efcf296cf1e17b994316fbcf1"),
            (12705844, "0x75264242c731b2e872c40d61539c4dcecc4c0110"),
            (12705844, "0x48d8257ab6276f87668cc12194a0fb1fa1bf6d10"),
            (12706032, "0x09b49185cd513924a11d5f2d37c64441a20a381e"),
            (12706312, "0xc68b83d4eafffb2b98405657a21be61cb63f9e8f"),
            (12706496, "0xc2aecaa79e3376f9b058c13d2718b39a077555df"),
            (12706505, "0xcd43d79840400db7c29a8e121c26fee81c4d9010"),
            (12706596, "0x0fc5c10b3f5206dd05c04bda6a519deaf3ec52d5"),
            (12706607, "0xa0cf710f25e413c1e9c76ed6b7953b5d87b324bb"),
            (12706644, "0x3a65361033c8b172a910272d9ac0137147dbb95f"),
            (12706647, "0xdfd3674a232882846fb29354a837fc816a719af4"),
            (12706750, "0x949f0e7aad6c2ff1cba54b577b05d6f5551fb5d6"),
            (12707027, "0xc89593185760b0cc9afd6e129be17bd7f1f2d25e"),
            (12707109, "0x9bd730ddcfe6b5b9ecba1cf66e153adaa807e238"),
            (12707169, "0xc28cb6006fff11792355708cc54bb7e68813a7fb"),
            (12707348, "0xad7e0d5330305a284e266b23d5a763af136f62cf"),
            (12707458, "0x667860e79188383c861d797733c6fcd54c02acbc"),
            (12707530, "0x22b2267f1a5e14efebae53db484a1f67c2803107"),
            (12707555, "0x370568ba394b1a072b0a3313ad6ebab0ea5d7c7d"),
            (12707628, "0xa11ec86cf0ea90caef6e47aa970f1306867e5740"),
            (12707646, "0x1599683071ce8f3fb4603f0b66b93c22116c8439"),
            (12707686, "0x83296bc20ebf8be43d4071f47a20e790c2332f06"),
            (12707772, "0xca518236bd803566aaf28ed7ae3c00ff9c1188c0"),
            (12708016, "0x6b93824ea76ed90dbf85973ebe7cf531278aea7f"),
            (12708298, "0x3c1b69b1a15d15b95a4b6525db43f0b3afcfffaf"),
            (12708315, "0x0911f29d37531400eaf38b212de2cc4063330159"),
            (12708712, "0x033fae28f5d661b911f462b0da19451b9d9fda8d"),
            (12708723, "0xa559799a2c2966f21b4198ee1be70e774cf0d26b"),
            (12708725, "0xd3058e90522c955874089c5617de0e10b727820e"),
            (12708968, "0xc4d9c69962ddb2388e1532279704fc6eb199c963"),
            (12708987, "0x06afd02f44a6c976fce90ebb17c0e14fac9ed9e5"),
            (12709036, "0xf8a4e2b93314789c6693369e9616878293533a1b"),
            (12709154, "0xc4d0a51faf72feee91f158855de828e44765f6df"),
            (12709254, "0xc2dc6589c16d230bb542c782d2a5e964e65a35c2"),
            (12709397, "0xcd4391c8a8a6b1bf2518a0b90e3dea212cbc2a75"),
            (12709474, "0xa721ab9c04e812d5a3d454677e5ccd629555a7be"),
            (12709498, "0xccc06400301acfde69c6e748de2d04ed5ab42ad0"),
            (12709957, "0xc15866aad52cd0e237c5f87c04e3ed22d1c94451"),
            (12710033, "0x15e7f2cd6f5c19c9a362c9320e231450d0e4a129"),
            (12710213, "0x42b43738db5252308f83400dd8c30754c6d0764d"),
            (12710556, "0x6b6466b93daf9ec664e39e6b0f3afcf6b26ffbba"),
            (12710686, "0x5bf6b4bb92d4552a5874b51cb38190319a19a1ac"),
            (12710699, "0xa81f2d6dcdedffdb122444197c7c3b0336e91571"),
            (12710916, "0xdf2aabc4fd53419575eed4f135d0025bd1f1adc1"),
            (12711244, "0x2e11dfaefb6851a9bc8ff2a4b67911c7694a96c2"),
            (12711531, "0xea657714cb00c4fa73b305891001e2d320585277"),
            (12711585, "0x88b094b4018def3968ae17bcd9843964f3c6c1e5"),
            (12711851, "0x668fd4a51bc62dfc83cf5439ee5fda897a8733c6"),
            (12711955, "0xf01efb849b4d4d2f7ceb8809825be1adf37227b2"),
            (12712023, "0xdc6e2045d48399008f16c0c1409f6779aa74ef36"),
            (12712119, "0x4b05b80770a2d4228f86a7b510184e7f085049d2"),
            (12712243, "0xd0753968ae358715303112dce905c60e18e05892"),
            (12712437, "0xc840464a8c3324e0bdc9429439dde3a12205424a"),
            (12712768, "0xa85bf2d2138ff99a5f69f8c99c94ca11985d8c19"),
            (12712841, "0xeb5c182c42cce1a4ae09884e022ec99918b76267"),
            (12712900, "0xd2cc51adf50ff9f774c42fc492ece81cb8c98c46"),
            (12713211, "0x3d3a645a4edd9cce81ff15941042d6f08f9b053f"),
            (12713259, "0x0b732e2e0fb07c5dec674932895beb4e3a7ccf2c"),
            (12713272, "0x8ca580042454ddea4f09fe99dcd189cc3464fa63"),
            (12713418, "0xde9b30b68496cccad21d716085d7818b8bd311d6"),
            (12713485, "0x7e6709125dae37fecbbb90af74bff0a8bb7e0cab"),
            (12713604, "0x41454f14bde226c035b99901e4e98df59aefabeb"),
            (12713908, "0x4c93cbfcb686217e7779d0dcf6604f9378735be0"),
            (12714283, "0x8af3690525a79f0fe27ebc8527a00307a45eb464"),
            (12714983, "0xa324a33a6bc402de1d81b7a501bdf8ac7bf845ab"),
            (12715312, "0xce2bbbbbcc771c0f8f83f8a1596753af10663b22"),
            (12715571, "0xb279dc80deab68c85b7a52dc24edcc638e8b2604"),
            (12716038, "0x8f0c27ba7bea85a5ce52dee6030d3000d36641a1"),
            (12716263, "0xd24b1542323096ccbf9cba3b13c5b9eb4a92c506"),
            (12716497, "0x8d3c733c7d10e4f99dfbf94a4804b2e81a1a9ab8"),
            (12716515, "0x2f987dcc348d81e0b482e4970392d1ed88ea2f31"),
            (12716708, "0xb0596f43c890dce90ee5ce8c61f5ffbb9563f007"),
            (12716874, "0x6a3e98be2c7fdf5d601d250ee8a46cbad889a851"),
            (12717385, "0x190a6e305990f202ac98dc3a76500f339a35982d"),
            (12717427, "0xaf1291730f716e13791d3bd837c7c31111a01778"),
            (12717778, "0xb1de756d038a7f5d5f3076151304d4cc225cacfb"),
            (12718235, "0xc8ff0c9e78157a92db86306b16fc265faa3e4dff"),
            (12718271, "0xafd9e146a473bc32c21075a0e6089dec85d29078"),
            (12718403, "0x2a65bb152c49320ad700194292a9fa1e7fa155fa"),
            (12718552, "0xb8f68a43981f83d322da61c29aad2ff43cc71ada"),
            (12719042, "0xf5e40fe622b06c2f08d179ccd77b192c124d238c"),
            (12719050, "0x594e705dd661ba25da3ddb656270105091dd373a"),
            (12719058, "0x855a765d9dc0c30e83312a86a7c7e32506f0657e"),
            (12719061, "0x515403e755c2eb7f486d379d478efb3d9ec528e5"),
            (12719111, "0x49cda447fda2b42afab5a12750374b268a6f4d75"),
            (12719519, "0xaf9c98cf6e5a63442d6599a44f113fcbbd7c9338"),
            (12719556, "0xdd021d943c7973fb877fa15950b2637810db6b2d"),
            (12719613, "0x675e0f9c03455aa50b04c5f80ec640005e8ce04b"),
            (12719615, "0xe8952ed8f9c175a654fcd399b63e73b24082de32"),
            (12720380, "0x4b4a52e0d0601e1f639441206967fc98ff21ec79"),
            (12720763, "0xea31faaa17d4c5cc7d2f459182b8b7ce6a3cf2cc"),
            (12720865, "0x7ee092fd479185dd741e3e6994f255bb3624f765"),
            (12721097, "0x03d886fad87b2538ea1fcff8fd71fde2ff727ab2"),
            (12721519, "0x7501b6a57c9660b54150724eb11fa5968f3ff4b6"),
            (12721850, "0xa7d9d72bb2d14bdf93baa2cf8669385089203f1e"),
            (12722064, "0x344b551110efd57b6c8d7a80daeb794b14093d78"),
            (12722141, "0x76ad89d68b81affc636b4b4b23efb426f0150078"),
            (12722226, "0x51d1c573c9c88d3663460ac83a96a7c0b3403177"),
            (12722255, "0xa295c1301e063728d733f9fb2155448fff9c70b7"),
            (12723565, "0x4429d161f86f94180ecbbe71963d73ea74f5212c"),
            (12723850, "0xf6866aaeb54042418410d1333cb489f3650dde5b"),
            (12724437, "0x9e6756d73550f603eba52e98a6d461fdf4acae33"),
            (12724447, "0x87fe5b001444419b4751ce713a87f8f2aba36bdc"),
            (12724933, "0x412940fdac1214fc3df430769f54e69210a18e49"),
            (12725382, "0x3b830a5a91ce2768f9439c4af54f29d572566624"),
            (12725511, "0x600e9b4d0e18dd5922a9db3134ec7d1af85ea009"),
            (12725886, "0x7e72e3ea3b2307bd1164bdae244d40b1e820e4ed"),
            (12725960, "0x544abf323863977ee641dfc7e4cb91bcc0b6c959"),
            (12726075, "0xa39251273f88fe3847aa45452e7fae8e0188a966"),
            (12726380, "0x3d90fab05130fd9152aabdd26e9910bd0cec706c"),
            (12726412, "0xf7f6c093bacc5604a0c7f09369f5ff29c7cdc743"),
            (12726576, "0xf9fb2bece15a1b3eb9d822126f32c4b050615f86"),
            (12726609, "0x487abf6dad66168e891b90925d401899586553c8"),
            (12726791, "0x7da6717b5fee7b6e879e5b238533659884679b6b"),
            (12726948, "0xc843f93c02a5236fe19da6278dda4448f79bce98"),
            (12727026, "0xb109b9a5e0dd17f89790a48fd3eadd5d3e3cdad5"),
            (12727147, "0xdecb25e9fa611a5ec28684ef54165638fa1beb25"),
            (12727238, "0x30a0e8811b2a04d24c9df1ddf254b2d2f8760d55"),
            (12727446, "0x8c1cb0cccb941dc1083d1b73c14cb350a11a426e"),
            (12727888, "0x059558947ae15d4bacbee50b7a7c81633bb37101"),
            (12728244, "0x1d84f218038e78fce2e447623dfc46360d8ab5a4"),
            (12728374, "0x4a6b97888dfbe85049d6b8b379c0d82c29654b81"),
            (12728439, "0x5418cb047a7665497003409cee3de2ed02b896a2"),
            (12728694, "0x68f5d4009f9001c8d00ced56c1e90702c998cc95"),
            (12728755, "0x9e525e1784165a766f23e5e0f4beec0f3a8e76a0"),
            (12728772, "0xc274115fbc126cc6c14d724e529fce64c9e7a60e"),
            (12728790, "0xa5a03c5159aa898bf81834f6b0cc105ada0ff9d5"),
            (12729158, "0x7111b14f92f18a1d39ff7c26dc4b654d208c8185"),
            (12729241, "0x8c8236487cbcd5bbb732ec38667ca6cc54fefc88"),
            (12729710, "0x17d6d786d9fd2c70ee060c5fd48f5d5db31db5fc"),
            (12729840, "0x885bff2f17d3719826a58672c7b78356d191c5ef"),
            (12729880, "0x13dd5313a8f36c9a4de3022839fa7976b117d466"),
            (12730262, "0x06e480786d30ab18ce066d8099f035bfa0c0adbf"),
            (12730403, "0x82d5b441a697ecf989d61136c3fb7a77120e6a71"),
            (12730578, "0x87986ae1e99f99da1f955d16930dc8914ffbed56"),
            (12731116, "0xe459d33ead5a579b81f2232d02b58119eb4a2266"),
            (12731159, "0x1be6302ba8b61c779d02fa90875a8ee56b528fc5"),
            (12731185, "0x407e6fbaaf8f0db44ab90c68bbbbf5817529aac0"),
            (12731560, "0x47ff5e99e694408afff9eaba1ba65ef47ec9ed1f"),
            (12731788, "0xa6d825d424742a01076169786cee46135eb9e285"),
            (12732009, "0xe7f5c39f714c0d777acd3769c3b6cb90c9d09946"),
            (12732119, "0x5eaa74a47bd8a60db45760beee4338564e18bf01"),
            (12732390, "0x7c36e5ff10752fd632f6bb2a2a30845faee52d89"),
            (12732771, "0xbdcf57a7e5a2cf228294b22cdab3cf576c812c16"),
            (12732983, "0x6497e7d437ca702a85e02c212fad087f2458a882"),
            (12733042, "0xdca901028656ed1b4e610c68de6b147ea217c228"),
            (12733548, "0x74a49e7d45f5cf02b90524f650ed6d3a02dadba6"),
            (12733814, "0x6bdf9d53792c205041bc14980c795b2d43c381ee"),
            (12733864, "0x0e10ee115fa6252fe72e2590c3a5561941d67be9"),
            (12733972, "0xb4706a0a67e8d6560274cef0b5b6917538fa7984"),
            (12734713, "0xfdc3456e1efa79dbab60a63eb8536173e9350a61"),
            (12734751, "0x1184bee1b6f5bf6bbe0d2321d302962eee270711"),
            (12734924, "0x162ca4565f6631adeb0c6343ec03131c8303625c"),
            (12735503, "0xe7c1be1216975321d81bae09a1c8c57f413f6f94"),
            (12735583, "0x15a7e01386fe8be69e486edbc55baac10629830c"),
            (12735625, "0x5977359ee049675b9c77edf3c7dc2fb4216df21c"),
            (12735880, "0xbfee588a1283aa680d975a8e6e27e83e43976119"),
            (12735994, "0xdadcfa1359216f92338632de1bbf557d98cfff38"),
            (12736183, "0xfeec357718e6195ef72c365580f1e4722e9d55eb"),
            (12736257, "0x8fee780a753c39f8448ed6459f330b051e7a05a9"),
            (12737096, "0xd99d095bb919f91ee637cbb1001ff9e4b1b919cf"),
            (12737099, "0xc8d8b32edb1bd674334ee2374ae598c2d6db79db"),
            (12737203, "0x8e794abe66a4f65c0b6369ceb91af9d50d78fbfa"),
            (12737839, "0x5b03f478f289f4d4823a2d1f650e8e49dd2e4b2e"),
            (12738041, "0x4bec87cb126de6c1f8b410e32d1f4ae472fdd83b"),
            (12738126, "0xf0577454fe8bbc6652f8c84399e07c97893f81ed"),
            (12738265, "0xb20e9d746a602847ec0f3dd6d2bd76110e883943"),
            (12738632, "0x810249fd442c25fe5ed7cc89c53e96d9d997f021"),
            (12738856, "0x5f9514478fa800e9a61ebe85d922c26406d26115"),
            (12738870, "0x7c105c8c0c7f74f99e1c8f6e521737fff4e84501"),
            (12739076, "0xddd557b3cec65df7b7c4665332330130c378c88c"),
            (12739459, "0xe39bc30f2953ade323837c62f6705658b78e29b3"),
            (12739588, "0xee65260e47b4a0e5d2881148fb1caa6860b16609"),
            (12739947, "0x06bd1af522f43bc270203e96a019b4779195b870"),
            (12740245, "0x74092c7c0024feeb3839bed5ab6189eed86ae856"),
            (12740311, "0x86bf85216f9a903bbac935b71da51b694d2aac74"),
            (12740329, "0x35925cb5f0dd948f561527e7774c52271fe6989c"),
            (12740515, "0x9f440d14cc51bcd018555fc62a771a516593b44f"),
            (12740548, "0x325bad3656f8dca31714c4adc8c2328c87f747a4"),
            (12740909, "0xb3dfd05310ca831722c0269044caadc9e2021744"),
            (12741012, "0x72d49e8810128ac45eefc0311d9d6be90c8234b9"),
            (12741126, "0xd17c6056d3863928f072ef0059ae215935eb8cb8"),
            (12741887, "0x882d6e6ff3dd701c571d71794e31af511572d769"),
            (12742010, "0xb03f87e577c4fe4685cf2c88a8473414bb1d04f1"),
            (12742360, "0xe730e98515a074b3e68d44ef8b897e2791a104e4"),
            (12742683, "0x9ac681f68a589cc3763bad9ce43be3380696b136"),
            (12742861, "0x84383fb05f610222430f69727aa638f8fdbf5cc1"),
            (12742914, "0x71d4c0837080dc6dea591db7aafc011dde540d67"),
            (12742921, "0xafb40320fb0c77176d73ca6452daba080a5f04ab"),
            (12743057, "0x95e706283aab6e6bdd21560c27a06d63530286d9"),
            (12743092, "0x9c6c1e13d01fd33bf98770d5d3a7d2f0928f638c"),
            (12743136, "0xb8f903520efdb640b2859b253f2a9ab472a27c29"),
            (12743187, "0xe59deac4494eacb128d2ec063f16556c2a7d9f9f"),
            (12743307, "0xf4de986ff682ec877729ecb8b2ad61da0b882ebd"),
            (12743618, "0x6bdeeff9c09fb935736275609b138645c69419c5"),
            (12743716, "0xe5e26f36e8a2bfbfc859dd3b9c853a7652448d84"),
            (12743895, "0x5ebb017390ece8215a943ae52cc2c70e5a113d84"),
            (12743998, "0xeda6a1930a25330f9238d119f4ae1c1277618626"),
            (12744179, "0xd19a4df3221738b14b55d90803f423ede2c229e7"),
            (12744192, "0x203734bfa10de545619a2a86f94187900a5baf0c"),
            (12744356, "0x932aec20a46edff07e47b2ac77b99c2824ba4379"),
            (12744386, "0xc896c027d7855f922bbb81edb2628793332345bc"),
            (12744489, "0x8ed479711f1a6b306641a3df35141eb56fdaf012"),
            (12744515, "0x37e7b2f7d18768f01e209c397403f27acd84b80b"),
            (12744552, "0x8078daed5d6558a4d0037fa2a7fe0af88baedcd0"),
            (12744613, "0xb2ba53c261c82576009bb2242ca9d7330ae086bd"),
            (12744929, "0xf5f8007805c93f80a549efc39e71ebfe2486f366"),
            (12745040, "0xb7085be4a87f3e3c22885d44a40bc88884422171"),
            (12745135, "0x24dbedb4699eb996a8ceb2baef4a4ae057cf0294"),
            (12745259, "0x11dee4f84bb4978a97fb71c979816595c382ba25"),
            (12745340, "0x97779366c384825aff25127a221ce87081b350db"),
            (12745521, "0x1c59e1d03f81f213c7085ebc2eeeb06bf1741f49"),
            (12745682, "0x7a511ab054b429f73992bf6f285c5b84a7feadc5"),
            (12745756, "0x2223c884737c4758a017c4147b2b2fa26671ff0c"),
            (12745801, "0xfc49e21f449f955f4a5eb12264b0a46a7f4ed86e"),
            (12745883, "0x6cf01653df57391bf5a560c4b712bb3c79893bcb"),
            (12745986, "0x81216f193e8bed640a2378c38d689ebacc4b5d2c"),
            (12745989, "0xfead88881a192a22a803378356c946e377e9ffc1"),
            (12746045, "0x27132ef71d4badeafb220cf5818f9de0c88ceb37"),
            (12746198, "0x13cbef5c8bb4027bb8565a6b76adecc5dce3a2fa"),
            (12746266, "0xf575c3f86cbf9509df7f0e1bfade34c01388fce4"),
            (12746474, "0xb13697a7bf40e5ee55126a28045265a049330a12"),
            (12746519, "0x129fe8e05ae5df48fc07a557b6a4a0d42066a52c"),
            (12746847, "0xef814b39563b130b0a01133928ecc07cca9e6c8a"),
            (12746866, "0x5c9742e858b214d591d3e71ba8a07b0bd4eefe6a"),
            (12746887, "0x54b6f1b270270043c214fae675b9bc1be836ec1e"),
            (12747046, "0x4ba775fc1e08f156cc5247f5db001a1e6ec414c5"),
            (12747108, "0xfee78b7a20c3b0262d446613828d319bfbe1c595"),
            (12747133, "0xcfecc1c9f3cb6190cb1ff7f65a130bfbe5107d38"),
            (12747274, "0xbde484db131bd2ae80e44a57f865c1dfebb7e31f"),
            (12747299, "0x77229e8e4cfff6144cad575432fe125a703c396f"),
            (12747457, "0xce254b38383ecf0ad78f22f3be75825362a37bbd"),
            (12747483, "0xc39d2a4129c4c188efafd369919443493f464cd7"),
            (12747522, "0xd1395f3f4ace9994d79cc8693cc34f9fd7a6df54"),
            (12747551, "0x96449d66ab904ff6cc2c880f732f6034897f161b"),
            (12747689, "0x136e8408f7a6ac120f8ba7e1d0b64a7f71791a3b"),
            (12747742, "0xfa719429ba1497acf6659e398efdab58be8970c7"),
            (12747743, "0xa5555ee6d6a7e6b243a493ce1047e3365ccb9a76"),
            (12747802, "0xe4f8cd6f2de0ab71b5acc51e2e666de3beb349e8"),
            (12747917, "0x40d5c143419b4cd48f8346af6444d2e749b9bf97"),
            (12748003, "0x5b3b9a68e2ef9e611626dd20da2e0d07f42dbc4f"),
            (12748112, "0x34a64054c0bec069c4de5b890d1c001eff06fa50"),
            (12748669, "0x72511205f9143905cc70b7384445bfd41b0505e5"),
            (12749104, "0xea9f2e2ffa6304fc66411714eb40b1fad02af300"),
            (12749205, "0x5d1461990f636d35030cb603559d6a9d4487f9ca"),
            (12749229, "0xbf124abd5dd70f20114adc3fe5c644f7fa027d4d"),
            (12749513, "0xf85078b533364ec2da6520160b43b7ae07b75cdc"),
            (12749597, "0x5d95bb12ad95c3e8778de8dab3dfb3ace8ba69a9"),
            (12749673, "0x7ee1f2626f9321c66031761b8ee1cf786ab18621"),
            (12749868, "0x7016913d526faf457a4e55a6c891e2f751418d27"),
            (12749869, "0xba9fb8648fdfdbeb6b27528fad94d159920b2ce9"),
            (12749928, "0x1a515a19413789dcc06bf2d517e2a7716994bad0"),
            (12750571, "0x635bf82ec91427083b507415b5bfd1a069f84a36"),
            (12750801, "0xf9d19b06eea9e315affeb7244c5fd0c0ba3bd82c"),
            (12750901, "0xf459ae3199362764cec11f1a1dfb07ad1c93a58b"),
            (12750983, "0x2bfff6686f9036864275aad298eb9b505aed9a2c"),
            (12750994, "0x35166e5c48350344c2ebf31bad9c65a02eb1bb9b"),
            (12751069, "0x9a50b03ed346d6bf8a24f334c52e621418c7e69b"),
            (12751134, "0xab38116d3218b3e329e6ea562faae7b5004c34c1"),
            (12751775, "0x0548c63ee097b768b801e5a8fe83fb2b335dad2f"),
            (12752022, "0xf1b63cd9d80f922514c04b0fd0a30373316dd75b"),
            (12752178, "0xf714521cd45ba8e5a7d30c78aba91b882afd3e6d"),
            (12752328, "0x79fb66b2ad14a5cd395b225da7e250c11a979888"),
            (12752373, "0xf06b19397472f4458cd358f9faa9bab2ce02aa34"),
            (12752383, "0x7d85f1549407e11a0695def3ee20843abd653dc0"),
            (12752567, "0x05a2d831487ce05a6b8ac9d01c50e0f7d8609cfa"),
            (12752613, "0x56a9ce3b6a96b5ae27b66ee3b5ed2cd62ed8e358"),
            (12752630, "0x6f68a11c63d8139dfa8b9151908d77913bf9905e"),
            (12752926, "0x5a6c6cb3cac90487c9e487796087185959c44bd9"),
            (12752998, "0x77dfe5de823cb2cab3f43be5f24e9512e04afa73"),
            (12753091, "0x763dd0f2f333324d1b142bf234c0dca11ec50392"),
            (12753200, "0x3c8ad861df04a68e8a2559251e2009611523446d"),
            (12753299, "0xe0b9def812e9bf420e35dd4b5a526dc9dbd34989"),
            (12753491, "0x78927e89d947335c7b80c8b9877b6e4c145305a2"),
            (12753770, "0xede49232225bc8e7cf988ea79c19b0ffbca170ac"),
            (12753784, "0x6a1f2355f23089dfc5cef0edf5bb6e50bcd2183b"),
            (12753922, "0x923180541574378f48af976f01ecdfd0e8a1f909"),
            (12754068, "0x4ba950bed410a12c1294df28ed672f50c24297de"),
            (12754097, "0x856ad2964b114ee60285b9b4869896f7c4770043"),
            (12754141, "0x1a0ec29c18b0a3739c9204b557c01822baeb8990"),
            (12754454, "0x7029a842413810f856693ddc553ac2dc573fae5f"),
            (12754491, "0x5eb837c4b76239dcbe770be1de9a198b98078faf"),
            (12754833, "0x021d4d1c8a6ce7837d7cc93d47eedea379d9d5d4"),
            (12754862, "0x2837809fd68e4a4104af76bbec5b622b6146b2cb"),
            (12754889, "0xdd0c329e10714f4ecf489e5833bd7356e6526205"),
            (12754908, "0x0730abfb26cf04f7a31b20d42e88cd606a8441ea"),
            (12754919, "0x60a6b23a7a87c5ce9e3f81c869691f784b18a704"),
            (12754925, "0x7b54d97d6cf7691a6e71b5b90604128d0fb3157f"),
            (12754941, "0x1dfd47d8609e98b930f82e9e734d3fc6067bc643"),
            (12754989, "0x792618e17eec5e5cbf41582a0eea17afc21fa592"),
            (12755028, "0x0f992c7f073c553d673c7536218d196998e374c7"),
            (12755050, "0x8842d44c207b84739a69513410f3b6bd26dd499c"),
            (12755076, "0x31b047f6ad452cead4b2f95e6b08ccae5b116f19"),
            (12755356, "0x4094306ef222c3a4c50d5497e0a959a29d3dacc4"),
            (12755463, "0x9fbdfae84d9b42e9db10835124dc5667f215b05c"),
            (12755570, "0xd213a99d26480265178745558a7177e6e3404587"),
            (12756015, "0x801cbce6b0b8518c42b365e3135adfbe40085c0f"),
            (12756416, "0xea342a828376b1c4f3f06ea16172d3fad6578090"),
            (12756465, "0x368dd23f1a943f37bfca6a7ea6efe0df08f5705c"),
            (12756531, "0x4ba5df7beeef01ff9b3e84486c6bca2ead59dc1e"),
            (12756641, "0x1c2c708ae62a3d4e5d0df9527cdd01983cd73650"),
            (12756758, "0x0b09b67086d6888c59da451f50cf3e47976e29c3"),
            (12756970, "0x9af91e566e1817a0b3c9983bf68c0ddf0bd6943f"),
            (12757519, "0x40e37e6541a8adc8da1cf8871a7af673c1b51e3b"),
            (12757521, "0x66fc46c48522138b569516911f2efdc018b5f4dd"),
            (12757544, "0x526a2b1606aa0da169a9b0f48b42446a567c45c9"),
            (12757756, "0xfda24bcbdbff2738d2c8a78539a1d37b0c2a4e7d"),
            (12757982, "0xa850478adaace4c08fc61de44d8cf3b64f359bec"),
            (12758218, "0x5a24d06593d33d1967db224f010d51813dcbb868"),
            (12758333, "0x9f7e947239de8db3cd500e1180797b22c0192531"),
            (12758354, "0x712a2e0171a0791ca354d8f831643bbc684b45dc"),
            (12758361, "0x2df3f8e466d4924d86c35213dcef8c6955c6813a"),
            (12758451, "0x52f01c2c685c7bc243e266d9eb0f9bfb184e0df1"),
            (12758499, "0xc31f5f1809ed7b47e5cdd8403ce313d45d6da0a0"),
            (12758539, "0x5f7f44c304d016fe8cad589aaadba366528f0ad0"),
            (12758699, "0xad51a1169ebd62c1cb42d48804d31d689ce848cd"),
            (12758856, "0xc9535bef6b03891bb93be47058f91fb7a9b3279e"),
            (12759050, "0xdc212b0d75d430a5ad21e68ca4aaf7508bbf11f4"),
            (12759066, "0xbf9a7a27d0e7a7df4d0e7d57403935af82ac25d3"),
            (12759450, "0xe6e3d1cb69e4aea769a258be9a26fac24c01b59c"),
            (12759568, "0x87bcf796e9d13393b0cc8743bd58607131e2f7ae"),
            (12759783, "0x983dfad44d08a29ff4ff8c825d07b1a8bd3c8d00"),
            (12760304, "0x3813d7807b6076e45dfca3cfe70bb2cce3f441d7"),
            (12760463, "0x8ddb1367d6cd919dbaff05c8acf54b8e16df0003"),
            (12760629, "0x352ff70e039e781e8a6e815ad9abe424c3534d0b"),
            (12760722, "0x59434f700efdef581aac9faeb43d220c53121d08"),
            (12760860, "0x38866ae85e4ea03a05bffc4b59ac4fe960d0a41b"),
            (12760969, "0x936198fcac6d8cdec3815f24ba250041f593f6c3"),
            (12761045, "0xb9b0809ecf0441948355e72c5e33ed02023dad1b"),
            (12761456, "0x2a9db598abb59c165773e837acffc6c6e48e1162"),
            (12761770, "0x8dbeaff4cd60953ded38243284294bca42539515"),
            (12761770, "0xf659b79065158d1379e3a4d13a1370b086e1bcf0"),
            (12761828, "0x4c92a3988b94f2fefe3147ac9d538b081d9b815f"),
            (12761913, "0xab2e2c954cf4ffe04424f459a122e958dac41002"),
            (12762128, "0x9d28e23687d3ee5840382a8c7d2352e7142bc3ca"),
            (12762229, "0x54a98708169b9c0142a442a6c30b52a7798b7f98"),
            (12762239, "0x23273b9cbcdca2210135a1b00f93b48c045fa757"),
            (12762472, "0x30fd64c9271bcc6607ad2efaea9bf8ea3b85eeb7"),
            (12762731, "0x370eeba8bdaa9111d955fde517c45fac96310b39"),
            (12762828, "0xaddc4474edecc808d7215b359c756f69ae2cd6d0"),
            (12762991, "0x059b50bc9ea067394dcaf5f3de5a7227a8ae8907"),
            (12763178, "0xd6dcc1861c4e876a68def2d012cf58278c83c983"),
            (12763326, "0x5be1b63a3873cbe553f8a1807e83609ef0497797"),
            (12764300, "0x5719274c0951a90d11f8c34be8b8df82eec3e53a"),
            (12764760, "0xaf431efd64ecffa25086fa91c7c4c563480735b0"),
            (12764899, "0x5b6488b48c8d4bbc55b313ba50096fce4fff6f42"),
            (12764975, "0x59d3ba0f193de61f42aaf23266a45f4dd483d4f5"),
            (12765096, "0x548c1631c8becabd22debe715cca52a8c806e71b"),
            (12765254, "0x9b5970ffcbcb4b595e55b29cd73d3946b3b41c21"),
            (12765373, "0xe6a3642f0bf3a6e759384147abe577340a2f99e5"),
            (12765595, "0x5df61025738892076897704e05e790bce7a1c5f3"),
            (12765875, "0x1e3ee99f8e3586731a358e9fbe0daf8bb7eda3e8"),
            (12765997, "0x316fe9ce2e0ab45c2146967c4349d02b3563b057"),
            (12766039, "0xd3b71070ff55404ab91ef73d74691ac2bec23924"),
            (12766214, "0x871cf19148b259767a718272a03e6832d967254b"),
            (12766322, "0xa15acc956fb998102aad247592a86c66dfa52c5b"),
            (12766414, "0x074a35d73a5008ad2786b15c11279438e05a1db6"),
            (12766652, "0x005843e075e77ba46a26d24914db10a4d9ca0122"),
            (12766869, "0x1d4e935e5b15b5753d0bb38f1a7d5bc088924ee2"),
            (12767013, "0x006ac24a1f49e472673c82327bdf177a5c11491b"),
            (12767088, "0x6692500749df8b1e39e74fd897bd1113458b991b"),
            (12767438, "0x81a5274b944019ddd4689239d455400aaf543536"),
            (12767619, "0x344385a3b6fa6bd4bc7395c17eec8748901ec843"),
            (12767816, "0xd48e7bd0f02ce4ddfd82f8a97ad194f762e835cc"),
            (12768229, "0x1475a3f259052dc57b3e92bfb6e478f4769da9e6"),
            (12768242, "0x0c5a74fe23e982fe5af7c724f13e2799d19d8e61"),
            (12768366, "0xe7b32b39883166fb0929a12a169922414f08d002"),
            (12768421, "0xa388f891e5b7e8bb9b1ac1a09b410ca872e30450"),
            (12768988, "0x590c56d1f8e0d8d22cffea68c632c2074aa34807"),
            (12769189, "0x1cff53fc9a1a7d9df6b94b2721d7d786d448e60a"),
            (12769448, "0x8e183d36919b6a37873486b45698e04bf3fd9f8e"),
            (12769539, "0x2809923b4ef660fa81eb1e8a6e043d2557007b32"),
            (12769815, "0xb8c07a8848157213864c22ec25211aedd96de59f"),
            (12769929, "0xd3822e0cfc8c785d6e185532a4961aecc1ecdb5e"),
            (12769932, "0xe9875f763c647fd3abcc5713e55e3d0de27a4cfe"),
            (12769996, "0xf7e42b889827658e4d9d03ff82b40679cd4512e1"),
            (12770441, "0xe785fa5efd38b147057d6b0e070a30d23e173c74"),
            (12770486, "0x3394a6a9a69283cabfb8b309d4c09cd6359b9cc5"),
            (12770923, "0x45cd6762eb2441e32355f2efe923649cb4c92ab8"),
            (12771145, "0x3fab94456ef109a905ea6a5bd34ef7811ec51da3"),
            (12771222, "0x79599e9097866e3b274c00175fd92760af5e65fa"),
            // (12370687, "0x1c74dde716d3f2a1df5097b7c2543c5d97cfa4d3"),
            // (12371099, "0xe8c6c9227491c0a8156a0106a0204d881bb7e531"),
            // (12371145, "0x8581788cef3b7ee7313fea15fe279dc2f6c43899"),
            // (12371220, "0x14de8287adc90f0f95bf567c0707670de52e3813"),
            // (12439227, "0xa788ad489a6825100e6484fcc3b7f9a8d6c9b3f9"),
            // (12504792, "0xdcc531a543799e06138af64b20d919f1bba4e805"),
            // (12744356, "0x932aec20a46edff07e47b2ac77b99c2824ba4379"),
            // (12909093, "0xbce5598171f34e0e936df35157ca033ff8dba98a"),
            // (13173797, "0x546a5c1739c005afec87442f5b69198ba0978dd1"),
            // (13424641, "0x81489b0e7c7a515799c89374e23ac9295088551d"),
            // (13543862, "0xe15e6583425700993bd08f51bf6e7b73cd5da91b"),
            // (13639719, "0x39aa14c3adbc173d17ff746d49ca81c9f575b13e"),
            // (13813433, "0xb4ecce46b8d4e4abfd03c9b806276a6735c9c092"),
            // (13819193, "0x9758f2bf8e6f6ee89bd71fb434d92c699dc89e30"),
            // (13856597, "0x98e8bb5321adf5298ccb7674f102dd432ded1feb"),
            // (13856851, "0xcde473286561d9b876bead3ac7cc38040f738d3f"),
            // (13869973, "0x4006bed7bf103d70a1c6b7f1cef4ad059193dc25"),
        ];
        for data_source in data_sources {
            self.log.extend(EthereumLogFilter::from_mapping(
                &data_source.mapping,
                ADDRESSES
                    .iter()
                    .map(|pair| H160::from_str(pair.1).unwrap())
                    .collect::<Vec<H160>>()
                    .as_ref(),
            ));

            self.call
                .extend(EthereumCallFilter::from_mapping(&data_source.mapping));

            self.block
                .extend(EthereumBlockFilter::from_mapping(&data_source.mapping));
        }
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        let EthereumBlockFilter {
            polling_intervals,
            contract_addresses: _contract_addresses,
            trigger_every_block,
        } = self.block.clone();

        // If polling_intervals is empty this will return true, else it will be true only if all intervals are 0
        // ie: All triggers are initialization handlers. We do not need firehose to send all block headers for
        // initialization handlers
        let has_initilization_triggers_only = polling_intervals.iter().all(|(_, i)| *i == 0);

        let log_filters: Vec<LogFilter> = self.log.into();
        let mut call_filters: Vec<CallToFilter> = self.call.into();
        call_filters.extend(Into::<Vec<CallToFilter>>::into(self.block));

        if call_filters.is_empty() && log_filters.is_empty() && !trigger_every_block {
            return Vec::new();
        }

        let combined_filter = CombinedFilter {
            log_filters,
            call_filters,
            // We need firehose to send all block headers when `trigger_every_block` is true and when
            // We have polling triggers which are not from initiallization handlers
            send_all_block_headers: trigger_every_block || !has_initilization_triggers_only,
        };

        vec![Any {
            type_url: COMBINED_FILTER_TYPE_URL.into(),
            value: combined_filter.encode_to_vec(),
        }]
    }
}

#[derive(Clone, Debug, Default)]
pub struct EthereumLogFilter {
    /// Log filters can be represented as a bipartite graph between contracts and events. An edge
    /// exists between a contract and an event if a data source for the contract has a trigger for
    /// the event.
    /// Edges are of `bool` type and indicates when a trigger requires a transaction receipt.
    contracts_and_events_graph: GraphMap<LogFilterNode, bool, petgraph::Undirected>,

    addresses: HashSet<Address>,

    /// Event sigs with no associated address, matching on all addresses.
    /// Maps to a boolean representing if a trigger requires a transaction receipt.
    wildcard_events: HashMap<EventSignature, bool>,
    /// Events with any of the topic filters set
    /// Maps to a boolean representing if a trigger requires a transaction receipt.
    events_with_topic_filters: HashMap<EventSignatureWithTopics, bool>,
}

impl From<EthereumLogFilter> for Vec<LogFilter> {
    fn from(val: EthereumLogFilter) -> Self {
        let mut template_filters: Vec<LogFilter> = vec![];
        if !val.addresses.is_empty() && !val.wildcard_events.is_empty() {
            let filter = LogFilter {
                addresses: val
                    .addresses
                    .iter()
                    .map(|addr| addr.to_fixed_bytes().to_vec())
                    .collect_vec(),
                event_signatures: val
                    .wildcard_events
                    .keys()
                    .map(|sig| sig.to_fixed_bytes().to_vec())
                    .collect_vec(),
            };

            template_filters.push(filter);
        }

        val.eth_get_logs_filters()
            .map(
                |EthGetLogsFilter {
                     contracts,
                     event_signatures,
                     .. // TODO: Handle events with topic filters for firehose
                 }| LogFilter {
                    addresses: contracts
                        .iter()
                        .map(|addr| addr.to_fixed_bytes().to_vec())
                        .collect_vec(),
                    event_signatures: event_signatures
                        .iter()
                        .map(|sig| sig.to_fixed_bytes().to_vec())
                        .collect_vec(),
                },
            )
            .chain(template_filters)
            .collect_vec()
    }
}

impl EthereumLogFilter {
    /// Check if this filter matches the specified `Log`.
    pub fn matches(&self, log: &Log) -> bool {
        // First topic should be event sig
        match log.topics.first() {
            None => false,

            Some(sig) => {
                // The `Log` matches the filter either if the filter contains
                // a (contract address, event signature) pair that matches the
                // `Log`, or if the filter contains wildcard event that matches.
                let contract = LogFilterNode::Contract(log.address);
                let event = LogFilterNode::Event(*sig);
                self.contracts_and_events_graph
                    .all_edges()
                    .any(|(s, t, _)| (s == contract && t == event) || (t == contract && s == event))
                    || self.wildcard_events.contains_key(sig)
                    || self
                        .events_with_topic_filters
                        .iter()
                        .any(|(e, _)| e.matches(Some(&log.address), *sig, &log.topics))
            }
        }
    }

    /// Similar to [`matches`], checks if a transaction receipt is required for this log filter.
    pub fn requires_transaction_receipt(
        &self,
        event_signature: &H256,
        contract_address: Option<&Address>,
        topics: &Vec<H256>,
    ) -> bool {
        // Check for wildcard events first.
        if self.wildcard_events.get(event_signature) == Some(&true) {
            return true;
        }

        // Next, check events with topic filters.
        if self
            .events_with_topic_filters
            .iter()
            .any(|(event_with_topics, &requires_receipt)| {
                requires_receipt
                    && event_with_topics.matches(contract_address, *event_signature, topics)
            })
        {
            return true;
        }

        // Finally, check the contracts_and_events_graph if a contract address is specified.
        if let Some(address) = contract_address {
            let contract_node = LogFilterNode::Contract(*address);
            let event_node = LogFilterNode::Event(*event_signature);

            // Directly iterate over all edges and return true if a matching edge that requires a receipt is found.
            for (s, t, &r) in self.contracts_and_events_graph.all_edges() {
                if r && ((s == contract_node && t == event_node)
                    || (t == contract_node && s == event_node))
                {
                    return true;
                }
            }
        }

        // If none of the conditions above match, return false.
        false
    }

    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        let mut this = EthereumLogFilter::default();
        for ds in iter {
            for event_handler in ds.mapping.event_handlers.iter() {
                let event_sig = event_handler.topic0();
                match ds.address {
                    Some(contract) if !event_handler.has_additional_topics() => {
                        this.contracts_and_events_graph.add_edge(
                            LogFilterNode::Contract(contract),
                            LogFilterNode::Event(event_sig),
                            event_handler.receipt,
                        );
                    }
                    Some(contract) => {
                        this.events_with_topic_filters.insert(
                            EventSignatureWithTopics::new(
                                Some(contract),
                                event_sig,
                                event_handler.topic1.clone(),
                                event_handler.topic2.clone(),
                                event_handler.topic3.clone(),
                            ),
                            event_handler.receipt,
                        );
                    }

                    None if (!event_handler.has_additional_topics()) => {
                        this.wildcard_events
                            .insert(event_sig, event_handler.receipt);
                    }

                    None => {
                        this.events_with_topic_filters.insert(
                            EventSignatureWithTopics::new(
                                ds.address,
                                event_sig,
                                event_handler.topic1.clone(),
                                event_handler.topic2.clone(),
                                event_handler.topic3.clone(),
                            ),
                            event_handler.receipt,
                        );
                    }
                }
            }
        }
        this
    }

    pub fn from_mapping(mapping: &Mapping, sources: &[H160]) -> Self {
        let mut this = EthereumLogFilter::default();
        if !sources.is_empty() {
            this.addresses.extend(sources.iter());
        }
        for event_handler in &mapping.event_handlers {
            let signature = event_handler.topic0();

            this.wildcard_events
                .insert(signature, event_handler.receipt);
        }
        this
    }

    /// Extends this log filter with another one.
    pub fn extend(&mut self, other: EthereumLogFilter) {
        if other.is_empty() {
            return;
        };

        // Destructure to make sure we're checking all fields.
        let EthereumLogFilter {
            contracts_and_events_graph,
            wildcard_events,
            events_with_topic_filters,
            addresses,
        } = other;
        for (s, t, e) in contracts_and_events_graph.all_edges() {
            self.contracts_and_events_graph.add_edge(s, t, *e);
        }
        self.wildcard_events.extend(wildcard_events);
        self.events_with_topic_filters
            .extend(events_with_topic_filters);
        self.addresses.extend(addresses.clone().iter());
    }

    /// An empty filter is one that never matches.
    pub fn is_empty(&self) -> bool {
        // Destructure to make sure we're checking all fields.
        let EthereumLogFilter {
            contracts_and_events_graph,
            wildcard_events,
            events_with_topic_filters,
            addresses,
        } = self;
        contracts_and_events_graph.edge_count() == 0
            && wildcard_events.is_empty()
            && events_with_topic_filters.is_empty()
            && addresses.is_empty()
    }

    /// Filters for `eth_getLogs` calls. The filters will not return false positives. This attempts
    /// to balance between having granular filters but too many calls and having few calls but too
    /// broad filters causing the Ethereum endpoint to timeout.
    pub fn eth_get_logs_filters(self) -> impl Iterator<Item = EthGetLogsFilter> {
        let mut filters = Vec::new();

        if self.addresses.is_empty() {
            // Start with the wildcard event filters.
            filters.extend(
                self.wildcard_events
                    .into_keys()
                    .map(EthGetLogsFilter::from_event),
            );

            // Handle events with topic filters.
            filters.extend(self.events_with_topic_filters.into_iter().map(
                |(event_with_topics, _)| {
                    EthGetLogsFilter::from_event_with_topics(event_with_topics)
                },
            ));
        }

        // The current algorithm is to repeatedly find the maximum cardinality vertex and turn all
        // of its edges into a filter. This is nice because it is neutral between filtering by
        // contract or by events, if there are many events that appear on only one data source
        // we'll filter by many events on a single contract, but if there is an event that appears
        // on a lot of data sources we'll filter by many contracts with a single event.
        //
        // From a theoretical standpoint we're finding a vertex cover, and this is not the optimal
        // algorithm to find a minimum vertex cover, but should be fine as an approximation.
        //
        // One optimization we're not doing is to merge nodes that have the same neighbors into a
        // single node. For example if a subgraph has two data sources, each with the same two
        // events, we could cover that with a single filter and no false positives. However that
        // might cause the filter to become too broad, so at the moment it seems excessive.
        let mut g = self.contracts_and_events_graph;
        while g.edge_count() > 0 {
            let mut push_filter = |filter: EthGetLogsFilter| {
                // Sanity checks:
                // - The filter is not a wildcard because all nodes have neighbors.
                // - The graph is bipartite.
                assert!(!filter.contracts.is_empty() && !filter.event_signatures.is_empty());
                assert!(filter.contracts.len() == 1 || filter.event_signatures.len() == 1);
                filters.push(filter);
            };

            // If there are edges, there are vertexes.
            let max_vertex = g.nodes().max_by_key(|&n| g.neighbors(n).count()).unwrap();
            let mut filter = match max_vertex {
                LogFilterNode::Contract(address) => EthGetLogsFilter::from_contract(address),
                LogFilterNode::Event(event_sig) => EthGetLogsFilter::from_event(event_sig),
            };
            for neighbor in g.neighbors(max_vertex) {
                match neighbor {
                    LogFilterNode::Contract(address) => {
                        if filter.contracts.len() == ENV_VARS.get_logs_max_contracts {
                            // The batch size was reached, register the filter and start a new one.
                            let event = filter.event_signatures[0];
                            push_filter(filter);
                            filter = EthGetLogsFilter::from_event(event);
                        }
                        filter.contracts.push(address);
                    }
                    LogFilterNode::Event(event_sig) => filter.event_signatures.push(event_sig),
                }
            }

            push_filter(filter);
            g.remove_node(max_vertex);
        }
        filters.into_iter()
    }

    #[cfg(debug_assertions)]
    pub fn contract_addresses(&self) -> impl Iterator<Item = Address> + '_ {
        self.contracts_and_events_graph
            .nodes()
            .filter_map(|node| match node {
                LogFilterNode::Contract(address) => Some(address),
                LogFilterNode::Event(_) => None,
            })
    }
}

#[derive(Clone, Debug, Default)]
pub struct EthereumCallFilter {
    // Each call filter has a map of filters keyed by address, each containing a tuple with
    // start_block and the set of function signatures
    pub contract_addresses_function_signatures:
        HashMap<Address, (BlockNumber, HashSet<FunctionSelector>)>,

    pub wildcard_signatures: HashSet<FunctionSelector>,
}

impl Into<Vec<CallToFilter>> for EthereumCallFilter {
    fn into(self) -> Vec<CallToFilter> {
        if self.is_empty() {
            return Vec::new();
        }

        let EthereumCallFilter {
            contract_addresses_function_signatures,
            wildcard_signatures,
        } = self;

        let mut filters: Vec<CallToFilter> = contract_addresses_function_signatures
            .into_iter()
            .map(|(addr, (_, sigs))| CallToFilter {
                addresses: vec![addr.to_fixed_bytes().to_vec()],
                signatures: sigs.into_iter().map(|x| x.to_vec()).collect_vec(),
            })
            .collect();

        if !wildcard_signatures.is_empty() {
            filters.push(CallToFilter {
                addresses: vec![],
                signatures: wildcard_signatures
                    .into_iter()
                    .map(|x| x.to_vec())
                    .collect_vec(),
            });
        }

        filters
    }
}

impl EthereumCallFilter {
    pub fn matches(&self, call: &EthereumCall) -> bool {
        // Calls returned by Firehose actually contains pure transfers and smart
        // contract calls. If the input is less than 4 bytes, we assume it's a pure transfer
        // and discards those.
        if call.input.0.len() < 4 {
            return false;
        }

        // The `call.input.len()` is validated in the
        // DataSource::match_and_decode function.
        // Those calls are logged as warning and skipped.
        //
        // See 280b0108-a96e-4738-bb37-60ce11eeb5bf
        let call_signature = &call.input.0[..4];

        // Ensure the call is to a contract the filter expressed an interest in
        match self.contract_addresses_function_signatures.get(&call.to) {
            // If the call is to a contract with no specified functions, keep the call
            //
            // Allows the ability to genericly match on all calls to a contract.
            // Caveat is this catch all clause limits you from matching with a specific call
            // on the same address
            Some(v) if v.1.is_empty() => true,
            // There are some relevant signatures to test
            // this avoids having to call extend for every match call, checks the contract specific funtions, then falls
            // back on wildcards
            Some(v) => {
                let sig = &v.1;
                sig.contains(call_signature) || self.wildcard_signatures.contains(call_signature)
            }
            // no contract specific functions, check wildcards
            None => self.wildcard_signatures.contains(call_signature),
        }
    }

    pub fn from_mapping(mapping: &Mapping) -> Self {
        let functions = mapping
            .call_handlers
            .iter()
            .map(move |call_handler| {
                let sig = keccak256(call_handler.function.as_bytes());
                [sig[0], sig[1], sig[2], sig[3]]
            })
            .collect();

        Self {
            wildcard_signatures: functions,
            contract_addresses_function_signatures: HashMap::new(),
        }
    }

    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter_map(|data_source| data_source.address.map(|addr| (addr, data_source)))
            .flat_map(|(contract_addr, data_source)| {
                let start_block = data_source.start_block;
                data_source
                    .mapping
                    .call_handlers
                    .iter()
                    .map(move |call_handler| {
                        let sig = keccak256(call_handler.function.as_bytes());
                        (start_block, contract_addr, [sig[0], sig[1], sig[2], sig[3]])
                    })
            })
            .collect()
    }

    /// Extends this call filter with another one.
    pub fn extend(&mut self, other: EthereumCallFilter) {
        if other.is_empty() {
            return;
        };

        let EthereumCallFilter {
            contract_addresses_function_signatures,
            wildcard_signatures,
        } = other;

        // Extend existing address / function signature key pairs
        // Add new address / function signature key pairs from the provided EthereumCallFilter
        for (address, (proposed_start_block, new_sigs)) in
            contract_addresses_function_signatures.into_iter()
        {
            match self
                .contract_addresses_function_signatures
                .get_mut(&address)
            {
                Some((existing_start_block, existing_sigs)) => {
                    *existing_start_block = cmp::min(proposed_start_block, *existing_start_block);
                    existing_sigs.extend(new_sigs);
                }
                None => {
                    self.contract_addresses_function_signatures
                        .insert(address, (proposed_start_block, new_sigs));
                }
            }
        }

        self.wildcard_signatures.extend(wildcard_signatures);
    }

    /// An empty filter is one that never matches.
    pub fn is_empty(&self) -> bool {
        // Destructure to make sure we're checking all fields.
        let EthereumCallFilter {
            contract_addresses_function_signatures,
            wildcard_signatures: wildcard_matches,
        } = self;
        contract_addresses_function_signatures.is_empty() && wildcard_matches.is_empty()
    }
}

impl FromIterator<(BlockNumber, Address, FunctionSelector)> for EthereumCallFilter {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (BlockNumber, Address, FunctionSelector)>,
    {
        let mut lookup: HashMap<Address, (BlockNumber, HashSet<FunctionSelector>)> = HashMap::new();
        iter.into_iter()
            .for_each(|(start_block, address, function_signature)| {
                lookup
                    .entry(address)
                    .or_insert((start_block, HashSet::default()));
                lookup.get_mut(&address).map(|set| {
                    if set.0 > start_block {
                        set.0 = start_block
                    }
                    set.1.insert(function_signature);
                    set
                });
            });
        EthereumCallFilter {
            contract_addresses_function_signatures: lookup,
            wildcard_signatures: HashSet::new(),
        }
    }
}

impl From<&EthereumBlockFilter> for EthereumCallFilter {
    fn from(ethereum_block_filter: &EthereumBlockFilter) -> Self {
        Self {
            contract_addresses_function_signatures: ethereum_block_filter
                .contract_addresses
                .iter()
                .map(|(start_block_opt, address)| {
                    (*address, (*start_block_opt, HashSet::default()))
                })
                .collect::<HashMap<Address, (BlockNumber, HashSet<FunctionSelector>)>>(),
            wildcard_signatures: HashSet::new(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct EthereumBlockFilter {
    /// Used for polling block handlers, a hashset of (start_block, polling_interval)
    pub polling_intervals: HashSet<(BlockNumber, i32)>,
    pub contract_addresses: HashSet<(BlockNumber, Address)>,
    pub trigger_every_block: bool,
}

impl Into<Vec<CallToFilter>> for EthereumBlockFilter {
    fn into(self) -> Vec<CallToFilter> {
        self.contract_addresses
            .into_iter()
            .map(|(_, addr)| addr)
            .sorted()
            .dedup_by(|x, y| x == y)
            .map(|addr| CallToFilter {
                addresses: vec![addr.to_fixed_bytes().to_vec()],
                signatures: vec![],
            })
            .collect_vec()
    }
}

impl EthereumBlockFilter {
    /// from_mapping ignores contract addresses in this use case because templates can't provide Address or BlockNumber
    /// ahead of time. This means the filters applied to the block_stream need to be broad, in this case,
    /// specifically, will match all blocks. The blocks are then further filtered by the subgraph instance manager
    /// which keeps track of deployed contracts and relevant addresses.
    pub fn from_mapping(mapping: &Mapping) -> Self {
        Self {
            polling_intervals: HashSet::new(),
            contract_addresses: HashSet::new(),
            trigger_every_block: !mapping.block_handlers.is_empty(),
        }
    }

    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter(|data_source| data_source.address.is_some())
            .fold(Self::default(), |mut filter_opt, data_source| {
                let has_block_handler_with_call_filter = data_source
                    .mapping
                    .block_handlers
                    .clone()
                    .into_iter()
                    .any(|block_handler| match block_handler.filter {
                        Some(BlockHandlerFilter::Call) => true,
                        _ => false,
                    });

                let has_block_handler_without_filter = data_source
                    .mapping
                    .block_handlers
                    .clone()
                    .into_iter()
                    .any(|block_handler| block_handler.filter.is_none());

                filter_opt.extend(Self {
                    trigger_every_block: has_block_handler_without_filter,
                    polling_intervals: data_source
                        .mapping
                        .block_handlers
                        .clone()
                        .into_iter()
                        .filter_map(|block_handler| match block_handler.filter {
                            Some(BlockHandlerFilter::Polling { every }) => {
                                Some((data_source.start_block, every.get() as i32))
                            }
                            Some(BlockHandlerFilter::Once) => Some((data_source.start_block, 0)),
                            _ => None,
                        })
                        .collect(),
                    contract_addresses: if has_block_handler_with_call_filter {
                        vec![(data_source.start_block, data_source.address.unwrap())]
                            .into_iter()
                            .collect()
                    } else {
                        HashSet::default()
                    },
                });
                filter_opt
            })
    }

    pub fn extend(&mut self, other: EthereumBlockFilter) {
        if other.is_empty() {
            return;
        };

        let EthereumBlockFilter {
            polling_intervals,
            contract_addresses,
            trigger_every_block,
        } = other;

        self.trigger_every_block = self.trigger_every_block || trigger_every_block;

        for other in contract_addresses {
            let (other_start_block, other_address) = other;

            match self.find_contract_address(&other.1) {
                Some((current_start_block, current_address)) => {
                    if other_start_block < current_start_block {
                        self.contract_addresses
                            .remove(&(current_start_block, current_address));
                        self.contract_addresses
                            .insert((other_start_block, other_address));
                    }
                }
                None => {
                    self.contract_addresses
                        .insert((other_start_block, other_address));
                }
            }
        }

        for (other_start_block, other_polling_interval) in &polling_intervals {
            self.polling_intervals
                .insert((*other_start_block, *other_polling_interval));
        }
    }

    fn requires_traces(&self) -> bool {
        !self.contract_addresses.is_empty()
    }

    /// An empty filter is one that never matches.
    pub fn is_empty(&self) -> bool {
        let Self {
            contract_addresses,
            polling_intervals,
            trigger_every_block,
        } = self;
        // If we are triggering every block, we are of course not empty
        !*trigger_every_block && contract_addresses.is_empty() && polling_intervals.is_empty()
    }

    fn find_contract_address(&self, candidate: &Address) -> Option<(i32, Address)> {
        self.contract_addresses
            .iter()
            .find(|(_, current_address)| candidate == current_address)
            .cloned()
    }
}

pub enum ProviderStatus {
    Working,
    VersionFail,
    GenesisFail,
    VersionTimeout,
    GenesisTimeout,
}

impl From<ProviderStatus> for f64 {
    fn from(state: ProviderStatus) -> Self {
        match state {
            ProviderStatus::Working => 0.0,
            ProviderStatus::VersionFail => 1.0,
            ProviderStatus::GenesisFail => 2.0,
            ProviderStatus::VersionTimeout => 3.0,
            ProviderStatus::GenesisTimeout => 4.0,
        }
    }
}

const STATUS_HELP: &str = "0 = ok, 1 = net_version failed, 2 = get genesis failed, 3 = net_version timeout, 4 = get genesis timeout";
#[derive(Debug, Clone)]
pub struct ProviderEthRpcMetrics {
    request_duration: Box<HistogramVec>,
    errors: Box<CounterVec>,
    status: Box<GaugeVec>,
}

impl ProviderEthRpcMetrics {
    pub fn new(registry: Arc<MetricsRegistry>) -> Self {
        let request_duration = registry
            .new_histogram_vec(
                "eth_rpc_request_duration",
                "Measures eth rpc request duration",
                vec![String::from("method"), String::from("provider")],
                vec![0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6],
            )
            .unwrap();
        let errors = registry
            .new_counter_vec(
                "eth_rpc_errors",
                "Counts eth rpc request errors",
                vec![String::from("method"), String::from("provider")],
            )
            .unwrap();
        let status_help = format!("Whether the provider has failed ({STATUS_HELP})");
        let status = registry
            .new_gauge_vec(
                "eth_rpc_status",
                &status_help,
                vec![String::from("provider")],
            )
            .unwrap();
        Self {
            request_duration,
            errors,
            status,
        }
    }

    pub fn observe_request(&self, duration: f64, method: &str, provider: &str) {
        self.request_duration
            .with_label_values(&[method, provider])
            .observe(duration);
    }

    pub fn add_error(&self, method: &str, provider: &str) {
        self.errors.with_label_values(&[method, provider]).inc();
    }

    pub fn set_status(&self, status: ProviderStatus, provider: &str) {
        self.status
            .with_label_values(&[provider])
            .set(status.into());
    }
}

#[derive(Clone)]
pub struct SubgraphEthRpcMetrics {
    request_duration: GaugeVec,
    errors: CounterVec,
    deployment: String,
}

impl SubgraphEthRpcMetrics {
    pub fn new(registry: Arc<MetricsRegistry>, subgraph_hash: &str) -> Self {
        let request_duration = registry
            .global_gauge_vec(
                "deployment_eth_rpc_request_duration",
                "Measures eth rpc request duration for a subgraph deployment",
                vec!["deployment", "method", "provider"].as_slice(),
            )
            .unwrap();
        let errors = registry
            .global_counter_vec(
                "deployment_eth_rpc_errors",
                "Counts eth rpc request errors for a subgraph deployment",
                vec!["deployment", "method", "provider"].as_slice(),
            )
            .unwrap();
        Self {
            request_duration,
            errors,
            deployment: subgraph_hash.into(),
        }
    }

    pub fn observe_request(&self, duration: f64, method: &str, provider: &str) {
        self.request_duration
            .with_label_values(&[&self.deployment, method, provider])
            .set(duration);
    }

    pub fn add_error(&self, method: &str, provider: &str) {
        self.errors
            .with_label_values(&[&self.deployment, method, provider])
            .inc();
    }
}

/// Common trait for components that watch and manage access to Ethereum.
///
/// Implementations may be implemented against an in-process Ethereum node
/// or a remote node over RPC.
#[async_trait]
pub trait EthereumAdapter: Send + Sync + 'static {
    /// The `provider.label` from the adapter's configuration
    fn provider(&self) -> &str;

    /// Ask the Ethereum node for some identifying information about the Ethereum network it is
    /// connected to.
    async fn net_identifiers(&self) -> Result<ChainIdentifier, Error>;

    /// Get the latest block, including full transactions.
    fn latest_block(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = bc::IngestorError> + Send + Unpin>;

    /// Get the latest block, with only the header and transaction hashes.
    fn latest_block_header(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = web3::types::Block<H256>, Error = bc::IngestorError> + Send>;

    fn load_block(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send>;

    /// Load Ethereum blocks in bulk, returning results as they come back as a Stream.
    /// May use the `chain_store` as a cache.
    async fn load_blocks(
        &self,
        logger: Logger,
        chain_store: Arc<dyn ChainStore>,
        block_hashes: HashSet<H256>,
    ) -> Box<dyn Stream<Item = Arc<LightEthereumBlock>, Error = Error> + Send>;

    /// Find a block by its hash.
    fn block_by_hash(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<dyn Future<Item = Option<LightEthereumBlock>, Error = Error> + Send>;

    fn block_by_number(
        &self,
        logger: &Logger,
        block_number: BlockNumber,
    ) -> Box<dyn Future<Item = Option<LightEthereumBlock>, Error = Error> + Send>;

    /// Load full information for the specified `block` (in particular, transaction receipts).
    fn load_full_block(
        &self,
        logger: &Logger,
        block: LightEthereumBlock,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<EthereumBlock, bc::IngestorError>> + Send + '_>,
    >;

    /// Find a block by its number, according to the Ethereum node.
    ///
    /// Careful: don't use this function without considering race conditions.
    /// Chain reorgs could happen at any time, and could affect the answer received.
    /// Generally, it is only safe to use this function with blocks that have received enough
    /// confirmations to guarantee no further reorgs, **and** where the Ethereum node is aware of
    /// those confirmations.
    /// If the Ethereum node is far behind in processing blocks, even old blocks can be subject to
    /// reorgs.
    fn block_hash_by_block_number(
        &self,
        logger: &Logger,
        block_number: BlockNumber,
    ) -> Box<dyn Future<Item = Option<H256>, Error = Error> + Send>;

    /// Finds the hash and number of the lowest non-null block with height greater than or equal to
    /// the given number.
    ///
    /// Note that the same caveats on reorgs apply as for `block_hash_by_block_number`, and must
    /// also be considered for the resolved block, in case it is higher than the requested number.
    async fn next_existing_ptr_to_number(
        &self,
        logger: &Logger,
        block_number: BlockNumber,
    ) -> Result<BlockPtr, Error>;

    /// Call the function of a smart contract. A return of `None` indicates
    /// that the call reverted. The returned `CallSource` indicates where
    /// the result came from for accounting purposes
    async fn contract_call(
        &self,
        logger: &Logger,
        call: &ContractCall,
        cache: Arc<dyn EthereumCallCache>,
    ) -> Result<(Option<Vec<Token>>, call::Source), ContractCallError>;

    /// Make multiple contract calls in a single batch. The returned `Vec`
    /// has results in the same order as the calls in `calls` on input. The
    /// calls must all be for the same block
    async fn contract_calls(
        &self,
        logger: &Logger,
        calls: &[&ContractCall],
        cache: Arc<dyn EthereumCallCache>,
    ) -> Result<Vec<(Option<Vec<Token>>, call::Source)>, ContractCallError>;

    fn get_balance(
        &self,
        logger: &Logger,
        address: H160,
        block_ptr: BlockPtr,
    ) -> Box<dyn Future<Item = U256, Error = EthereumRpcError> + Send>;

    // Returns the compiled bytecode of a smart contract
    fn get_code(
        &self,
        logger: &Logger,
        address: H160,
        block_ptr: BlockPtr,
    ) -> Box<dyn Future<Item = Bytes, Error = EthereumRpcError> + Send>;
}

#[cfg(test)]
mod tests {
    use crate::adapter::{FunctionSelector, COMBINED_FILTER_TYPE_URL};

    use super::{EthereumBlockFilter, LogFilterNode};
    use super::{EthereumCallFilter, EthereumLogFilter, TriggerFilter};

    use base64::prelude::*;
    use graph::blockchain::TriggerFilter as _;
    use graph::firehose::{CallToFilter, CombinedFilter, LogFilter, MultiLogFilter};
    use graph::petgraph::graphmap::GraphMap;
    use graph::prelude::ethabi::ethereum_types::H256;
    use graph::prelude::web3::types::Address;
    use graph::prelude::web3::types::Bytes;
    use graph::prelude::EthereumCall;
    use hex::ToHex;
    use itertools::Itertools;
    use prost::Message;
    use prost_types::Any;

    use std::collections::{HashMap, HashSet};
    use std::iter::FromIterator;
    use std::str::FromStr;

    #[test]
    fn ethereum_log_filter_codec() {
        let hex_addr = "0x4c7b8591c50f4ad308d07d6294f2945e074420f5";
        let address = Address::from_str(hex_addr).expect("unable to parse addr");
        assert_eq!(hex_addr, format!("0x{}", address.encode_hex::<String>()));

        let event_sigs = vec![
            "0xafb42f194014ece77df0f9e4bc3ced9757555dc1fe7dc803161a2de3b7c4839a",
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        ];

        let hex_sigs = event_sigs
            .iter()
            .map(|addr| {
                format!(
                    "0x{}",
                    H256::from_str(addr)
                        .expect("unable to parse addr")
                        .encode_hex::<String>()
                )
            })
            .collect_vec();

        assert_eq!(event_sigs, hex_sigs);

        let sigs = event_sigs
            .iter()
            .map(|addr| {
                H256::from_str(addr)
                    .expect("unable to parse addr")
                    .to_fixed_bytes()
                    .to_vec()
            })
            .collect_vec();

        let filter = LogFilter {
            addresses: vec![address.to_fixed_bytes().to_vec()],
            event_signatures: sigs,
        };
        // This base64 was provided by Streamingfast as a binding example of the expected encoded for the
        // addresses and signatures above.
        let expected_base64 = "CloKFEx7hZHFD0rTCNB9YpTylF4HRCD1EiCvtC8ZQBTs533w+eS8PO2XV1Vdwf59yAMWGi3jt8SDmhIg3fJSrRviyJtpwrBo/DeNqpUrp/FjxKEWKPVaTfUjs+8=";

        let filter = MultiLogFilter {
            log_filters: vec![filter],
        };

        let output = BASE64_STANDARD.encode(filter.encode_to_vec());
        assert_eq!(expected_base64, output);
    }

    #[test]
    fn ethereum_call_filter_codec() {
        let hex_addr = "0xeed2b7756e295a9300e53dd049aeb0751899bae3";
        let sig = "a9059cbb";
        let mut fs: FunctionSelector = [0u8; 4];
        let hex_sig = hex::decode(sig).expect("failed to parse sig");
        fs.copy_from_slice(&hex_sig[..]);

        let actual_sig = hex::encode(fs);
        assert_eq!(sig, actual_sig);

        let filter = LogFilter {
            addresses: vec![Address::from_str(hex_addr)
                .expect("failed to parse address")
                .to_fixed_bytes()
                .to_vec()],
            event_signatures: vec![fs.to_vec()],
        };

        // This base64 was provided by Streamingfast as a binding example of the expected encoded for the
        // addresses and signatures above.
        let expected_base64 = "ChTu0rd1bilakwDlPdBJrrB1GJm64xIEqQWcuw==";

        let output = BASE64_STANDARD.encode(filter.encode_to_vec());
        assert_eq!(expected_base64, output);
    }

    #[test]
    fn ethereum_trigger_filter_to_firehose_specialise_wildcards_to_addresses() {
        let address = Address::from_low_u64_be;
        let sig = H256::from_low_u64_le;
        let filter = TriggerFilter {
            log: EthereumLogFilter {
                contracts_and_events_graph: GraphMap::new(),
                wildcard_events: HashMap::from_iter(vec![
                    (sig(1), false),
                    (sig(2), false),
                    (sig(3), false),
                    (sig(4), false),
                ]),
                events_with_topic_filters: HashMap::new(),
                addresses: HashSet::from_iter(vec![
                    address(900),
                    address(901),
                    address(902),
                    address(903),
                ]),
            },
            call: EthereumCallFilter {
                contract_addresses_function_signatures: HashMap::default(),
                wildcard_signatures: HashSet::from_iter(vec![]),
            },
            block: EthereumBlockFilter {
                polling_intervals: HashSet::default(),
                contract_addresses: HashSet::default(),
                trigger_every_block: false,
            },
        };

        let expected_call_filters: Vec<CallToFilter> = vec![];

        let expected_log_filters = vec![LogFilter {
            addresses: vec![
                address(900).to_fixed_bytes().to_vec(),
                address(901).to_fixed_bytes().to_vec(),
                address(902).to_fixed_bytes().to_vec(),
                address(903).to_fixed_bytes().to_vec(),
            ],
            event_signatures: vec![
                sig(1).to_fixed_bytes().to_vec(),
                sig(2).to_fixed_bytes().to_vec(),
                sig(3).to_fixed_bytes().to_vec(),
                sig(4).to_fixed_bytes().to_vec(),
            ],
        }];

        let firehose_filter = filter.clone().to_firehose_filter();
        assert_eq!(1, firehose_filter.len());

        let firehose_filter: HashMap<_, _> = HashMap::from_iter::<Vec<(String, Any)>>(
            firehose_filter
                .into_iter()
                .map(|any| (any.type_url.clone(), any))
                .collect_vec(),
        );

        let mut combined_filter = &firehose_filter
            .get(COMBINED_FILTER_TYPE_URL)
            .expect("a CombinedFilter")
            .value[..];

        let combined_filter =
            CombinedFilter::decode(&mut combined_filter).expect("combined filter to decode");

        let CombinedFilter {
            log_filters: mut actual_log_filters,
            call_filters: mut actual_call_filters,
            send_all_block_headers: actual_send_all_block_headers,
        } = combined_filter;

        actual_call_filters.sort_by(|a, b| a.addresses.cmp(&b.addresses));
        for filter in actual_call_filters.iter_mut() {
            filter.signatures.sort();
        }
        assert_eq!(expected_call_filters, actual_call_filters);

        actual_log_filters.sort_by(|a, b| a.addresses.cmp(&b.addresses));
        for filter in actual_log_filters.iter_mut() {
            filter.event_signatures.sort();
            filter.addresses.sort();
        }
        assert_eq!(expected_log_filters, actual_log_filters);
        assert_eq!(false, actual_send_all_block_headers);
    }

    #[test]
    fn ethereum_trigger_filter_to_firehose() {
        let address = Address::from_low_u64_be;
        let sig = H256::from_low_u64_le;
        let mut filter = TriggerFilter {
            log: EthereumLogFilter {
                contracts_and_events_graph: GraphMap::new(),
                wildcard_events: HashMap::new(),
                events_with_topic_filters: HashMap::new(),
                addresses: HashSet::default(),
            },
            call: EthereumCallFilter {
                contract_addresses_function_signatures: HashMap::from_iter(vec![
                    (address(0), (0, HashSet::from_iter(vec![[0u8; 4]]))),
                    (address(1), (1, HashSet::from_iter(vec![[1u8; 4]]))),
                    (address(2), (2, HashSet::new())),
                ]),
                wildcard_signatures: HashSet::new(),
            },
            block: EthereumBlockFilter {
                polling_intervals: HashSet::from_iter(vec![(1, 10), (3, 24)]),
                contract_addresses: HashSet::from_iter([
                    (100, address(1000)),
                    (200, address(2000)),
                    (300, address(3000)),
                    (400, address(1000)),
                    (500, address(1000)),
                ]),
                trigger_every_block: false,
            },
        };

        let expected_call_filters = vec![
            CallToFilter {
                addresses: vec![address(0).to_fixed_bytes().to_vec()],
                signatures: vec![[0u8; 4].to_vec()],
            },
            CallToFilter {
                addresses: vec![address(1).to_fixed_bytes().to_vec()],
                signatures: vec![[1u8; 4].to_vec()],
            },
            CallToFilter {
                addresses: vec![address(2).to_fixed_bytes().to_vec()],
                signatures: vec![],
            },
            CallToFilter {
                addresses: vec![address(1000).to_fixed_bytes().to_vec()],
                signatures: vec![],
            },
            CallToFilter {
                addresses: vec![address(2000).to_fixed_bytes().to_vec()],
                signatures: vec![],
            },
            CallToFilter {
                addresses: vec![address(3000).to_fixed_bytes().to_vec()],
                signatures: vec![],
            },
        ];

        filter.log.contracts_and_events_graph.add_edge(
            LogFilterNode::Contract(address(10)),
            LogFilterNode::Event(sig(100)),
            false,
        );
        filter.log.contracts_and_events_graph.add_edge(
            LogFilterNode::Contract(address(10)),
            LogFilterNode::Event(sig(101)),
            false,
        );
        filter.log.contracts_and_events_graph.add_edge(
            LogFilterNode::Contract(address(20)),
            LogFilterNode::Event(sig(100)),
            false,
        );

        let expected_log_filters = vec![
            LogFilter {
                addresses: vec![address(10).to_fixed_bytes().to_vec()],
                event_signatures: vec![sig(101).to_fixed_bytes().to_vec()],
            },
            LogFilter {
                addresses: vec![
                    address(10).to_fixed_bytes().to_vec(),
                    address(20).to_fixed_bytes().to_vec(),
                ],
                event_signatures: vec![sig(100).to_fixed_bytes().to_vec()],
            },
        ];

        let firehose_filter = filter.clone().to_firehose_filter();
        assert_eq!(1, firehose_filter.len());

        let firehose_filter: HashMap<_, _> = HashMap::from_iter::<Vec<(String, Any)>>(
            firehose_filter
                .into_iter()
                .map(|any| (any.type_url.clone(), any))
                .collect_vec(),
        );

        let mut combined_filter = &firehose_filter
            .get(COMBINED_FILTER_TYPE_URL)
            .expect("a CombinedFilter")
            .value[..];

        let combined_filter =
            CombinedFilter::decode(&mut combined_filter).expect("combined filter to decode");

        let CombinedFilter {
            log_filters: mut actual_log_filters,
            call_filters: mut actual_call_filters,
            send_all_block_headers: actual_send_all_block_headers,
        } = combined_filter;

        actual_call_filters.sort_by(|a, b| a.addresses.cmp(&b.addresses));
        for filter in actual_call_filters.iter_mut() {
            filter.signatures.sort();
        }
        assert_eq!(expected_call_filters, actual_call_filters);

        actual_log_filters.sort_by(|a, b| a.addresses.cmp(&b.addresses));
        for filter in actual_log_filters.iter_mut() {
            filter.event_signatures.sort();
        }
        assert_eq!(expected_log_filters, actual_log_filters);
        assert_eq!(true, actual_send_all_block_headers);
    }

    #[test]
    fn ethereum_trigger_filter_to_firehose_every_block_plus_logfilter() {
        let address = Address::from_low_u64_be;
        let sig = H256::from_low_u64_le;
        let mut filter = TriggerFilter {
            log: EthereumLogFilter {
                contracts_and_events_graph: GraphMap::new(),
                wildcard_events: HashMap::new(),
                events_with_topic_filters: HashMap::new(),
                addresses: HashSet::default(),
            },
            call: EthereumCallFilter {
                contract_addresses_function_signatures: HashMap::new(),
                wildcard_signatures: HashSet::new(),
            },
            block: EthereumBlockFilter {
                polling_intervals: HashSet::default(),
                contract_addresses: HashSet::new(),
                trigger_every_block: true,
            },
        };

        filter.log.contracts_and_events_graph.add_edge(
            LogFilterNode::Contract(address(10)),
            LogFilterNode::Event(sig(101)),
            false,
        );

        let expected_log_filters = vec![LogFilter {
            addresses: vec![address(10).to_fixed_bytes().to_vec()],
            event_signatures: vec![sig(101).to_fixed_bytes().to_vec()],
        }];

        let firehose_filter = filter.clone().to_firehose_filter();
        assert_eq!(1, firehose_filter.len());

        let firehose_filter: HashMap<_, _> = HashMap::from_iter::<Vec<(String, Any)>>(
            firehose_filter
                .into_iter()
                .map(|any| (any.type_url.clone(), any))
                .collect_vec(),
        );

        let mut combined_filter = &firehose_filter
            .get(COMBINED_FILTER_TYPE_URL)
            .expect("a CombinedFilter")
            .value[..];

        let combined_filter =
            CombinedFilter::decode(&mut combined_filter).expect("combined filter to decode");

        let CombinedFilter {
            log_filters: mut actual_log_filters,
            call_filters: actual_call_filters,
            send_all_block_headers: actual_send_all_block_headers,
        } = combined_filter;

        assert_eq!(0, actual_call_filters.len());

        actual_log_filters.sort_by(|a, b| a.addresses.cmp(&b.addresses));
        for filter in actual_log_filters.iter_mut() {
            filter.event_signatures.sort();
        }
        assert_eq!(expected_log_filters, actual_log_filters);

        assert_eq!(true, actual_send_all_block_headers);
    }

    #[test]
    fn matching_ethereum_call_filter() {
        let call = |to: Address, input: Vec<u8>| EthereumCall {
            to,
            input: bytes(input),
            ..Default::default()
        };

        let mut filter = EthereumCallFilter {
            contract_addresses_function_signatures: HashMap::from_iter(vec![
                (address(0), (0, HashSet::from_iter(vec![[0u8; 4]]))),
                (address(1), (1, HashSet::from_iter(vec![[1u8; 4]]))),
                (address(2), (2, HashSet::new())),
            ]),
            wildcard_signatures: HashSet::new(),
        };
        let filter2 = EthereumCallFilter {
            contract_addresses_function_signatures: HashMap::from_iter(vec![(
                address(0),
                (0, HashSet::from_iter(vec![[10u8; 4]])),
            )]),
            wildcard_signatures: HashSet::from_iter(vec![[11u8; 4]]),
        };

        assert_eq!(
            false,
            filter.matches(&call(address(2), vec![])),
            "call with empty bytes are always ignore, whatever the condition"
        );

        assert_eq!(
            false,
            filter.matches(&call(address(4), vec![1; 36])),
            "call with incorrect address should be ignored"
        );

        assert_eq!(
            true,
            filter.matches(&call(address(1), vec![1; 36])),
            "call with correct address & signature should match"
        );

        assert_eq!(
            true,
            filter.matches(&call(address(1), vec![1; 32])),
            "call with correct address & signature, but with incorrect input size should match"
        );

        assert_eq!(
            false,
            filter.matches(&call(address(1), vec![4u8; 36])),
            "call with correct address but incorrect signature for a specific contract filter (i.e. matches some signatures) should be ignored"
        );

        assert_eq!(
            false,
            filter.matches(&call(address(0), vec![11u8; 36])),
            "this signature should not match filter1, this avoid false passes if someone changes the code"
        );
        assert_eq!(
            false,
            filter2.matches(&call(address(1), vec![10u8; 36])),
            "this signature should not match filter2 because the address is not the expected one"
        );
        assert_eq!(
            true,
            filter2.matches(&call(address(0), vec![10u8; 36])),
            "this signature should match filter2 on the non wildcard clause"
        );
        assert_eq!(
            true,
            filter2.matches(&call(address(0), vec![11u8; 36])),
            "this signature should match filter2 on the wildcard clause"
        );

        // extend filter1 and test the filter 2 stuff again
        filter.extend(filter2);
        assert_eq!(
            true,
            filter.matches(&call(address(0), vec![11u8; 36])),
            "this signature should not match filter1, this avoid false passes if someone changes the code"
        );
        assert_eq!(
            false,
            filter.matches(&call(address(1), vec![10u8; 36])),
            "this signature should not match filter2 because the address is not the expected one"
        );
        assert_eq!(
            true,
            filter.matches(&call(address(0), vec![10u8; 36])),
            "this signature should match filter2 on the non wildcard clause"
        );
        assert_eq!(
            true,
            filter.matches(&call(address(0), vec![11u8; 36])),
            "this signature should match filter2 on the wildcard clause"
        );
    }

    #[test]
    fn extending_ethereum_block_filter_no_found() {
        let mut base = EthereumBlockFilter {
            polling_intervals: HashSet::new(),
            contract_addresses: HashSet::new(),
            trigger_every_block: false,
        };

        let extension = EthereumBlockFilter {
            polling_intervals: HashSet::from_iter(vec![(1, 3)]),
            contract_addresses: HashSet::from_iter(vec![(10, address(1))]),
            trigger_every_block: false,
        };

        base.extend(extension);

        assert_eq!(
            HashSet::from_iter(vec![(10, address(1))]),
            base.contract_addresses,
        );

        assert_eq!(HashSet::from_iter(vec![(1, 3)]), base.polling_intervals,);
    }

    #[test]
    fn extending_ethereum_block_filter_conflict_includes_one_copy() {
        let mut base = EthereumBlockFilter {
            polling_intervals: HashSet::from_iter(vec![(3, 3)]),
            contract_addresses: HashSet::from_iter(vec![(10, address(1))]),
            trigger_every_block: false,
        };

        let extension = EthereumBlockFilter {
            polling_intervals: HashSet::from_iter(vec![(2, 3), (3, 3)]),
            contract_addresses: HashSet::from_iter(vec![(2, address(1))]),
            trigger_every_block: false,
        };

        base.extend(extension);

        assert_eq!(
            HashSet::from_iter(vec![(2, address(1))]),
            base.contract_addresses,
        );

        assert_eq!(
            HashSet::from_iter(vec![(2, 3), (3, 3)]),
            base.polling_intervals,
        );
    }

    #[test]
    fn extending_ethereum_block_filter_conflict_doesnt_include_both_copies() {
        let mut base = EthereumBlockFilter {
            polling_intervals: HashSet::from_iter(vec![(2, 3)]),
            contract_addresses: HashSet::from_iter(vec![(2, address(1))]),
            trigger_every_block: false,
        };

        let extension = EthereumBlockFilter {
            polling_intervals: HashSet::from_iter(vec![(3, 3), (2, 3)]),
            contract_addresses: HashSet::from_iter(vec![(10, address(1))]),
            trigger_every_block: false,
        };

        base.extend(extension);

        assert_eq!(
            HashSet::from_iter(vec![(2, address(1))]),
            base.contract_addresses,
        );

        assert_eq!(
            HashSet::from_iter(vec![(2, 3), (3, 3)]),
            base.polling_intervals,
        );
    }

    #[test]
    fn extending_ethereum_block_filter_every_block_in_ext() {
        let mut base = EthereumBlockFilter {
            polling_intervals: HashSet::new(),
            contract_addresses: HashSet::default(),
            trigger_every_block: false,
        };

        let extension = EthereumBlockFilter {
            polling_intervals: HashSet::new(),
            contract_addresses: HashSet::default(),
            trigger_every_block: true,
        };

        base.extend(extension);

        assert_eq!(true, base.trigger_every_block);
    }

    #[test]
    fn extending_ethereum_block_filter_every_block_in_base_and_merge_contract_addresses_and_polling_intervals(
    ) {
        let mut base = EthereumBlockFilter {
            polling_intervals: HashSet::from_iter(vec![(10, 3)]),
            contract_addresses: HashSet::from_iter(vec![(10, address(2))]),
            trigger_every_block: true,
        };

        let extension = EthereumBlockFilter {
            polling_intervals: HashSet::new(),
            contract_addresses: HashSet::from_iter(vec![]),
            trigger_every_block: false,
        };

        base.extend(extension);

        assert_eq!(true, base.trigger_every_block);
        assert_eq!(
            HashSet::from_iter(vec![(10, address(2))]),
            base.contract_addresses,
        );
        assert_eq!(HashSet::from_iter(vec![(10, 3)]), base.polling_intervals,);
    }

    #[test]
    fn extending_ethereum_block_filter_every_block_in_ext_and_merge_contract_addresses() {
        let mut base = EthereumBlockFilter {
            polling_intervals: HashSet::from_iter(vec![(10, 3)]),
            contract_addresses: HashSet::from_iter(vec![(10, address(2))]),
            trigger_every_block: false,
        };

        let extension = EthereumBlockFilter {
            polling_intervals: HashSet::from_iter(vec![(10, 3)]),
            contract_addresses: HashSet::from_iter(vec![(10, address(1))]),
            trigger_every_block: true,
        };

        base.extend(extension);

        assert_eq!(true, base.trigger_every_block);
        assert_eq!(
            HashSet::from_iter(vec![(10, address(2)), (10, address(1))]),
            base.contract_addresses,
        );
        assert_eq!(
            HashSet::from_iter(vec![(10, 3), (10, 3)]),
            base.polling_intervals,
        );
    }

    #[test]
    fn extending_ethereum_call_filter() {
        let mut base = EthereumCallFilter {
            contract_addresses_function_signatures: HashMap::from_iter(vec![
                (
                    Address::from_low_u64_be(0),
                    (0, HashSet::from_iter(vec![[0u8; 4]])),
                ),
                (
                    Address::from_low_u64_be(1),
                    (1, HashSet::from_iter(vec![[1u8; 4]])),
                ),
            ]),
            wildcard_signatures: HashSet::new(),
        };
        let extension = EthereumCallFilter {
            contract_addresses_function_signatures: HashMap::from_iter(vec![
                (
                    Address::from_low_u64_be(0),
                    (2, HashSet::from_iter(vec![[2u8; 4]])),
                ),
                (
                    Address::from_low_u64_be(3),
                    (3, HashSet::from_iter(vec![[3u8; 4]])),
                ),
            ]),
            wildcard_signatures: HashSet::new(),
        };
        base.extend(extension);

        assert_eq!(
            base.contract_addresses_function_signatures
                .get(&Address::from_low_u64_be(0)),
            Some(&(0, HashSet::from_iter(vec![[0u8; 4], [2u8; 4]])))
        );
        assert_eq!(
            base.contract_addresses_function_signatures
                .get(&Address::from_low_u64_be(3)),
            Some(&(3, HashSet::from_iter(vec![[3u8; 4]])))
        );
        assert_eq!(
            base.contract_addresses_function_signatures
                .get(&Address::from_low_u64_be(1)),
            Some(&(1, HashSet::from_iter(vec![[1u8; 4]])))
        );
    }

    fn address(id: u64) -> Address {
        Address::from_low_u64_be(id)
    }

    fn bytes(value: Vec<u8>) -> Bytes {
        Bytes::from(value)
    }
}

// Tests `eth_get_logs_filters` in instances where all events are filtered on by all contracts.
// This represents, for example, the relationship between dynamic data sources and their events.
#[test]
fn complete_log_filter() {
    use std::collections::BTreeSet;

    // Test a few combinations of complete graphs.
    for i in [1, 2] {
        let events: BTreeSet<_> = (0..i).map(H256::from_low_u64_le).collect();

        for j in [1, 1000, 2000, 3000] {
            let contracts: BTreeSet<_> = (0..j).map(Address::from_low_u64_le).collect();

            // Construct the complete bipartite graph with i events and j contracts.
            let mut contracts_and_events_graph = GraphMap::new();
            for &contract in &contracts {
                for &event in &events {
                    contracts_and_events_graph.add_edge(
                        LogFilterNode::Contract(contract),
                        LogFilterNode::Event(event),
                        false,
                    );
                }
            }

            // Run `eth_get_logs_filters`, which is what we want to test.
            let logs_filters: Vec<_> = EthereumLogFilter {
                contracts_and_events_graph,
                wildcard_events: HashMap::new(),
                events_with_topic_filters: HashMap::new(),
                addresses: HashSet::default(),
            }
            .eth_get_logs_filters()
            .collect();

            // Assert that a contract or event is filtered on iff it was present in the graph.
            assert_eq!(
                logs_filters
                    .iter()
                    .flat_map(|l| l.contracts.iter())
                    .copied()
                    .collect::<BTreeSet<_>>(),
                contracts
            );
            assert_eq!(
                logs_filters
                    .iter()
                    .flat_map(|l| l.event_signatures.iter())
                    .copied()
                    .collect::<BTreeSet<_>>(),
                events
            );

            // Assert that chunking works.
            for filter in logs_filters {
                assert!(filter.contracts.len() <= ENV_VARS.get_logs_max_contracts);
            }
        }
    }
}

#[test]
fn log_filter_require_transacion_receipt_method() {
    // test data
    let event_signature_a = H256::zero();
    let event_signature_b = H256::from_low_u64_be(1);
    let event_signature_c = H256::from_low_u64_be(2);
    let contract_a = Address::from_low_u64_be(3);
    let contract_b = Address::from_low_u64_be(4);
    let contract_c = Address::from_low_u64_be(5);

    let wildcard_event_with_receipt = H256::from_low_u64_be(6);
    let wildcard_event_without_receipt = H256::from_low_u64_be(7);
    let wildcard_events = [
        (wildcard_event_with_receipt, true),
        (wildcard_event_without_receipt, false),
    ]
    .into_iter()
    .collect();

    let events_with_topic_filters = HashMap::new(); // TODO(krishna): Test events with topic filters

    let alien_event_signature = H256::from_low_u64_be(8); // those will not be inserted in the graph
    let alien_contract_address = Address::from_low_u64_be(9);

    // test graph nodes
    let event_a_node = LogFilterNode::Event(event_signature_a);
    let event_b_node = LogFilterNode::Event(event_signature_b);
    let event_c_node = LogFilterNode::Event(event_signature_c);
    let contract_a_node = LogFilterNode::Contract(contract_a);
    let contract_b_node = LogFilterNode::Contract(contract_b);
    let contract_c_node = LogFilterNode::Contract(contract_c);

    // build test graph with the following layout:
    //
    // ```dot
    // graph bipartite {
    //
    //     // conected and require a receipt
    //     event_a  contract_a [ receipt=true  ]
    //     event_b  contract_b [ receipt=true  ]
    //     event_c  contract_c [ receipt=true  ]
    //
    //     // connected but don't require a receipt
    //     event_a  contract_b [ receipt=false ]
    //     event_b  contract_a [ receipt=false ]
    // }
    // ```
    let mut contracts_and_events_graph = GraphMap::new();

    let event_a_id = contracts_and_events_graph.add_node(event_a_node);
    let event_b_id = contracts_and_events_graph.add_node(event_b_node);
    let event_c_id = contracts_and_events_graph.add_node(event_c_node);
    let contract_a_id = contracts_and_events_graph.add_node(contract_a_node);
    let contract_b_id = contracts_and_events_graph.add_node(contract_b_node);
    let contract_c_id = contracts_and_events_graph.add_node(contract_c_node);
    contracts_and_events_graph.add_edge(event_a_id, contract_a_id, true);
    contracts_and_events_graph.add_edge(event_b_id, contract_b_id, true);
    contracts_and_events_graph.add_edge(event_a_id, contract_b_id, false);
    contracts_and_events_graph.add_edge(event_b_id, contract_a_id, false);
    contracts_and_events_graph.add_edge(event_c_id, contract_c_id, true);

    let filter = EthereumLogFilter {
        contracts_and_events_graph,
        wildcard_events,
        events_with_topic_filters,
        addresses: HashSet::default(),
    };

    let empty_vec: Vec<H256> = vec![];

    // connected contracts and events graph
    assert!(filter.requires_transaction_receipt(&event_signature_a, Some(&contract_a), &empty_vec));
    assert!(filter.requires_transaction_receipt(&event_signature_b, Some(&contract_b), &empty_vec));
    assert!(filter.requires_transaction_receipt(&event_signature_c, Some(&contract_c), &empty_vec));
    assert!(!filter.requires_transaction_receipt(
        &event_signature_a,
        Some(&contract_b),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(
        &event_signature_b,
        Some(&contract_a),
        &empty_vec
    ));

    // Event C and Contract C are not connected to the other events and contracts
    assert!(!filter.requires_transaction_receipt(
        &event_signature_a,
        Some(&contract_c),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(
        &event_signature_b,
        Some(&contract_c),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(
        &event_signature_c,
        Some(&contract_a),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(
        &event_signature_c,
        Some(&contract_b),
        &empty_vec
    ));

    // Wildcard events
    assert!(filter.requires_transaction_receipt(&wildcard_event_with_receipt, None, &empty_vec));
    assert!(!filter.requires_transaction_receipt(
        &wildcard_event_without_receipt,
        None,
        &empty_vec
    ));

    // Alien events and contracts always return false
    assert!(!filter.requires_transaction_receipt(
        &alien_event_signature,
        Some(&alien_contract_address),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(&alien_event_signature, None, &empty_vec),);
    assert!(!filter.requires_transaction_receipt(
        &alien_event_signature,
        Some(&contract_a),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(
        &alien_event_signature,
        Some(&contract_b),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(
        &alien_event_signature,
        Some(&contract_c),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(
        &event_signature_a,
        Some(&alien_contract_address),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(
        &event_signature_b,
        Some(&alien_contract_address),
        &empty_vec
    ));
    assert!(!filter.requires_transaction_receipt(
        &event_signature_c,
        Some(&alien_contract_address),
        &empty_vec
    ));
}
