use graph::prelude::{
    alloy::{
        contract::{ContractInstance, Interface},
        dyn_abi::DynSolValue,
        json_abi::JsonAbi,
        network::{Ethereum, TransactionBuilder},
        primitives::{Address, Bytes, U256},
        providers::{Provider, ProviderBuilder, WalletProvider},
        rpc::types::{Block, TransactionReceipt, TransactionRequest},
        signers::local::PrivateKeySigner,
    },
    hex, lazy_static,
    serde_json::{self, Value},
};

use crate::{error, helpers::TestFile, status, CONFIG};

// `FROM` and `FROM_KEY` are the address and private key of the first
// account that anvil prints on startup
lazy_static! {
    static ref FROM: Address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        .parse()
        .unwrap();
    static ref FROM_KEY: PrivateKeySigner =
        "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
            .parse()
            .unwrap();
    static ref CONTRACTS: Vec<Contract> = {
        vec![
            Contract {
                name: "SimpleContract".to_string(),
                address: "0x5fbdb2315678afecb367f032d93f642f64180aa3"
                    .parse()
                    .unwrap(),
            },
            Contract {
                name: "LimitedContract".to_string(),
                address: "0x0b306bf915c4d645ff596e518faf3f9669b97016"
                    .parse()
                    .unwrap(),
            },
            Contract {
                name: "RevertingContract".to_string(),
                address: "0x959922be3caee4b8cd9a407cc3ac1c251c2007b1"
                    .parse()
                    .unwrap(),
            },
            Contract {
                name: "OverloadedContract".to_string(),
                address: "0x9a9f2ccfde556a7e9ff0848998aa4a0cfd8863ae"
                    .parse()
                    .unwrap(),
            },
            Contract {
                name: "DeclaredCallsContract".to_string(),
                address: "0x68b1d87f95878fe05b998f19b66f4baba5de1aed"
                    .parse()
                    .unwrap(),
            },
        ]
    };
}

/// Helper function to create a DynSolValue::Uint from a u64 value
pub fn uint(val: u64, bits: usize) -> DynSolValue {
    DynSolValue::Uint(U256::from(val), bits)
}

/// Helper function to create a DynSolValue::Address
pub fn addr(a: Address) -> DynSolValue {
    DynSolValue::Address(a)
}

/// Helper function to create a DynSolValue::String
pub fn string(s: &str) -> DynSolValue {
    DynSolValue::String(s.to_string())
}

#[derive(Debug, Clone)]
pub struct Contract {
    pub name: String,
    pub address: Address,
}

impl Contract {
    fn provider() -> impl Provider<Ethereum> + WalletProvider + Clone {
        ProviderBuilder::new()
            .wallet(FROM_KEY.clone())
            .connect_http(CONFIG.eth.url().parse().unwrap())
    }

    async fn exists(&self) -> bool {
        let bytes = self.code().await;
        !bytes.is_empty()
    }

    pub async fn code(&self) -> Bytes {
        let provider = Self::provider();
        provider.get_code_at(self.address).await.unwrap()
    }

    fn code_and_abi(name: &str) -> anyhow::Result<(Bytes, JsonAbi)> {
        let bin = TestFile::new(&format!("contracts/out/{}.sol/{}.json", name, name));

        let json: Value = serde_json::from_reader(bin.reader()?).unwrap();
        let abi: JsonAbi = serde_json::from_value(json["abi"].clone())?;
        let code_hex = json["bytecode"]["object"].as_str().unwrap();
        let code = Bytes::from(hex::decode(code_hex.trim_start_matches("0x"))?);
        Ok((code, abi))
    }

    pub async fn deploy(name: &str) -> anyhow::Result<Self> {
        let provider = Self::provider();
        let (code, _abi) = Self::code_and_abi(name)?;

        let deploy_tx = TransactionRequest::default().with_deploy_code(code);

        let receipt = provider
            .send_transaction(deploy_tx)
            .await?
            .with_required_confirmations(1)
            .get_receipt()
            .await?;

        let address = receipt
            .contract_address
            .ok_or_else(|| anyhow::anyhow!("No contract address in receipt"))?;

        Ok(Self {
            name: name.to_string(),
            address,
        })
    }

    pub async fn call(
        &self,
        func: &str,
        params: &[DynSolValue],
    ) -> anyhow::Result<TransactionReceipt> {
        let provider = Self::provider();
        let (_, abi) = Self::code_and_abi(&self.name)?;

        let interface = Interface::new(abi);
        let contract = ContractInstance::new(self.address, provider, interface);

        let receipt = contract
            .function(func, params)?
            .send()
            .await?
            .with_required_confirmations(1)
            .get_receipt()
            .await?;

        Ok(receipt)
    }

    pub async fn deploy_all() -> anyhow::Result<Vec<Self>> {
        let mut contracts = Vec::new();
        status!("contracts", "Deploying contracts");
        for contract in &*CONTRACTS {
            let mut contract = contract.clone();
            if !contract.exists().await {
                status!(
                    "contracts",
                    "Contract {} does not exist, deploying",
                    contract.name
                );
                let old_address = contract.address;
                contract = Self::deploy(&contract.name).await?;
                if old_address != contract.address {
                    error!(
                        "contracts",
                        "Contract address for {} changed from {:?} to {:?}",
                        contract.name,
                        old_address,
                        contract.address
                    );
                } else {
                    status!(
                        "contracts",
                        "Deployed contract {} at {:?}",
                        contract.name,
                        contract.address
                    );
                }
                // Some tests want 10 calls to `emitTrigger` in place
                if contract.name == "SimpleContract" {
                    status!("contracts", "Calling SimpleContract.emitTrigger 10 times");
                    for i in 1u64..=10 {
                        contract.call("emitTrigger", &[uint(i, 16)]).await.unwrap();
                    }
                }
                // Declared calls tests need a Transfer
                if contract.name == "DeclaredCallsContract" {
                    status!("contracts", "Emitting transfers from DeclaredCallsContract");
                    let addr1: Address = "0x1111111111111111111111111111111111111111"
                        .parse()
                        .unwrap();
                    let addr2: Address = "0x2222222222222222222222222222222222222222"
                        .parse()
                        .unwrap();
                    let addr3: Address = "0x3333333333333333333333333333333333333333"
                        .parse()
                        .unwrap();
                    let addr4: Address = "0x4444444444444444444444444444444444444444"
                        .parse()
                        .unwrap();

                    contract
                        .call(
                            "emitTransfer",
                            &[addr(addr1), addr(addr2), uint(100u64, 256)],
                        )
                        .await
                        .unwrap();

                    // Emit an asset transfer event to trigger struct field declared calls
                    contract
                        .call(
                            "emitAssetTransfer",
                            &[
                                addr(addr1),
                                uint(150u64, 256),
                                DynSolValue::Bool(true),
                                addr(addr3),
                            ],
                        )
                        .await
                        .unwrap();

                    // Also emit a complex asset event for nested struct testing
                    let values =
                        DynSolValue::Array(vec![uint(1u64, 256), uint(2u64, 256), uint(3u64, 256)]);
                    contract
                        .call(
                            "emitComplexAssetCreated",
                            &[
                                addr(addr4),
                                uint(250u64, 256),
                                DynSolValue::Bool(true),
                                string("Complex Asset Metadata"),
                                values,
                                uint(99u64, 256),
                            ],
                        )
                        .await
                        .unwrap();
                }
                // The topic-filter test needs some events emitted
                if contract.name == "SimpleContract" {
                    status!("contracts", "Emitting anotherTrigger from SimpleContract");
                    let args = &[
                        [uint(1, 256), uint(2, 256), uint(3, 256), string("abc")],
                        [uint(1, 256), uint(1, 256), uint(1, 256), string("abc")],
                        [uint(4, 256), uint(2, 256), uint(3, 256), string("abc")],
                        [uint(4, 256), uint(4, 256), uint(3, 256), string("abc")],
                    ];
                    for arg in args {
                        contract.call("emitAnotherTrigger", arg).await.unwrap();
                    }
                }
            } else {
                status!(
                    "contracts",
                    "Contract {} exists at {:?}",
                    contract.name,
                    contract.address
                );
            }
            contracts.push(contract);
        }
        Ok(contracts)
    }

    pub async fn latest_block() -> Option<Block> {
        let provider = Self::provider();
        provider
            .get_block_by_number(Default::default())
            .await
            .ok()
            .flatten()
    }
}
