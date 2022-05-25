pub use runtime_adapter::RuntimeAdapter;

pub mod abi;
pub mod runtime_adapter;

pub mod generated;

// use crate::protobuf::pbcodec::*;
// use graph::runtime::AscPtr;
// use crate::runtime::generated::*;
// use graph::runtime::IndexForAscTypeId;
// use graph_runtime_derive::*;
// use graph::runtime::AscIndexId;
// use graph_runtime_wasm::asc_abi::class::AscString;

// #[derive(GenerateAscType)]
// #[chain_name(Cosmos)]
// #[derive(graph_runtime_derive::ToAscObj)]
// #[asc_obj_type(AscBlock)]
// #[required(header,evidence)]
// pub struct MyBlock {
//     pub header: ::core::option::Option<Header>,
//     pub evidence: ::core::option::Option<EvidenceList>,
//     pub last_commit: ::core::option::Option<Commit>,
//     pub result_begin_block: ::core::option::Option<ResponseBeginBlock>,
//     pub result_end_block: ::core::option::Option<ResponseEndBlock>,
//     pub transactions: ::prost::alloc::vec::Vec<TxResult>,
//     pub validator_updates: ::prost::alloc::vec::Vec<Validator>,
// }
// #[cfg(test)]
// mod my_test{
//     use super::*;
//     #[test]
//     fn zz(){
//         println!("{} -> {} {}", std::mem::size_of::<MyBlock>(),std::mem::size_of::<MyBlock>()/8, std::mem::size_of::<MyBlock>()%8 );

//         println!("{}", std::mem::size_of::<::core::option::Option<Header>>());
//         println!("{}", std::mem::size_of::<::core::option::Option<EvidenceList>>());
//         println!("{}", std::mem::size_of::<::core::option::Option<Commit>>());
//         println!("{}", std::mem::size_of::<::core::option::Option<ResponseBeginBlock>>());
//         println!("{}", std::mem::size_of::<::core::option::Option<ResponseEndBlock>>());
//         println!("{}", std::mem::size_of::<::prost::alloc::vec::Vec<TxResult>>());
//         println!("{}", std::mem::size_of::<::prost::alloc::vec::Vec<Validator>>());
//     }

//     #[test]
//     fn zzz(){
//         println!("{} -> {} {}", std::mem::size_of::<AscMyBlock>(),std::mem::size_of::<AscMyBlock>()/8, std::mem::size_of::<AscMyBlock>()%8 );

//         println!("{}", std::mem::size_of::<AscPtr<AscHeader>>());
//         println!("{}", std::mem::size_of::<AscPtr<AscEvidenceList>>());
//         println!("{}", std::mem::size_of::<AscPtr<AscCommit>>());
//         println!("{}", std::mem::size_of::<AscPtr<AscResponseBeginBlock>>());
//         println!("{}", std::mem::size_of::<AscPtr<AscResponseEndBlock>>());
//         println!("{}", std::mem::size_of::<AscPtr<AscTxResultArray>>());
//         println!("{}", std::mem::size_of::<AscPtr<AscValidatorArray>>());
//     }

// }

// // #[repr(C)]
// // #[derive(AscType)]
// // pub(crate) struct AscMyBlock {
// //     pub header: AscPtr<AscHeader>,
// //     pub evidence: AscPtr<AscEvidenceList>,
// //     pub last_commit: AscPtr<AscCommit>,
// //     pub result_begin_block: AscPtr<AscResponseBeginBlock>,
// //     pub result_end_block: AscPtr<AscResponseEndBlock>,
// //     pub transactions: AscPtr<AscTxResultArray>,
// //     pub validator_updates: AscPtr<AscValidatorArray>,
// // }

// // impl AscIndexId for AscMyBlock {
// //     const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosMyBlock;
// // }

// // #[repr(C)]
// // #[derive(graph_runtime_derive::AscType)]
// // pub(crate) struct MyAscCoin {
// //     pub denom: graph::runtime::AscPtr<graph_runtime_wasm::asc_abi::class::AscString>,
// //     pub amount: graph::runtime::AscPtr<graph_runtime_wasm::asc_abi::class::AscString>,
// // }

// // impl graph::runtime::AscIndexId for MyAscCoin {
// //     const INDEX_ASC_TYPE_ID: graph::runtime::IndexForAscTypeId = graph::runtime::IndexForAscTypeId::CosmosCoin;
// // }

