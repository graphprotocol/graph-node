pub mod receipts {
    pub mod v1 {
        include!("receipts.v1.rs");
    }
}
pub mod sf {
    pub mod arweave {
        pub mod r#type {
            pub mod v1 {
                include!("sf.arweave.r#type.v1.rs");
            }
        }
    }
    pub mod ethereum {
        pub mod r#type {
            pub mod v2 {
                include!("sf.ethereum.r#type.v2.rs");
            }
        }
    }
    pub mod near {
        pub mod codec {
            pub mod v1 {
                include!("sf.near.codec.v1.rs");
            }
        }
    }
}
pub mod substreams {
    pub mod entity {
        pub mod v1 {
            include!("substreams.entity.v1.rs");
        }
    }
}
