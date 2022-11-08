use std::sync::Mutex;

use cid::Cid;

use crate::{components::subgraph::Entity, ipfs_client::CidFile, prelude::Link};

use super::{
    offchain::{Mapping, Source},
    *,
};

#[test]
fn offchain_duplicate() {
    let a = offchain::DataSource {
        kind: "theKind".into(),
        name: "theName".into(),
        manifest_idx: 0,
        source: Source::Ipfs(CidFile {
            cid: Cid::default(),
            path: None,
        }),
        mapping: Mapping {
            language: String::new(),
            api_version: Version::new(0, 0, 0),
            entities: vec![],
            handler: String::new(),
            runtime: Arc::new(vec![]),
            link: Link {
                link: String::new(),
            },
        },
        context: Arc::new(None),
        creation_block: Some(0),
        done_at: Mutex::new(None),
        causality_region: CausalityRegion::ONCHAIN.next(),
    };
    let mut b = a.clone();

    // Equal data sources are duplicates.
    assert!(a.is_duplicate_of(&b));

    // The causality region, the creation block and the done status are ignored in the duplicate check.
    b.causality_region = a.causality_region.next();
    b.creation_block = Some(1);
    *b.done_at.lock().unwrap() = Some(1);
    assert!(a.is_duplicate_of(&b));

    // The manifest idx, the source and the context are relevant for duplicate detection.
    let mut c = a.clone();
    c.manifest_idx = 1;
    assert!(!a.is_duplicate_of(&c));

    let mut c = a.clone();
    c.source = Source::Ipfs(CidFile {
        cid: Cid::default(),
        path: Some("/foo".into()),
    });
    assert!(!a.is_duplicate_of(&c));

    let mut c = a.clone();
    c.context = Arc::new(Some(Entity::new()));
    assert!(!a.is_duplicate_of(&c));
}
