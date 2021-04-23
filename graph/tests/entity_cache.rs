use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::sync::Arc;

use graph::{components::store::EntityType, mock::MockStore};
use graph::{
    components::store::{DeploymentId, DeploymentLocator},
    prelude::{
        DeploymentHash, Entity, EntityCache, EntityKey, EntityModification, SubgraphStore, Value,
    },
};

lazy_static! {
    static ref SUBGRAPH_ID: DeploymentHash = DeploymentHash::new("entity_cache").unwrap();
    static ref DEPLOYMENT: DeploymentLocator =
        DeploymentLocator::new(DeploymentId::new(-12), SUBGRAPH_ID.clone());
}

fn make_band(id: &'static str, data: Vec<(&str, Value)>) -> (EntityKey, Entity) {
    (
        EntityKey::data(SUBGRAPH_ID.clone(), "Band".to_string(), id.into()),
        Entity::from(data),
    )
}

fn sort_by_entity_key(mut mods: Vec<EntityModification>) -> Vec<EntityModification> {
    mods.sort_by_key(|m| m.entity_key().clone());
    mods
}

#[test]
fn empty_cache_modifications() {
    let store = MockStore::new().writable(&*DEPLOYMENT).unwrap();
    let cache = EntityCache::new(store.clone());
    let result = cache.as_modifications();
    assert_eq!(result.unwrap().modifications, vec![]);
}

#[test]
fn insert_modifications() {
    let mut store = MockStore::new();

    // Return no entities from the store, forcing the cache to treat any `set`
    // operation as an insert.
    store
        .expect_get_many_mock()
        .returning(|_| Ok(BTreeMap::new()));

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store.clone());

    let (mogwai_key, mogwai_data) = make_band(
        "mogwai",
        vec![("id", "mogwai".into()), ("name", "Mogwai".into())],
    );
    cache.set(mogwai_key.clone(), mogwai_data.clone());

    let (sigurros_key, sigurros_data) = make_band(
        "sigurros",
        vec![("id", "sigurros".into()), ("name", "Sigur Ros".into())],
    );
    cache.set(sigurros_key.clone(), sigurros_data.clone());

    let result = cache.as_modifications();
    assert_eq!(
        sort_by_entity_key(result.unwrap().modifications),
        sort_by_entity_key(vec![
            EntityModification::Insert {
                key: mogwai_key,
                data: mogwai_data,
            },
            EntityModification::Insert {
                key: sigurros_key,
                data: sigurros_data,
            }
        ])
    );
}

#[test]
fn overwrite_modifications() {
    let mut store = MockStore::new();

    // Pre-populate the store with entities so that the cache treats
    // every set operation as an overwrite.
    store.expect_get_many_mock().returning(|_| {
        let mut map = BTreeMap::new();

        map.insert(
            EntityType::from("Band"),
            vec![
                make_band(
                    "mogwai",
                    vec![("id", "mogwai".into()), ("name", "Mogwai".into())],
                )
                .1,
                make_band(
                    "sigurros",
                    vec![("id", "sigurros".into()), ("name", "Sigur Ros".into())],
                )
                .1,
            ],
        );

        Ok(map)
    });

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store.clone());

    let (mogwai_key, mogwai_data) = make_band(
        "mogwai",
        vec![
            ("id", "mogwai".into()),
            ("name", "Mogwai".into()),
            ("founded", 1995.into()),
        ],
    );
    cache.set(mogwai_key.clone(), mogwai_data.clone());

    let (sigurros_key, sigurros_data) = make_band(
        "sigurros",
        vec![
            ("id", "sigurros".into()),
            ("name", "Sigur Ros".into()),
            ("founded", 1994.into()),
        ],
    );
    cache.set(sigurros_key.clone(), sigurros_data.clone());

    let result = cache.as_modifications();
    assert_eq!(
        sort_by_entity_key(result.unwrap().modifications),
        sort_by_entity_key(vec![
            EntityModification::Overwrite {
                key: mogwai_key,
                data: mogwai_data,
            },
            EntityModification::Overwrite {
                key: sigurros_key,
                data: sigurros_data,
            }
        ])
    );
}

#[test]
fn consecutive_modifications() {
    let mut store = MockStore::new();

    // Pre-populate the store with data so that we can test setting a field to
    // `Value::Null`.
    store.expect_get_many_mock().returning(|_| {
        let mut map = BTreeMap::new();

        map.insert(
            EntityType::from("Band"),
            vec![
                make_band(
                    "mogwai",
                    vec![
                        ("id", "mogwai".into()),
                        ("name", "Mogwai".into()),
                        ("label", "Chemikal Underground".into()),
                    ],
                )
                .1,
            ],
        );

        Ok(map)
    });

    let store = Arc::new(store);
    let mut cache = EntityCache::new(store.clone());

    // First, add "founded" and change the "label".
    let (update_key, update_data) = make_band(
        "mogwai",
        vec![
            ("id", "mogwai".into()),
            ("founded", 1995.into()),
            ("label", "Rock Action Records".into()),
        ],
    );
    cache.set(update_key.clone(), update_data.clone());

    // Then, just reset the "label".
    let (update_key, update_data) = make_band(
        "mogwai",
        vec![("id", "mogwai".into()), ("label", Value::Null)],
    );
    cache.set(update_key.clone(), update_data.clone());

    // We expect a single overwrite modification for the above that leaves "id"
    // and "name" untouched, sets "founded" and removes the "label" field.
    let result = cache.as_modifications();
    assert_eq!(
        sort_by_entity_key(result.unwrap().modifications),
        sort_by_entity_key(vec![EntityModification::Overwrite {
            key: update_key,
            data: Entity::from(vec![
                ("id", "mogwai".into()),
                ("name", "Mogwai".into()),
                ("founded", 1995.into()),
            ]),
        },])
    );
}
