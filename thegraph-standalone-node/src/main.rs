#[macro_use]
extern crate sentry;

use sentry::integrations::panic::register_panic_handler;
use std::env;

fn main() {
    // Set up Sentry, with release tracking and panic handling;
    // fall back to an empty URL, which will result in no errors being reported
    let sentry_url = env::var_os("THEGRAPH_SENTRY_URL")
        .or(Some("".into()))
        .unwrap();
    let _sentry = sentry::init((
        sentry_url,
        sentry::ClientOptions {
            release: sentry_crate_release!(),
            ..Default::default()
        },
    ));
    register_panic_handler();

    println!("Welcome to The Graph!");
}
