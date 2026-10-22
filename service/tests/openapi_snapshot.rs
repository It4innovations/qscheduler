//! Keeps the repo-root `openapi.json` in sync with the API defined in code.
//! Regenerate with `UPDATE_OPENAPI=1 cargo test -p service --test openapi_snapshot`.

use std::path::Path;

#[test]
fn openapi_snapshot_is_current() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../openapi.json");
    let spec = service::openapi().to_pretty_json().unwrap() + "\n";
    if std::env::var_os("UPDATE_OPENAPI").is_some() {
        std::fs::write(&path, &spec).unwrap();
        return;
    }
    let snapshot = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        snapshot == spec,
        "openapi.json is out of date; regenerate it with \
         `UPDATE_OPENAPI=1 cargo test -p service --test openapi_snapshot` \
         and bump the version if scripts/check_api_version.py requires it"
    );
}
