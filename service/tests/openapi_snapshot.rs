//! Keeps the repo-root `openapi.json` in sync with the API defined in code.
//! Regenerate with `UPDATE_OPENAPI=1 cargo test -p service --test openapi_snapshot`.
//!
//! The snapshot leaves out `info.version`: release-plz bumps the version in `Cargo.toml` only,
//! so a version in the snapshot would make every release PR stale. The served spec keeps it.

use std::path::Path;

#[test]
fn openapi_snapshot_is_current() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../openapi.json");
    // Edited as text: a serde_json::Value round trip would sort all keys
    let spec = service::openapi().to_pretty_json().unwrap() + "\n";
    let version_line = concat!(",\n    \"version\": \"", env!("CARGO_PKG_VERSION"), "\"");
    assert_eq!(
        spec.matches(version_line).count(),
        1,
        "info.version not found"
    );
    let spec = spec.replacen(version_line, "", 1);
    if std::env::var_os("UPDATE_OPENAPI").is_some() {
        std::fs::write(&path, &spec).unwrap();
        return;
    }
    let snapshot = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        snapshot == spec,
        "openapi.json is out of date; regenerate it with \
         `UPDATE_OPENAPI=1 cargo test -p service --test openapi_snapshot` \
         and run scripts/check_api_version.py for the commit type the change needs"
    );
}
