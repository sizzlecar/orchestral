use std::path::PathBuf;

fn main() {
    // The application polls nested async command futures on the main thread.
    // MSVC's 1 MiB default is too small for this in debug builds; reserve the
    // same 8 MiB budget used by the supported Unix hosts. Pages commit on demand.
    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        println!("cargo:rustc-link-arg-bin=orchestral=/STACK:8388608");
    }
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    // Registry packages contain the tested PWA inside the crate. Workspace
    // builds use the canonical generated distribution without duplicating it.
    let packaged = manifest.join("web-dist");
    let assets = if packaged.is_dir() {
        packaged
    } else {
        manifest.join("../../web/orchestral-web/dist")
    };
    assert!(
        assets.join("index.html").is_file(),
        "embedded PWA is missing; run scripts/build_web.sh in the workspace or use a complete published package"
    );
    let assets = assets
        .canonicalize()
        .expect("resolve embedded PWA directory");
    println!("cargo:rerun-if-changed={}", assets.display());
    println!("cargo:rustc-env=ORCHESTRAL_WEB_DIST={}", assets.display());
}
