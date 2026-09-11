fn main() {
    // The application polls nested async command futures on the main thread.
    // MSVC's 1 MiB default is too small for this in debug builds; reserve the
    // same 8 MiB budget used by the supported Unix hosts. Pages commit on demand.
    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        println!("cargo:rustc-link-arg-bin=orchestral=/STACK:8388608");
    }
}
