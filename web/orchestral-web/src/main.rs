#[cfg(feature = "web")]
fn main() {
    dioxus::launch(orchestral_web::app::App);
}

#[cfg(not(feature = "web"))]
fn main() {
    // The deployable entry point is WebAssembly. Keeping a native no-op target
    // lets workspace tooling discover and test the platform-neutral library.
}
