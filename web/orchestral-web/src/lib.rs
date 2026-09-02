//! Typed Dioxus client for Orchestral's remote Agent API.
//!
//! State reduction and SSE framing stay platform-neutral so they can be
//! covered by ordinary Rust tests. Browser effects and components are only
//! compiled for the `web` feature.

pub mod markdown;
pub mod model;
pub mod presentation;
pub mod sse;
pub mod state;

#[cfg(feature = "web")]
pub mod app;
#[cfg(feature = "web")]
pub mod browser;
#[cfg(feature = "web")]
pub mod components;

#[cfg(test)]
mod shell_layout_tests {
    const STYLES: &str = include_str!("../assets/styles.css");

    fn rule(selector: &str) -> &str {
        let marker = format!("\n{selector} {{");
        STYLES
            .split_once(&marker)
            .unwrap_or_else(|| panic!("missing {selector} rule"))
            .1
            .split_once('}')
            .expect("unterminated CSS rule")
            .0
    }

    #[test]
    fn mobile_shell_tracks_the_visual_viewport_without_fixing_body() {
        let body = rule("body");
        assert!(!body.contains("position: fixed"));
        assert!(!body.contains("inset: 0"));

        let main = rule("#main");
        assert!(main.contains("position: fixed"));
        assert!(main.contains("--visual-viewport-top"));
        assert!(main.contains("--visual-viewport-left"));
        assert!(main.contains("--visual-viewport-width"));
        assert!(main.contains("--visual-viewport-height"));

        let shell = rule(".app-shell");
        assert!(shell.contains("height: 100%"));
        assert!(!shell.contains("100vh"));
        assert!(!shell.contains("100svh"));
        assert!(!shell.contains("100dvh"));
    }
}
