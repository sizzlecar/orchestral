mod echo;
mod file_io;
mod http;
mod json_stdout;
pub(crate) mod session;
mod shell;
pub(crate) mod skill_activate;
mod subprocess;
mod support;
pub(crate) mod tool_lookup;

use orchestral_core::action::Action;
use orchestral_core::config::ActionSpec;

use self::echo::EchoAction;
use self::file_io::{FileReadAction, FileWriteAction};
use self::http::HttpAction;
pub(crate) use self::json_stdout::JsonStdoutAction;
use self::shell::ShellAction;
use self::subprocess::SubprocessAction;

pub fn build_builtin_action(spec: &ActionSpec) -> Option<Box<dyn Action>> {
    match spec.kind.as_str() {
        "echo" => Some(Box::new(EchoAction::from_spec(spec))),
        "json_stdout" => Some(Box::new(JsonStdoutAction::from_spec(spec))),
        "http" => Some(Box::new(HttpAction::from_spec(spec))),
        "shell" => Some(Box::new(ShellAction::from_spec(spec))),
        "subprocess" => Some(Box::new(SubprocessAction::from_spec(spec))),
        "file_read" => Some(Box::new(FileReadAction::from_spec(spec))),
        "file_write" => Some(Box::new(FileWriteAction::from_spec(spec))),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
