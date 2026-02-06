use crossterm::event::KeyEvent;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum UiMsg {
    Key(KeyEvent),
    Resize(u16, u16),
    UiTick,
    AnimTick,
    Runtime(crate::runtime::RuntimeMsg),
    SubmitInput(String),
    ScrollUp,
    ScrollDown,
    Interrupt,
    Quit,
}
