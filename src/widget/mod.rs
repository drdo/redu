mod list;
pub mod app;

pub enum Event {
    Resize(u16, u16),
    Left,
    Right,
    Up,
    Down,
    Quit,
}

pub enum Action {
    Nothing,
    Render,
    Quit,
}
