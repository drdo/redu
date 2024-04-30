mod list;
pub mod app;
mod heading;

#[derive(Debug)]
pub enum Event {
    Resize(u16, u16),
    Left,
    Right,
    Up,
    Down,
    Quit,
}

#[derive(Debug)]
pub enum Action {
    Nothing,
    Render,
    Quit,
}
