use crossterm::event::KeyCode;

pub mod app;

#[derive(Debug)]
pub enum Event {
    Resize(u16, u16),
    KeyPress(KeyCode),
}

#[derive(Debug)]
pub enum Action {
    Nothing,
    Render,
    Quit,
}

