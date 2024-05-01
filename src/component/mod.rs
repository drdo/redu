use ratatui::text::Line;

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

trait ToLine {
    fn to_line(&self, width: u16) -> Line;
}
