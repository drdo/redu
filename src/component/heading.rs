use std::cmp::max;
use std::fmt::Display;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::{Paragraph, WidgetRef};
use crate::component::shorten_to;

pub struct Heading<T> {
    pub item: T,
}

impl<T> Heading<T> {
    pub fn new(item: T) -> Self {
        Heading { item }
    }
}

impl<T: Display> WidgetRef for Heading<T> {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let mut string = "--- ".to_string();
        string.push_str(
            shorten_to(
                &self.item.to_string(),
                area.width as usize - string.len()
            ).as_ref()
        );
        let mut remaining_width = max(
            0,
            area.width as isize - 4 - string.len() as isize
        ) as usize;
        if remaining_width > 0 {
            string.push(' ');
            remaining_width -= 1;
        }
        string.push_str(&"-".repeat(remaining_width));
        Paragraph::new(string).render_ref(area, buf);
    }
}