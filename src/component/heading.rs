use std::fmt::Display;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::{Paragraph, WidgetRef};

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
        let text = {
            let item_str = self.item.to_string();
            format!("--- {item_str} {:-^width$}",
                    "",
                    width = area.width as usize - 4 - item_str.len())
        };
        Paragraph::new(text).render_ref(area, buf);
    }
}