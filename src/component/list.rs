use std::cmp;
use std::fmt::Display;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::prelude::Widget;
use ratatui::style::Stylize;
use ratatui::widgets::{ListItem, WidgetRef};

use crate::component::{Action, Event};

pub struct List<T> {
    height: u16,
    items: Vec<T>,
    selected: usize,
    offset: usize,
    focused: bool,
}

impl<T> List<T> {
    pub fn new(
        height: u16,
        items: Vec<T>,
        focused: bool,
    ) -> Self
    {
        List {
            height,
            items,
            selected: 0,
            offset: 0,
            focused,
        }
    }

    pub fn set_items(&mut self, items: Vec<T>) {
        self.items = items;
        self.selected = cmp::max(
            0,
            cmp::min(
                self.items.len() as isize - 1,
                self.selected as isize
            ),
        ) as usize;
        self.offset = 0;
    }

    pub fn selected_item(&self) -> Option<&T> {
        if self.items.is_empty() {
            None
        } else {
            Some(&self.items[self.selected])
        }
    }

    pub fn handle_event(&mut self, event: Event) -> Action {
        use Event::*;
        use Action::*;
        match event {
            Resize(_, h) => {
                self.height = h;
                self.fix_offset();
            }
            Up => self.move_selection(-1),
            Down => self.move_selection(1),
            _ => return Nothing,
        }
        Render
    }

    fn move_selection(&mut self, delta: isize) {
        if self.items.is_empty() { return }

        let selected = self.selected as isize;
        let len = self.items.len() as isize;
        self.selected = (selected + delta).rem_euclid(len) as usize;
        self.fix_offset();
    }

    /// Adjust offset to make sure the selected item is visible.
    fn fix_offset(&mut self) {
        let offset = self.offset as isize;
        let selected = self.selected as isize;
        let h = self.height as isize;
        let first_visible = offset;
        let last_visible = offset + h - 1;
        let new_offset =
            if selected < first_visible {
                selected
            } else if last_visible < selected {
                selected - h + 1
            } else {
                offset
            };
        self.offset = new_offset as usize;
    }

}

impl<T: Display> WidgetRef for List<T> {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let items = self.items[self.offset..]
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let item = ListItem::new(item.to_string());
                if index == self.selected && self.focused {
                    item.black().on_white()
                } else {
                    item
                }
            });
        ratatui::widgets::List::new(items).render(area, buf);
    }
}
