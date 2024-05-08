use std::borrow::Cow;
use std::cmp;
use std::cmp::{max, min};

use camino::{Utf8Path, Utf8PathBuf};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Position, Rect, Size};
use ratatui::prelude::Line;
use ratatui::style::{Style, Stylize};
use ratatui::text::Span;
use ratatui::widgets::{List, ListItem, Paragraph, WidgetRef};

use crate::component::{Action, Event, shorten_to};
use crate::types::{Directory, Entry, File};

struct ListEntry {
    name: Utf8PathBuf,
    size: usize,
    relative_size: f64,
    is_dir: bool,
}

impl ListEntry {
    fn to_line(&self, width: u16, selected: bool) -> Line {
        let size_span = Span::raw(
            format!(" {:>10}", humansize::format_size(self.size, humansize::BINARY))
        );
        let bar_span = Span::raw({
            let max_bar_width: usize = max(16, min(24, (0.1 * width as f64) as usize));
            let bar_width = (self.relative_size * max_bar_width as f64) as usize;
            let bar = format!(
                " [{:#^bar_size$}{:empty_bar_size$}] ",
                "", "",
                bar_size = bar_width,
                empty_bar_size = max_bar_width - bar_width
            );
            bar
        });
        let name_span = {
            let available_width = {
                let used = size_span.content.len() + bar_span.content.len();
                max(0, width as isize - used as isize) as usize
            };
            if self.is_dir
            {
                let mut name = Cow::Borrowed(self.name.as_str());
                if name.chars().last() != Some('/') {
                    name.to_mut().push('/');
                }
                let span = Span::raw(shorten_to(&name, available_width).into_owned())
                    .bold();
                if selected { span.dark_gray() }
                else { span.blue() }
            } else {
                Span::raw(self.name.as_str())
            }
        };
        let style = if selected {
            Style::new().black().on_white()
        } else {
            Style::new()
        };
        Line::from(vec![size_span, bar_span, name_span]).style(style)
    }
}

pub struct App {
    heading_size: Size,
    list_size: Size,
    path: Option<Utf8PathBuf>,
    entries: Vec<ListEntry>,
    selected: usize,
    offset: usize,
}

impl App {
    /// `files` is expected to be sorted by size, largest first.
    pub fn new<'a, P>(
        screen: Size,
        path: Option<P>,
        files: Vec<Entry>,
    ) -> Self
    where
        P: Into<Cow<'a, Utf8Path>>,
    {
        let (heading_size, list_size) = compute_sizes(screen);
        App {
            heading_size,
            list_size,
            path: path.map(|p| p.into().into_owned()),
            entries: to_list_entries(files),
            selected: 0,
            offset: 0,
        }
    }

    /// The result of `get_files` is expected to be sorted by size, largest first.
    pub fn handle_event<E, G>(
        &mut self,
        get_entries: G,
        event: Event,
    ) -> Result<Action, E>
    where
        G: FnOnce(Option<&Utf8Path>) -> Result<Vec<Entry>, E>,
    {
        log::debug!("received {:?}", event);
        use Event::*;
        use crossterm::event::KeyCode::*;
        match event {
            Resize(w, h) => Ok(self.handle_resize(Size::new(w, h))),
            KeyPress(Char('q')) => Ok(Action::Quit),
            KeyPress(Right) => self.handle_right(get_entries),
            KeyPress(Char(';')) => self.handle_right(get_entries),
            KeyPress(Left) => self.handle_left(get_entries),
            KeyPress(Char('h')) => self.handle_left(get_entries),
            KeyPress(Up) => { self.move_selection(-1); Ok(Action::Render) },
            KeyPress(Char('k')) => { self.move_selection(-1); Ok(Action::Render) }
            KeyPress(Down) => { self.move_selection(1); Ok(Action::Render) }
            KeyPress(Char('j')) => { self.move_selection(1); Ok(Action::Render) }
            _ => Ok(Action::Nothing)
        }
    }

    fn handle_resize(&mut self, new_size: Size) -> Action {
        (self.heading_size, self.list_size) = compute_sizes(new_size);
        self.fix_offset();
        Action::Render
    }

    fn handle_left<E, G>(
        &mut self,
        get_entries: G,
    ) -> Result<Action, E>
    where
        G: FnOnce(Option<&Utf8Path>) -> Result<Vec<Entry>, E>,
    {
        path_pop(&mut self.path);
        self.set_entries(get_entries(self.path.as_deref())?);
        log::debug!("path is now {:?}", self.path.as_deref());
        Ok(Action::Render)
    }

    fn handle_right<E, G>(
        &mut self,
        get_entries: G,
    ) -> Result<Action, E>
        where
            G: FnOnce(Option<&Utf8Path>) -> Result<Vec<Entry>, E>,
    {
        if !self.entries.is_empty() && self.entries[self.selected].is_dir {
            let name = &self.entries[self.selected].name;
            path_push(&mut self.path, name);
            let files = get_entries(self.path.as_deref())?;
            self.set_entries(files);
            Ok(Action::Render)
        } else {
            Ok(Action::Nothing)
        }
    }

    /// `entries` is expected to be sorted by size, largest first.
    pub fn set_entries(&mut self, entries: Vec<Entry>) {
        self.entries = to_list_entries(entries);
        self.selected = cmp::max(
            0,
            cmp::min(
                self.entries.len() as isize - 1,
                self.selected as isize
            ),
        ) as usize;
        self.offset = 0;
    }

    fn move_selection(&mut self, delta: isize) {
        if self.entries.is_empty() { return }

        let selected = self.selected as isize;
        let len = self.entries.len() as isize;
        self.selected = (selected + delta).rem_euclid(len) as usize;
        self.fix_offset();
    }

    /// Adjust offset to make sure the selected item is visible.
    fn fix_offset(&mut self) {
        let offset = self.offset as isize;
        let selected = self.selected as isize;
        let h = self.list_size.height as isize;
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

impl WidgetRef for App {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let (heading_rect, list_rect) = compute_layout(area);
        { // Heading
            let mut string = "--- ".to_string();
            string.push_str(
                shorten_to(
                    match &self.path {
                        None => "#",
                        Some(path) => path.as_str(),
                    },
                    heading_rect.width as usize - string.len()
                ).as_ref()
            );
            let mut remaining_width = max(
                0,
                heading_rect.width as isize - 4 - string.len() as isize
            ) as usize;
            if remaining_width > 0 {
                string.push(' ');
                remaining_width -= 1;
            }
            string.push_str(&"-".repeat(remaining_width));
            Paragraph::new(string).render_ref(heading_rect, buf);
        }

        { // List
            let items = self.entries
                .iter()
                .enumerate()
                .skip(self.offset)
                .map(|(index, entry)| { ListItem::new(
                    entry.to_line(
                        self.list_size.width,
                        index == self.selected
                    )
                )});
            List::new(items).render_ref(list_rect, buf)
        }
    }
}

/// `entries` is expected to be sorted by size, largest first.
fn to_list_entries(entries: Vec<Entry>) -> Vec<ListEntry> {
    if entries.is_empty() { return Vec::new() }

    let largest = entries[0].size() as f64;
    entries
        .into_iter()
        .map(|e| match e {
            Entry::File(File{ path, size }) => ListEntry {
                name: path,
                size,
                relative_size: size as f64 / largest,
                is_dir: false,
            },
            Entry::Directory(Directory{ path, size }) => ListEntry {
                name: path,
                size,
                relative_size: size as f64 / largest,
                is_dir: true,
            },
        })
        .collect()
}

fn compute_sizes(area: Size) -> (Size, Size) {
    let (heading, list) = compute_layout((Position::new(0, 0), area).into());
    (heading.as_size(), list.as_size())
}

fn compute_layout(area: Rect) -> (Rect, Rect) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Fill(100),
        ])
        .split(area);
    (layout[0], layout[1])
}

fn path_push(o_path: &mut Option<Utf8PathBuf>, name: &Utf8Path) {
    if let Some(path) = o_path {
        path.push(name);
    } else {
        *o_path = Some(name.to_owned());
    }
}

fn path_pop(o_path: &mut Option<Utf8PathBuf>) {
    if let Some(path) = o_path {
        if path.parent().is_none() {
            *o_path = None;
        } else {
            path.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fileitem_to_line() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 999 * 1024 + 1010,
            relative_size: 0.9,
            is_dir: false,
        };
        assert_eq!(
            f.to_line(80),
            Line::raw(" 999.99 KiB [##############  ] 1234567890123456789012345678901234567890".to_owned())
        );
        assert_eq!(
            f.to_line(2),
            Line::raw(" 999.99 KiB [##############  ] ".to_owned())
        );
    }
}