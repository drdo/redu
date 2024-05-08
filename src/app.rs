use std::borrow::Cow;
use std::iter;
use std::cmp::{max, min};

use camino::{Utf8Path, Utf8PathBuf};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Position, Rect, Size};
use ratatui::prelude::Line;
use ratatui::style::{Style, Stylize};
use ratatui::text::Span;
use ratatui::widgets::{List, ListItem, Paragraph, WidgetRef};
use unicode_segmentation::UnicodeSegmentation;
use crossterm::event::KeyCode;

use crate::types::{Directory, Entry, File};


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
                Span::raw(shorten_to(self.name.as_str(), available_width))
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
    path: Option<Utf8PathBuf>,
    entries: Vec<Entry>,
    heading_size: Size,
    list_size: Size,
    selected: usize,
    offset: usize,
}

impl App {
    /// `files` is expected to be sorted by size, largest first.
    pub fn new<'a, P>(
        screen: Size,
        path: Option<P>,
        entries: Vec<Entry>,
    ) -> Self
    where
        P: Into<Cow<'a, Utf8Path>>,
    {
        let (heading_size, list_size) = compute_sizes(screen);
        App {
            heading_size,
            list_size,
            path: path.map(|p| p.into().into_owned()),
            entries,
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
        if !self.entries.is_empty() {
            match &self.entries[self.selected] {
                Entry::Directory(Directory{ path, .. }) => {
                    path_push(&mut self.path, &path);
                    let files = get_entries(self.path.as_deref())?;
                    self.set_entries(files);
                    Ok(Action::Render)
                }
                _ => Ok(Action::Nothing),
            }
        } else {
            Ok(Action::Nothing)
        }
    }

    /// `entries` is expected to be sorted by size, largest first.
    pub fn set_entries(&mut self, entries: Vec<Entry>) {
        self.entries = entries;
        self.selected = max(
            0,
            min(
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
            let list_entries = to_list_entries(self.entries.iter());
            let items = list_entries
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
fn to_list_entries<'a, I>(entries: I) -> Vec<ListEntry>
where
    I: IntoIterator<Item=&'a Entry>
{
    let mut entries = entries.into_iter();
    match entries.next() {
        None => Vec::new(),
        Some(first) => {
            let largest = first.size() as f64;
            iter::once(first).chain(entries)
                .map(|e| match e {
                    Entry::File(File{ path, size }) => ListEntry {
                        name: path.clone(),
                        size: *size,
                        relative_size: *size as f64 / largest,
                        is_dir: false,
                    },
                    Entry::Directory(Directory{ path, size }) => ListEntry {
                        name: path.clone(),
                        size: *size,
                        relative_size: *size as f64 / largest,
                        is_dir: true,
                    },
                })
                .collect()
        }
    }
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

fn shorten_to(s: &str, width: usize) -> Cow<str> {
    let len = s.graphemes(true).count();
    let res = if len <= width {
        Cow::Borrowed(s)
    }
    else if width <= 3 {
        Cow::Owned(".".repeat(width))
    } else {
        let front_width = (width - 3).div_euclid(2);
        let back_width = width - front_width - 3;
        let graphemes = s.graphemes(true);
        let mut name = graphemes.clone().take(front_width).collect::<String>();
        name.push_str("...");
        for g in graphemes.skip(len-back_width) { name.push_str(g); }
        Cow::Owned(name)
    };
    res
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use super::shorten_to;

    use super::*;

    #[test]
    fn list_entry_to_line_large_size_file() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 999 * 1024 + 1010,
            relative_size: 0.9,
            is_dir: false,
        };
        assert_eq!(
            f.to_line(80, false),
            Line::from(vec![
                Span::raw(" 999.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890")
            ])
        );
        assert_eq!(
            f.to_line(2, false),
            Line::from(vec![
                Span::raw(" 999.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("")
            ])
        );
    }

    #[test]
    fn list_entry_to_line_small_size_file() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 9 * 1024,
            relative_size: 0.9,
            is_dir: false,
        };
        assert_eq!(
            f.to_line(80, false),
            Line::from(vec![
                Span::raw("      9 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890")
            ])
        );
        assert_eq!(
            f.to_line(2, false),
            Line::from(vec![
                Span::raw("      9 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("")
            ])
        );
    }

    #[test]
    fn list_entry_to_line_directory() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 9 * 1024 + 1010,
            relative_size: 0.9,
            is_dir: true,
        };
        assert_eq!(
            f.to_line(80, false),
            Line::from(vec![
                Span::raw("   9.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890/")
                    .bold().blue()
            ])
        );
        assert_eq!(
            f.to_line(2, false),
            Line::from(vec![
                Span::raw("   9.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("")
                    .bold().blue()
            ])
        );
    }

    #[test]
    fn list_entry_to_line_file_selected() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 999 * 1024 + 1010,
            relative_size: 0.9,
            is_dir: false,
        };
        assert_eq!(
            f.to_line(80, true),
            Line::from(vec![
                Span::raw(" 999.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890")
            ]).black().on_white()
        );
        assert_eq!(
            f.to_line(2, true),
            Line::from(vec![
                Span::raw(" 999.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("")
            ]).black().on_white()
        );
    }

    #[test]
    fn list_entry_to_line_directory_selected() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 9 * 1024 + 1010,
            relative_size: 0.9,
            is_dir: true,
        };
        assert_eq!(
            f.to_line(80, true),
            Line::from(vec![
                Span::raw("   9.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890/")
                    .bold().dark_gray()
            ]).black().on_white()
        );
        assert_eq!(
            f.to_line(2, true),
            Line::from(vec![
                Span::raw("   9.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("")
                    .bold().dark_gray()
            ]).black().on_white()
        );
    }

    #[test]
    fn shorten_to_test() {
        let s = "123456789";
        assert_eq!(shorten_to(s, 0), Cow::Owned::<str>("".to_owned()));
        assert_eq!(shorten_to(s, 1), Cow::Owned::<str>(".".to_owned()));
        assert_eq!(shorten_to(s, 2), Cow::Owned::<str>("..".to_owned()));
        assert_eq!(shorten_to(s, 3), Cow::Owned::<str>("...".to_owned()));
        assert_eq!(shorten_to(s, 4), Cow::Owned::<str>("...9".to_owned()));
        assert_eq!(shorten_to(s, 5), Cow::Owned::<str>("1...9".to_owned()));
        assert_eq!(shorten_to(s, 8), Cow::Owned::<str>("12...789".to_owned()));
        assert_eq!(shorten_to(s, 9), Cow::Borrowed(s));
    }
}
