use std::borrow::Cow;
use std::iter;
use std::cmp::{max, min};
use std::collections::HashSet;

use camino::{Utf8Path, Utf8PathBuf};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Position, Rect, Size};
use ratatui::prelude::Line;
use ratatui::style::{Style, Stylize};
use ratatui::text::Span;
use ratatui::widgets::{List, ListItem, Paragraph, WidgetRef};
use unicode_segmentation::UnicodeSegmentation;

use dorestic::types::{Directory, Entry, File};

#[derive(Debug)]
pub enum Event {
    Resize(Size),
    Left,
    Right,
    Up,
    Down,
    Mark,
    Unmark,
    UnmarkAll,
    Quit,
    Generate,
    Entries { /// `children` is expected to be sorted by size, largest first.
        parent: Option<Utf8PathBuf>,
        children: Vec<Entry>,
    },
    Marks(Vec<Utf8PathBuf>),
}

#[derive(Debug)]
pub enum Action {
    Nothing,
    Render,
    Quit,
    Generate(Vec<Box<str>>),
    GetEntries(Option<Utf8PathBuf>),
    UpsertMark(Utf8PathBuf),
    DeleteMark(Utf8PathBuf),
    DeleteAllMarks,
}

pub struct App {
    path: Option<Utf8PathBuf>,
    entries: Vec<Entry>,
    marks: HashSet<Utf8PathBuf>,
    heading_size: Size,
    list_size: Size,
    selected: usize,
    offset: usize,
}

impl App {
    /// `entries` is expected to be sorted by size, largest first.
    pub fn new<'a, P>(
        screen: Size,
        path: Option<P>,
        entries: Vec<Entry>,
        marks: Vec<Utf8PathBuf>,
    ) -> Self
    where
        P: Into<Cow<'a, Utf8Path>>,
    {
        let (heading_size, list_size) = compute_sizes(screen);
        App {
            path: path.map(|p| p.into().into_owned()),
            entries,
            marks: HashSet::from_iter(marks.into_iter()),
            heading_size,
            list_size,
            selected: 0,
            offset: 0,
        }
    }

    pub fn update(&mut self, event: Event) -> Action
    {
        log::debug!("received {:?}", event);
        use Event::*;
        match event {
            Resize(new_size) => self.resize(new_size),
            Left => self.left(),
            Right => self.right(),
            Up => self.move_selection(-1),
            Down => self.move_selection(1),
            Mark => self.mark_selection(),
            Unmark => self.unmark_selection(),
            UnmarkAll => self.unmark_all(),
            Quit => Action::Quit,
            Generate => self.generate(),
            Entries { parent, children } => self.set_entries(parent, children),
            Marks(new_marks) => self.set_marks(new_marks),
        }
    }

    fn resize(&mut self, new_size: Size) -> Action {
        (self.heading_size, self.list_size) = compute_sizes(new_size);
        self.fix_offset();
        Action::Render
    }

    fn left(&mut self) -> Action {
        match &self.path {
            None =>
                Action::Nothing,
            Some(path) =>
                Action::GetEntries(path.parent().map(Utf8Path::to_path_buf)),
        }
    }

    fn right(&mut self) -> Action {
        if !self.entries.is_empty() {
            match &self.entries[self.selected] {
                Entry::Directory(Directory{ path, .. }) => {
                    let new_path = path_extended(self.path.as_deref(), &path);
                    Action::GetEntries(Some(new_path.into_owned()))
                }
                _ => Action::Nothing,
            }
        } else {
            Action::Nothing
        }
    }

    fn move_selection(&mut self, delta: isize) -> Action {
        if self.entries.is_empty() { return Action::Nothing }

        let selected = self.selected as isize;
        let len = self.entries.len() as isize;
        self.selected = (selected + delta).rem_euclid(len) as usize;
        self.fix_offset();

        Action::Render
    }

    fn mark_selection(&mut self) -> Action {
        self.selected_entry()
            .map(Action::UpsertMark)
            .unwrap_or(Action::Nothing)
    }

    fn unmark_selection(&mut self) -> Action {
        self.selected_entry()
            .map(Action::DeleteMark)
            .unwrap_or(Action::Nothing)
    }

    fn unmark_all(&self) -> Action {
        Action::DeleteAllMarks
    }

    fn generate(&self) -> Action {
        let mut lines = self.marks
            .iter()
            .map(|p| Box::from(p.as_str()))
            .collect::<Vec<_>>();
        lines.sort_unstable();
        Action::Generate(lines)
    }

    fn set_entries(
        &mut self,
        parent: Option<Utf8PathBuf>,
        entries: Vec<Entry>
    ) -> Action
    {
        self.selected =
            entries
                .iter()
                .map(|e| path_extended(parent.as_deref(), e.path()))
                .enumerate()
                .find(|(_, path)| Some(path.as_ref()) == self.path.as_deref())
                .map(|(i, _)| i)
                .unwrap_or(0);
        self.offset = 0;
        self.path = parent;
        self.entries = entries;
        Action::Render
    }

    fn set_marks(&mut self, new_marks: Vec<Utf8PathBuf>) -> Action {
        self.marks = HashSet::from_iter(new_marks.into_iter());
        Action::Render
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

    fn selected_entry(&self) -> Option<Utf8PathBuf> {
        if self.entries.is_empty() { return None }

        let full_path = path_extended(
            self.path.as_deref(),
            self.entries[self.selected].path()
        ).into_owned();
        Some(full_path)
    }
}

fn path_extended<'a>(
    o_path: Option<&Utf8Path>,
    more: &'a Utf8Path
) -> Cow<'a, Utf8Path>
{
    match o_path {
        None => Cow::Borrowed(more),
        Some(path) => {
            let mut full_path = path.to_path_buf();
            full_path.push(more);
            Cow::Owned(full_path)
        }
    }
}

/// Render /////////////////////////////////////////////////////////////////////

struct ListEntry {
    name: Utf8PathBuf,
    size: usize,
    relative_size: f64,
    is_dir: bool,
    is_marked: bool,
}

impl ListEntry {
    fn to_line(&self, width: u16, selected: bool) -> Line {
        let mut spans = Vec::with_capacity(4);

        // Mark
        spans.push(Span::raw(
            if self.is_marked { "*" }
            else { " " }
        ));

        // Size
        spans.push(Span::raw(
            format!(" {:>10}", humansize::format_size(self.size, humansize::BINARY))
        ));

        // Bar
        spans.push(Span::raw({
            let max_bar_width: usize = max(16, min(24, (0.1 * width as f64) as usize));
            let bar_width = (self.relative_size * max_bar_width as f64) as usize;
            let bar = format!(
                " [{:#^bar_size$}{:empty_bar_size$}] ",
                "", "",
                bar_size = bar_width,
                empty_bar_size = max_bar_width - bar_width
            );
            bar
        }));

        // Name
        spans.push({
            let available_width = {
                let used: usize = spans.iter().map(|s| s.content.len()).sum();
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
        });

        Line::from(spans).style(
            if selected { Style::new().black().on_white() }
            else { Style::new() }
        )
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
            let list_entries = to_list_entries(
                |p| self.marks.contains(
                    path_extended(self.path.as_deref(), p).as_ref()
                ),
                self.entries.iter(),
            );
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
fn to_list_entries<'a>(
    mut is_marked: impl FnMut(&Utf8Path) -> bool,
    entries: impl IntoIterator<Item=&'a Entry>,
) -> Vec<ListEntry>
{
    let mut entries = entries.into_iter();
    match entries.next() {
        None => Vec::new(),
        Some(first) => {
            let largest = first.size() as f64;
            iter::once(first).chain(entries)
                .map(|e| {
                    let (path, size, is_dir) = match e {
                        Entry::File(File{ path, size }) =>
                            (path, size, false),
                        Entry::Directory(Directory{ path, size }) =>
                            (path, size, true),
                    };
                    ListEntry {
                        name: path.clone(),
                        size: *size,
                        relative_size: *size as f64 / largest,
                        is_dir,
                        is_marked: is_marked(path),
                    }
                })
                .collect()
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

/// Misc //////////////////////////////////////////////////////////////////////

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

/// Tests //////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use super::shorten_to;

    use super::*;

    #[test]
    fn list_entry_to_line_narrow_width() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 999 * 1024 + 1010,
            relative_size: 0.9,
            is_dir: false,
            is_marked: false,
        };
        assert_eq!(
            f.to_line(40, false),
            Line::from(vec![
                Span::raw(" "),
                Span::raw(" 999.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("12...890")
            ])
        );
    }

    #[test]
    fn list_entry_to_line_large_size_file() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 999 * 1024 + 1010,
            relative_size: 0.9,
            is_dir: false,
            is_marked: false,
        };
        assert_eq!(
            f.to_line(80, false),
            Line::from(vec![
                Span::raw(" "),
                Span::raw(" 999.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890")
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
            is_marked: false,
        };
        assert_eq!(
            f.to_line(80, false),
            Line::from(vec![
                Span::raw(" "),
                Span::raw("      9 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890")
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
            is_marked: false,
        };
        assert_eq!(
            f.to_line(80, false),
            Line::from(vec![
                Span::raw(" "),
                Span::raw("   9.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890/")
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
            is_marked: false,
        };
        assert_eq!(
            f.to_line(80, true),
            Line::from(vec![
                Span::raw(" "),
                Span::raw(" 999.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890")
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
            is_marked: false,
        };
        assert_eq!(
            f.to_line(80, true),
            Line::from(vec![
                Span::raw(" "),
                Span::raw("   9.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890/")
                    .bold().dark_gray()
            ]).black().on_white()
        );
    }

    #[test]
    fn list_entry_to_line_file_marked() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 999 * 1024 + 1010,
            relative_size: 0.9,
            is_dir: false,
            is_marked: true,
        };
        assert_eq!(
            f.to_line(80, false),
            Line::from(vec![
                Span::raw("*"),
                Span::raw(" 999.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890")
            ])
        );
    }

    #[test]
    fn list_entry_to_line_file_marked_selected() {
        let f = ListEntry {
            name: "1234567890123456789012345678901234567890".into(),
            size: 999 * 1024 + 1010,
            relative_size: 0.9,
            is_dir: false,
            is_marked: true,
        };
        assert_eq!(
            f.to_line(80, true),
            Line::from(vec![
                Span::raw("*"),
                Span::raw(" 999.99 KiB"),
                Span::raw(" [##############  ] "),
                Span::raw("1234567890123456789012345678901234567890")
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
