use std::borrow::Cow;
use std::cmp::{max, min};
use std::collections::HashSet;
use std::iter;

use camino::Utf8PathBuf;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Position, Rect, Size};
use ratatui::prelude::Line;
use ratatui::style::{Style, Stylize};
use ratatui::text::Span;
use ratatui::widgets::{
    Block, BorderType, Clear, List, ListItem, Padding, Paragraph, Widget,
    WidgetRef, Wrap,
};
use unicode_segmentation::UnicodeSegmentation;

use redu::cache::{Entry, PathId};

#[derive(Clone, Debug)]
pub enum Event {
    Resize(Size),
    Left,
    Right,
    Up,
    Down,
    PageUp,
    PageDown,
    Enter,
    Exit,
    Mark,
    Unmark,
    UnmarkAll,
    Quit,
    Generate,
    Entries {
        /// `entries` is expected to be sorted by size, largest first.
        path_id: Option<PathId>,
        entries: Vec<Entry>,
    },
    Marks(Vec<Utf8PathBuf>),
}

#[derive(Debug)]
pub enum Action {
    Nothing,
    Render,
    Quit,
    Generate(Vec<Utf8PathBuf>),
    GetParentEntries(PathId),
    GetEntries(Option<PathId>),
    UpsertMark(Utf8PathBuf),
    DeleteMark(Utf8PathBuf),
    DeleteAllMarks,
}

pub struct App {
    path_id: Option<PathId>,
    path: Utf8PathBuf,
    entries: Vec<Entry>,
    marks: HashSet<Utf8PathBuf>,
    list_size: Size,
    selected: usize,
    offset: usize,
    footer_extra: Vec<Span<'static>>,
    confirm_dialog: Option<ConfirmDialog>,
}

impl App {
    /// `entries` is expected to be sorted by size, largest first.
    pub fn new(
        screen: Size,
        path_id: Option<PathId>,
        path: Utf8PathBuf,
        entries: Vec<Entry>,
        marks: Vec<Utf8PathBuf>,
        footer_extra: Vec<Span<'static>>,
    ) -> Self
    {
        let list_size = compute_list_size(screen);
        App {
            path_id,
            path,
            entries,
            marks: HashSet::from_iter(marks),
            list_size,
            selected: 0,
            offset: 0,
            footer_extra,
            confirm_dialog: None,
        }
    }

    pub fn update(&mut self, event: Event) -> Action {
        log::debug!("received {:?}", event);
        use Event::*;
        match event {
            Resize(new_size) => self.resize(new_size),
            Left =>
                if let Some(ref mut confirm_dialog) = self.confirm_dialog {
                    confirm_dialog.yes_selected = false;
                    Action::Render
                } else {
                    self.left()
                },
            Right =>
                if let Some(ref mut confirm_dialog) = self.confirm_dialog {
                    confirm_dialog.yes_selected = true;
                    Action::Render
                } else {
                    self.right()
                },
            Up => self.move_selection(-1, true),
            Down => self.move_selection(1, true),
            PageUp =>
                self.move_selection(-(self.list_size.height as isize), false),
            PageDown =>
                self.move_selection(self.list_size.height as isize, false),
            Enter =>
                if let Some(confirm_dialog) = self.confirm_dialog.take() {
                    if confirm_dialog.yes_selected {
                        confirm_dialog.action
                    } else {
                        Action::Render
                    }
                } else {
                    Action::Nothing
                },
            Exit =>
                if self.confirm_dialog.take().is_some() {
                    Action::Render
                } else {
                    Action::Nothing
                },
            Mark => self.mark_selection(),
            Unmark => self.unmark_selection(),
            UnmarkAll =>
                if self.confirm_dialog.is_none() {
                    self.confirm_dialog = Some(ConfirmDialog {
                        text: "Are you sure you want to delete all marks?"
                            .into(),
                        yes: "Yes".into(),
                        no: "No".into(),
                        yes_selected: false,
                        action: Action::DeleteAllMarks,
                    });
                    Action::Render
                } else {
                    Action::Nothing
                },
            Quit => Action::Quit,
            Generate => self.generate(),
            Entries { path_id, entries } => self.set_entries(path_id, entries),
            Marks(new_marks) => self.set_marks(new_marks),
        }
    }

    fn resize(&mut self, new_size: Size) -> Action {
        self.list_size = compute_list_size(new_size);
        self.fix_offset();
        Action::Render
    }

    fn left(&mut self) -> Action {
        if let Some(path_id) = self.path_id {
            Action::GetParentEntries(path_id)
        } else {
            Action::Nothing
        }
    }

    fn right(&mut self) -> Action {
        if !self.entries.is_empty() {
            let entry = &self.entries[self.selected];
            Action::GetEntries(Some(entry.path_id))
        } else {
            Action::Nothing
        }
    }

    fn move_selection(&mut self, delta: isize, wrap: bool) -> Action {
        if self.entries.is_empty() {
            return Action::Nothing;
        }

        let selected = self.selected as isize;
        let len = self.entries.len() as isize;
        self.selected = if wrap {
            (selected + delta).rem_euclid(len)
        } else {
            max(0, min(len - 1, selected + delta))
        } as usize;
        self.fix_offset();

        Action::Render
    }

    fn mark_selection(&mut self) -> Action {
        self.selected_entry().map(Action::UpsertMark).unwrap_or(Action::Nothing)
    }

    fn unmark_selection(&mut self) -> Action {
        self.selected_entry().map(Action::DeleteMark).unwrap_or(Action::Nothing)
    }

    fn generate(&self) -> Action {
        let mut lines = self
            .marks
            .iter()
            .map(Clone::clone)
            .collect::<Vec<_>>();
        lines.sort_unstable();
        Action::Generate(lines)
    }

    fn set_entries(
        &mut self,
        path_id: Option<PathId>,
        entries: Vec<Entry>,
    ) -> Action {
        // See if any of the new entries matches the current directory
        // and pre-select it. This means that we went up to the parent dir.
        self.selected = entries
            .iter()
            .enumerate()
            .find(|(_, e)| Some(e.path_id) == self.path_id)
            .map(|(i, _)| i)
            .unwrap_or(0);
        self.offset = 0;
        self.path_id = path_id;
        {
            // Check if the new path_id matches any of the old entries.
            // If we find one this means that we are going down into that entry.
            if let Some(e) = self.entries
                .iter()
                .find(|e| Some(e.path_id) == path_id)
            {
                self.path.push(&e.component);
            } else {
                self.path.pop();
            }
        }
        self.entries = entries;
        self.fix_offset();
        Action::Render
    }

    fn set_marks(&mut self, new_marks: Vec<Utf8PathBuf>) -> Action {
        self.marks = HashSet::from_iter(new_marks);
        Action::Render
    }

    /// Adjust offset to make sure the selected item is visible.
    fn fix_offset(&mut self) {
        let offset = self.offset as isize;
        let selected = self.selected as isize;
        let h = self.list_size.height as isize;
        let first_visible = offset;
        let last_visible = offset + h - 1;
        let new_offset = if selected < first_visible {
            selected
        } else if last_visible < selected {
            selected - h + 1
        } else {
            offset
        };
        self.offset = new_offset as usize;
    }

    fn selected_entry(&self) -> Option<Utf8PathBuf> {
        if self.entries.is_empty() {
            return None;
        }
        Some(self.full_path(&self.entries[self.selected]))
    }


    fn full_path(&self, entry: &Entry) -> Utf8PathBuf {
        let mut full_loc = self.path.clone();
        full_loc.push(&entry.component);
        full_loc
    }
}

/// ConfirmDialog //////////////////////////////////////////////////////////////
struct ConfirmDialog {
    text: String,
    yes: String,
    no: String,
    yes_selected: bool,
    action: Action,
}

impl WidgetRef for ConfirmDialog {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let main_text = Paragraph::new(self.text.clone())
            .centered()
            .wrap(Wrap { trim: false });

        let padding = Padding { left: 2, right: 2, top: 1, bottom: 0 };
        let horiz_padding = padding.left + padding.right;
        let vert_padding = padding.top + padding.bottom;
        let dialog_area = {
            let max_text_width = min(80, area.width - 2 - horiz_padding); // take out the border and padding
            let text_width =
                min(self.text.graphemes(true).count() as u16, max_text_width);
            let text_height = main_text.line_count(max_text_width) as u16;
            let max_width = text_width + 2 + horiz_padding; // text + border + padding
            let max_height = text_height + 2 + vert_padding + 1 + 2 + 1; // text + border + padding + empty line + buttons
            centered(max_width, max_height, area)
        };

        let block = Block::bordered().title("Confirm").padding(padding);

        let (main_text_area, buttons_area) = {
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Fill(100), Constraint::Length(3)])
                .split(block.inner(dialog_area));
            (layout[0], layout[1])
        };
        let (no_button_area, yes_button_area) = {
            let layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Fill(1),
                    Constraint::Min(self.no.graphemes(true).count() as u16),
                    Constraint::Fill(1),
                    Constraint::Min(self.yes.graphemes(true).count() as u16),
                    Constraint::Fill(1),
                ])
                .split(buttons_area);
            (layout[1], layout[3])
        };

        fn render_button(
            label: &str,
            selected: bool,
            area: Rect,
            buf: &mut Buffer,
        ) {
            let mut block = Block::bordered().border_type(BorderType::Plain);
            let mut button = Paragraph::new(label)
                .centered()
                .wrap(Wrap { trim: false });
            if selected {
                block = block.border_type(BorderType::QuadrantInside);
                button = button.black().on_white();
            }
            button.render(block.inner(area), buf);
            block.render(area, buf);
        }

        Clear.render(dialog_area, buf);
        block.render(dialog_area, buf);
        main_text.render(main_text_area, buf);
        render_button(&self.no, !self.yes_selected, no_button_area, buf);
        render_button(&self.yes, self.yes_selected, yes_button_area, buf);
    }
}

/// Render /////////////////////////////////////////////////////////////////////

struct ListEntry<'a> {
    name: &'a str,
    size: usize,
    relative_size: f64,
    is_dir: bool,
    is_marked: bool,
}

impl<'a> ListEntry<'a> {
    fn to_line(&self, width: u16, selected: bool) -> Line {
        let mut spans = Vec::with_capacity(4);

        // Mark
        spans.push(Span::raw(if self.is_marked { "*" } else { " " }));

        // Size
        spans.push(Span::raw(format!(
            " {:>10}",
            humansize::format_size(self.size, humansize::BINARY)
        )));

        // Bar
        spans.push(
            Span::raw({
                const MAX_BAR_WIDTH: usize = 16;
                let bar_frac_width =
                    (self.relative_size * (MAX_BAR_WIDTH * 8) as f64) as usize;
                let full_blocks = bar_frac_width / 8;
                let last_block = match (bar_frac_width % 8) as u32 {
                    0 => String::new(),
                    x => String::from(unsafe {
                        char::from_u32_unchecked(0x2590 - x)
                    }),
                };
                let empty_width = MAX_BAR_WIDTH
                    - full_blocks
                    - last_block.graphemes(true).count();
                let mut bar = String::with_capacity(1 + MAX_BAR_WIDTH + 1);
                bar.push(' ');
                for _ in 0..full_blocks {
                    bar.push('\u{2588}');
                }
                bar.push_str(&last_block);
                for _ in 0..empty_width {
                    bar.push(' ');
                }
                bar.push(' ');
                bar
            })
            .green(),
        );

        // Name
        spans.push({
            let available_width = {
                let used: usize = spans
                    .iter()
                    .map(|s| s.content.graphemes(true).count())
                    .sum();
                max(0, width as isize - used as isize) as usize
            };
            if self.is_dir {
                let mut name = Cow::Borrowed(self.name);
                if !name.ends_with('/') {
                    name.to_mut().push('/');
                }
                let span =
                    Span::raw(shorten_to(&name, available_width).into_owned())
                        .bold();
                if selected {
                    span.dark_gray()
                } else {
                    span.blue()
                }
            } else {
                Span::raw(shorten_to(self.name, available_width))
            }
        });

        Line::from(spans).style(if selected {
            Style::new().black().on_white()
        } else {
            Style::new()
        })
    }
}

impl WidgetRef for App {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let (header_rect, list_rect, footer_rect) = compute_layout(area);
        {
            // Header
            let mut string = "--- ".to_string();
            string.push_str(
                shorten_to(
                    if self.path.as_str().is_empty() {
                        "#"
                    } else {
                        self.path.as_str()
                    },
                    max(0, header_rect.width as isize - string.len() as isize)
                        as usize,
                )
                .as_ref(),
            );
            let mut remaining_width = max(
                0,
                header_rect.width as isize
                    - string.graphemes(true).count() as isize,
            ) as usize;
            if remaining_width > 0 {
                string.push(' ');
                remaining_width -= 1;
            }
            string.push_str(&"-".repeat(remaining_width));
            Paragraph::new(string).on_light_blue().render_ref(header_rect, buf);
        }

        {
            // List
            let list_entries = to_list_entries(
                |e| { self.marks.contains(&self.full_path(e)) },
                self.entries.iter(),
            );
            let items =
                list_entries.iter().enumerate().skip(self.offset).map(
                    |(index, entry)| {
                        ListItem::new(entry.to_line(
                            self.list_size.width,
                            index == self.selected,
                        ))
                    },
                );
            List::new(items).render_ref(list_rect, buf)
        }

        {
            // Footer
            let spans = vec![
                Span::from(format!(" Marks: {}", self.marks.len())),
                Span::from("  |  "),
            ]
            .into_iter()
            .chain(self.footer_extra.clone())
            .collect::<Vec<_>>();
            Paragraph::new(Line::from(spans))
                .on_light_blue()
                .render_ref(footer_rect, buf);
        }

        if let Some(confirm_dialog) = &self.confirm_dialog {
            confirm_dialog.render_ref(area, buf);
        }
    }
}

/// `entries` is expected to be sorted by size, largest first.
fn to_list_entries<'a>(
    mut is_marked: impl FnMut(&'a Entry) -> bool,
    entries: impl IntoIterator<Item = &'a Entry>,
) -> Vec<ListEntry<'a>> {
    let mut entries = entries.into_iter();
    if let Some(first) = entries.next() {
        let largest = first.size as f64;
        iter::once(first)
            .chain(entries)
            .map(|e@Entry { component, size, is_dir, .. }| {
                ListEntry {
                    name: component,
                    size: *size,
                    relative_size: *size as f64 / largest,
                    is_dir: *is_dir,
                    is_marked: is_marked(e),
                }
            })
            .collect()
    }
    else  {
        Vec::new()
    }
}

fn shorten_to(s: &str, width: usize) -> Cow<str> {
    let len = s.graphemes(true).count();
    let res = if len <= width {
        Cow::Borrowed(s)
    } else if width <= 3 {
        Cow::Owned(".".repeat(width))
    } else {
        let front_width = (width - 3).div_euclid(2);
        let back_width = width - front_width - 3;
        let graphemes = s.graphemes(true);
        let mut name = graphemes.clone().take(front_width).collect::<String>();
        name.push_str("...");
        for g in graphemes.skip(len - back_width) {
            name.push_str(g);
        }
        Cow::Owned(name)
    };
    res
}

/// Misc //////////////////////////////////////////////////////////////////////

fn compute_list_size(area: Size) -> Size {
    let (_, list, _) = compute_layout((Position::new(0, 0), area).into());
    list.as_size()
}

fn compute_layout(area: Rect) -> (Rect, Rect, Rect) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Fill(100),
            Constraint::Length(1),
        ])
        .split(area);
    (layout[0], layout[1], layout[2])
}

/// Returns a `Rect` centered in `area` with a maximum width and height.
fn centered(max_width: u16, max_height: u16, area: Rect) -> Rect {
    let width = min(max_width, area.width);
    let height = min(max_height, area.height);
    Rect {
        x: area.width / 2 - width / 2,
        y: area.height / 2 - height / 2,
        width,
        height,
    }
}

/// Tests //////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::{*, shorten_to};

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
                Span::raw(" ██████████████▍  ").green(),
                Span::raw("123...7890")
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
                Span::raw(" ██████████████▍  ").green(),
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
                Span::raw(" ██████████████▍  ").green(),
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
                Span::raw(" ██████████████▍  ").green(),
                Span::raw("1234567890123456789012345678901234567890/")
                    .bold()
                    .blue()
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
                Span::raw(" ██████████████▍  ").green(),
                Span::raw("1234567890123456789012345678901234567890")
            ])
            .black()
            .on_white()
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
                Span::raw(" ██████████████▍  ").green(),
                Span::raw("1234567890123456789012345678901234567890/")
                    .bold()
                    .dark_gray()
            ])
            .black()
            .on_white()
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
                Span::raw(" ██████████████▍  ").green(),
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
                Span::raw(" ██████████████▍  ").green(),
                Span::raw("1234567890123456789012345678901234567890")
            ])
            .black()
            .on_white()
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
