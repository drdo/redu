use std::{
    borrow::Cow,
    cmp::{max, min},
    collections::HashSet,
    iter,
};

use camino::Utf8PathBuf;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Position, Rect, Size},
    prelude::Line,
    style::{Style, Stylize},
    text::Span,
    widgets::{
        Block, BorderType, Clear, Padding, Paragraph, Row, Table, Widget,
        WidgetRef, Wrap,
    },
};
use redu::cache::EntryDetails;
use unicode_segmentation::UnicodeSegmentation;

use crate::{
    cache::{Entry, PathId},
    util::snapshot_short_id,
};

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
    EntryDetails(EntryDetails),
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
    GetEntryDetails(PathId),
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
    details_drawer: Option<DetailsDrawer>,
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
    ) -> Self {
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
            details_drawer: None,
            confirm_dialog: None,
        }
    }

    pub fn update(&mut self, event: Event) -> Action {
        log::debug!("received {:?}", event);
        use Event::*;
        match event {
            Resize(new_size) => self.resize(new_size),
            Left => {
                if let Some(ref mut confirm_dialog) = self.confirm_dialog {
                    confirm_dialog.yes_selected = false;
                    Action::Render
                } else {
                    self.left()
                }
            }
            Right => {
                if let Some(ref mut confirm_dialog) = self.confirm_dialog {
                    confirm_dialog.yes_selected = true;
                    Action::Render
                } else {
                    self.right()
                }
            }
            Up => self.move_selection(-1, true),
            Down => self.move_selection(1, true),
            PageUp => {
                self.move_selection(-(self.list_size.height as isize), false)
            }
            PageDown => {
                self.move_selection(self.list_size.height as isize, false)
            }
            Enter => {
                if let Some(confirm_dialog) = self.confirm_dialog.take() {
                    if confirm_dialog.yes_selected {
                        confirm_dialog.action
                    } else {
                        Action::Render
                    }
                } else if self.confirm_dialog.is_none() {
                    Action::GetEntryDetails(self.entries[self.selected].path_id)
                } else {
                    Action::Nothing
                }
            }
            Exit => {
                if self.confirm_dialog.take().is_some()
                    || self.details_drawer.take().is_some()
                {
                    Action::Render
                } else {
                    Action::Nothing
                }
            }
            Mark => self.mark_selection(),
            Unmark => self.unmark_selection(),
            UnmarkAll => {
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
                }
            }
            Quit => Action::Quit,
            Generate => self.generate(),
            Entries { path_id, entries } => self.set_entries(path_id, entries),
            EntryDetails(details) => {
                self.details_drawer = Some(DetailsDrawer { details });
                Action::Render
            }
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
            if entry.is_dir {
                return Action::GetEntries(Some(entry.path_id));
            }
        }
        Action::Nothing
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

        if self.details_drawer.is_some() {
            Action::GetEntryDetails(self.entries[self.selected].path_id)
        } else {
            Action::Render
        }
    }

    fn mark_selection(&mut self) -> Action {
        self.selected_entry().map(Action::UpsertMark).unwrap_or(Action::Nothing)
    }

    fn unmark_selection(&mut self) -> Action {
        self.selected_entry().map(Action::DeleteMark).unwrap_or(Action::Nothing)
    }

    fn generate(&self) -> Action {
        let mut lines = self.marks.iter().map(Clone::clone).collect::<Vec<_>>();
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
            if let Some(e) =
                self.entries.iter().find(|e| Some(e.path_id) == path_id)
            {
                self.path.push(&e.component);
            } else {
                self.path.pop();
            }
        }
        self.entries = entries;
        self.fix_offset();

        if self.details_drawer.is_some() {
            Action::GetEntryDetails(self.entries[self.selected].path_id)
        } else {
            Action::Render
        }
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

impl WidgetRef for App {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let (header_area, table_area, footer_area) = compute_layout(area);
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
                    max(0, header_area.width as isize - string.len() as isize)
                        as usize,
                )
                .as_ref(),
            );
            let mut remaining_width = max(
                0,
                header_area.width as isize
                    - string.graphemes(true).count() as isize,
            ) as usize;
            if remaining_width > 0 {
                string.push(' ');
                remaining_width -= 1;
            }
            string.push_str(&"-".repeat(remaining_width));
            Paragraph::new(string).on_light_blue().render_ref(header_area, buf);
        }

        {
            // Table
            const MIN_WIDTH_SHOW_SIZEBAR: u16 = 50;
            let show_sizebar = table_area.width >= MIN_WIDTH_SHOW_SIZEBAR;
            let mut rows: Vec<Row> = Vec::with_capacity(self.entries.len());
            let mut entries = self.entries.iter();
            if let Some(first) = entries.next() {
                let largest_size = first.size as f64;
                for (index, entry) in iter::once(first)
                    .chain(entries)
                    .enumerate()
                    .skip(self.offset)
                {
                    let selected = index == self.selected;
                    let mut spans = Vec::with_capacity(4);
                    spans.push(render_mark(
                        self.marks.contains(&self.full_path(entry)),
                    ));
                    spans.push(render_size(entry.size));
                    if show_sizebar {
                        spans.push(render_sizebar(
                            entry.size as f64 / largest_size,
                        ));
                    }
                    let used_width: usize = spans
                        .iter()
                        .map(|s| grapheme_len(&s.content))
                        .sum::<usize>()
                        + spans.len(); // separators
                    let available_width =
                        max(0, table_area.width as isize - used_width as isize)
                            as usize;
                    spans.push(render_name(
                        &entry.component,
                        entry.is_dir,
                        selected,
                        available_width,
                    ));
                    rows.push(Row::new(spans).style(if selected {
                        Style::new().black().on_white()
                    } else {
                        Style::new()
                    }));
                }
            }
            let mut constraints = Vec::with_capacity(4);
            constraints.push(Constraint::Min(MARK_LEN));
            constraints.push(Constraint::Min(SIZE_LEN));
            if show_sizebar {
                constraints.push(Constraint::Min(SIZEBAR_LEN));
            }
            constraints.push(Constraint::Percentage(100));
            Table::new(rows, constraints).render_ref(table_area, buf)
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
                .render_ref(footer_area, buf);
        }

        if let Some(details_dialog) = &self.details_drawer {
            details_dialog.render_ref(table_area, buf);
        }

        if let Some(confirm_dialog) = &self.confirm_dialog {
            confirm_dialog.render_ref(area, buf);
        }
    }
}

const MARK_LEN: u16 = 1;

fn render_mark(is_marked: bool) -> Span<'static> {
    Span::raw(if is_marked { "*" } else { " " })
}

const SIZE_LEN: u16 = 11;

fn render_size(size: usize) -> Span<'static> {
    Span::raw(format!(
        "{:>11}",
        humansize::format_size(size, humansize::BINARY)
    ))
}

const SIZEBAR_LEN: u16 = 16;

fn render_sizebar(relative_size: f64) -> Span<'static> {
    Span::raw({
        let bar_frac_width =
            (relative_size * (SIZEBAR_LEN * 8) as f64) as usize;
        let full_blocks = bar_frac_width / 8;
        let last_block = match (bar_frac_width % 8) as u32 {
            0 => String::new(),
            x => String::from(unsafe { char::from_u32_unchecked(0x2590 - x) }),
        };
        let empty_width =
            SIZEBAR_LEN as usize - full_blocks - grapheme_len(&last_block);
        let mut bar = String::with_capacity(1 + SIZEBAR_LEN as usize + 1);
        for _ in 0..full_blocks {
            bar.push('\u{2588}');
        }
        bar.push_str(&last_block);
        for _ in 0..empty_width {
            bar.push(' ');
        }
        bar
    })
    .green()
}

fn render_name(
    name: &str,
    is_dir: bool,
    selected: bool,
    available_width: usize,
) -> Span {
    let mut escaped = escape_name(name);
    if is_dir {
        if !escaped.ends_with('/') {
            escaped.to_mut().push('/');
        }
        let span =
            Span::raw(shorten_to(&escaped, available_width).into_owned())
                .bold();
        if selected {
            span.dark_gray()
        } else {
            span.blue()
        }
    } else {
        Span::raw(shorten_to(&escaped, available_width).into_owned())
    }
}

fn escape_name(name: &str) -> Cow<str> {
    match name.find(char::is_control) {
        None => Cow::Borrowed(name),
        Some(index) => {
            let (left, right) = name.split_at(index);
            let mut escaped = String::with_capacity(name.len() + 1); // the +1 is for the extra \
            escaped.push_str(left);
            for c in right.chars() {
                if c.is_control() {
                    for d in c.escape_default() {
                        escaped.push(d);
                    }
                } else {
                    escaped.push(c)
                }
            }
            Cow::Owned(escaped)
        }
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

/// DetailsDialog //////////////////////////////////////////////////////////////
struct DetailsDrawer {
    details: EntryDetails,
}

impl WidgetRef for DetailsDrawer {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let details = &self.details;
        let text = format!(
            "max size: {} ({})\n\
             first seen: {} ({})\n\
             last seen: {} ({})\n",
            humansize::format_size(details.max_size, humansize::BINARY),
            snapshot_short_id(&details.max_size_snapshot_hash),
            details.first_seen.date_naive(),
            snapshot_short_id(&details.first_seen_snapshot_hash),
            details.last_seen.date_naive(),
            snapshot_short_id(&details.last_seen_snapshot_hash),
        );
        let paragraph = Paragraph::new(text).wrap(Wrap { trim: false });
        let padding = Padding { left: 2, right: 2, top: 0, bottom: 0 };
        let horiz_padding = padding.left + padding.right;
        let inner_width = {
            let desired_inner_width = paragraph.line_width() as u16;
            let max_inner_width = area.width.saturating_sub(2 + horiz_padding);
            min(max_inner_width, desired_inner_width)
        };
        let outer_width = inner_width + 2 + horiz_padding;
        let outer_height = {
            let vert_padding = padding.top + padding.bottom;
            let inner_height = paragraph.line_count(inner_width) as u16;
            inner_height + 2 + vert_padding
        };
        let block_area = Rect {
            x: area.x + area.width - outer_width,
            y: area.y + area.height - outer_height,
            width: outer_width,
            height: outer_height,
        };
        let block = Block::bordered().title("Details").padding(padding);
        let paragraph_area = block.inner(block_area);
        Clear.render(block_area, buf);
        block.render(block_area, buf);
        paragraph.render(paragraph_area, buf);
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
        let width = min(80, grapheme_len(&self.text) as u16);
        let height = main_text.line_count(width) as u16 + 1 + 3; // text + empty line + buttons
        let dialog_area = dialog(padding, width, height, area);

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
            let mut button =
                Paragraph::new(label).centered().wrap(Wrap { trim: false });
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

/// Misc //////////////////////////////////////////////////////////////////////
fn dialog(
    padding: Padding,
    max_inner_width: u16,
    max_inner_height: u16,
    area: Rect,
) -> Rect {
    let horiz_padding = padding.left + padding.right;
    let vert_padding = padding.top + padding.bottom;
    let max_width = max_inner_width + 2 + horiz_padding; // The extra 2 is the border
    let max_height = max_inner_height + 2 + vert_padding;
    centered(max_width, max_height, area)
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

fn grapheme_len(s: &str) -> usize {
    s.graphemes(true).count()
}

/// Tests //////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
    use super::{shorten_to, *};

    #[test]
    fn render_sizebar_test() {
        fn aux(size: f64, content: &str) {
            assert_eq!(render_sizebar(size).content, content);
        }

        aux(0.00, "                ");
        aux(0.25, "████            ");
        aux(0.50, "████████        ");
        aux(0.75, "████████████    ");
        aux(0.90, "██████████████▍ ");
        aux(1.00, "████████████████");
        aux(0.5 + (1.0 / (8.0 * 16.0)), "████████▏       ");
        aux(0.5 + (2.0 / (8.0 * 16.0)), "████████▎       ");
        aux(0.5 + (3.0 / (8.0 * 16.0)), "████████▍       ");
        aux(0.5 + (4.0 / (8.0 * 16.0)), "████████▌       ");
        aux(0.5 + (5.0 / (8.0 * 16.0)), "████████▋       ");
        aux(0.5 + (6.0 / (8.0 * 16.0)), "████████▊       ");
        aux(0.5 + (7.0 / (8.0 * 16.0)), "████████▉       ");
    }

    #[test]
    fn escape_name_test() {
        assert_eq!(
            escape_name("f\no\\tóà 学校\r"),
            Cow::Borrowed("f\\no\\tóà 学校\\r")
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
