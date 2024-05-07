use std::borrow::Cow;
use std::cmp::{max, min};
use std::fmt::{Display, Formatter};
use std::rc::Rc;

use camino::{Utf8Path, Utf8PathBuf};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Line;
use ratatui::widgets::WidgetRef;
use crate::component;

use crate::component::{Action, Event, ToLine};
use crate::component::heading::Heading;
use crate::component::list::List;
use crate::types::{Directory, Entry, File};

struct PathItem(Option<Utf8PathBuf>);

impl Display for PathItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            None => f.write_fmt(format_args!("#")),
            Some(path) => f.write_fmt(format_args!("{path}")),
        }
    }
}

struct ListItem {
    name: Utf8PathBuf,
    size: usize,
    relative_size: f64,
    is_dir: bool,
}

impl ToLine for ListItem {
    fn to_line(&self, width: u16) -> Line {
        let mut text =
        // Size
            format!(" {:>10}", humansize::format_size(self.size, humansize::BINARY));
        { // Bar
            let max_bar_width: usize = max(16, min(24, (0.1 * width as f64) as usize));
            let bar_width = (self.relative_size * max_bar_width as f64) as usize;
            let bar = format!(
                " [{:#^bar_size$}{:empty_bar_size$}] ",
                "", "",
                bar_size = bar_width,
                empty_bar_size = max_bar_width - bar_width
            );
            text.push_str(&bar);
        }
        // Name
        {
            let available_width = max(0, width as isize - text.len() as isize) as usize;
            text.push_str(component::shorten_to(self.name.as_str(), available_width).as_ref());
        }
        Line::raw(text)
    }
}

pub struct App {
    heading: Heading<PathItem>,
    files: List<ListItem>,
}

impl App {
    /// `files` is expected to be sorted by size, largest first.
    pub fn new<'a, P>(
        dimensions: (u16, u16),
        path: Option<P>,
        files: Vec<Entry>,
    ) -> Self
    where
        P: Into<Cow<'a, Utf8Path>>,
    {
        let heading = Heading::new(PathItem(
            path.map(|p| p.into().into_owned())
        ));
        let list = {
            let layout = compute_layout(Rect {
                x: 0, y: 0,
                width: dimensions.0, height: dimensions.1
            });
            List::new(layout[1].height, to_fsitems(files), true)
        };
        App { heading, files: list }
    }

    /// The result of `get_files` is expected to be sorted by size, largest first.
    pub fn handle_event<E, G>(
        &mut self,
        get_files: G,
        event: Event,
    ) -> Result<Action, E>
    where
        G: FnOnce(Option<&Utf8Path>) -> Result<Vec<Entry>, E>,
    {
        log::debug!("received {:?}", event);
        use Event::*;
        use crossterm::event::KeyCode::*;
        match event {
            Resize(w, h) => Ok(self.handle_resize(w, h)),
            KeyPress(Char('q')) => Ok(Action::Quit),
            KeyPress(Right) => self.handle_right(get_files),
            KeyPress(Char(';')) => self.handle_right(get_files),
            KeyPress(Left) => self.handle_left(get_files),
            KeyPress(Char('h')) => self.handle_left(get_files),
            event => Ok(self.files.handle_event(event)),
        }
    }

    fn handle_resize(&mut self, w: u16, h: u16) -> Action {
        let layout = compute_dimensions((w, h));
        self.files.handle_event(Event::Resize(layout[1].0, layout[1].1))
    }

    fn handle_left<E, G>(
        &mut self,
        get_files: G,
    ) -> Result<Action, E>
    where
        G: FnOnce(Option<&Utf8Path>) -> Result<Vec<Entry>, E>,
    {
        path_pop(&mut self.heading);
        self.files.set_items(to_fsitems(get_files(self.path())?));
        log::debug!("path is now {:?}", self.path());
        Ok(Action::Render)
    }

    fn handle_right<E, G>(
        &mut self,
        get_files: G,
    ) -> Result<Action, E>
        where
            G: FnOnce(Option<&Utf8Path>) -> Result<Vec<Entry>, E>,
    {
        match self.files.selected_item() {
            Some(ListItem { name, is_dir, ..}) if *is_dir => {
                path_push(&mut self.heading, name);
                let files = get_files(self.path().as_deref())?;
                self.files.set_items(to_fsitems(files));
                Ok(Action::Render)
            }
            _ =>
                Ok(Action::Nothing)
        }
    }

    fn path(&self) -> Option<&Utf8Path> {
        self.heading.item.0.as_deref()
    }

}

impl WidgetRef for App {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let layout = compute_layout(area);
        self.heading.render_ref(layout[0], buf);
        self.files.render_ref(layout[1], buf);
    }
}

/// `files` is expected to be sorted by size, largest first.
fn to_fsitems(files: Vec<Entry>) -> Vec<ListItem> {
    if files.is_empty() { return Vec::new() }

    let largest = files[0].size() as f64;
    files
        .into_iter()
        .map(|e| match e {
            Entry::File(File{ path, size }) => ListItem {
                name: path,
                size,
                relative_size: size as f64 / largest,
                is_dir: false,
            },
            Entry::Directory(Directory{ path, size }) => ListItem {
                name: path,
                size,
                relative_size: size as f64 / largest,
                is_dir: true,
            },
        })
        .collect()
}

fn compute_layout(area: Rect) -> Rc<[Rect]> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Fill(100),
        ])
        .split(area)
}

fn compute_dimensions(dimensions: (u16, u16)) -> Box<[(u16, u16)]> {
    let layout = compute_layout(Rect {
        x: 0, y: 0,
        width: dimensions.0, height: dimensions.1
    });
    layout.iter().map(|r| (r.width, r.height)).collect()
}

fn path_push(heading: &mut Heading<PathItem>, name: &Utf8Path) {
    if let Some(path) = &mut heading.item.0 {
        path.push(name);
    } else {
        heading.item.0 = Some(name.to_owned());
    }
}

fn path_pop(heading: &mut Heading<PathItem>) {
    if let Some(path) = &mut heading.item.0 {
        if path.parent().is_none() {
            heading.item.0 = None;
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
        let f = ListItem {
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