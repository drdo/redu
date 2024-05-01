use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::rc::Rc;

use camino::{Utf8Path, Utf8PathBuf};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Line;
use ratatui::widgets::WidgetRef;

use crate::component::{Action, Event, ToLine};
use crate::component::heading::Heading;
use crate::component::list::List;

struct PathItem(Option<Utf8PathBuf>);

impl Display for PathItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            None => f.write_fmt(format_args!("#")),
            Some(path) => f.write_fmt(format_args!("{path}")),
        }
    }
}

struct FileItem {
    name: Utf8PathBuf,
    size: usize,
    relative_size: f64,
}

impl ToLine for FileItem {
    fn to_line(&self, width: u16) -> Line {
        const MAX_BAR_SIZE: usize = 24;
        let bar_size = (self.relative_size * MAX_BAR_SIZE as f64) as usize;
        Line::raw(format!(
            " {:>10} [{:#^bar_size$}{:empty_bar_size$}] {}",
            humansize::format_size(self.size, humansize::BINARY),
            "", "",
            self.name,
            bar_size = bar_size,
            empty_bar_size = MAX_BAR_SIZE - bar_size,
        ))
    }
}

pub struct App {
    heading: Heading<PathItem>,
    list: List<FileItem>,
}

impl App {
    /// `files` is expected to be sorted by size, largest first.
    pub fn new<'a, P>(
        dimensions: (u16, u16),
        path: Option<P>,
        files: Vec<(Utf8PathBuf, usize)>,
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
            List::new(layout[1].height, to_fileitems(files), true)
        };
        App { heading, list }
    }

    /// The result of `get_files` is expected to be sorted by size, largest first.
    pub fn handle_event<E, G>(
        &mut self,
        get_files: G,
        event: Event,
    ) -> Result<Action, E>
    where
        G: FnOnce(Option<&Utf8Path>) -> Result<Vec<(Utf8PathBuf, usize)>, E>,
    {
        log::debug!("received {:?}", event);
        use Event::*;
        match event {
            Quit => Ok(Action::Quit),
            Left => {
                path_pop(&mut self.heading);
                self.list.set_items(to_fileitems(get_files(self.path())?));
                log::debug!("path is now {:?}", self.path());
                Ok(Action::Render)
            },
            Right => {
                if let Some(FileItem{name, ..}) = self.list.selected_item() {
                    path_push(&mut self.heading, name);
                    let files = get_files(self.path().as_deref())?;
                    if ! files.is_empty() {
                        self.list.set_items(to_fileitems(files));
                        return Ok(Action::Render);
                    }
                }
                Ok(Action::Nothing)
            }
            Resize(w, h) => {
                let layout = compute_dimensions((w, h));
                Ok(self.list.handle_event(Resize(layout[1].0, layout[1].1)))
            }
            e => Ok(self.list.handle_event(e)),
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
        self.list.render_ref(layout[1], buf);
    }
}

/// `files` is expected to be sorted by size, largest first.
fn to_fileitems(files: Vec<(Utf8PathBuf, usize)>) -> Vec<FileItem> {
    if files.is_empty() { return Vec::new() }

    let largest = files[0].1 as f64;
    files
        .into_iter()
        .map(|(name, size)| FileItem {
            name,
            size,
            relative_size: size as f64 / largest
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
