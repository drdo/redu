use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::rc::Rc;

use camino::{Utf8Path, Utf8PathBuf};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::widgets::WidgetRef;
use crate::component::{Action, Event};
use crate::component::list::List;

pub struct FileItem {
    pub name: Utf8PathBuf,
    pub size: usize,
}

impl Display for FileItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{} : {}",
            self.name,
            humansize::format_size(self.size, humansize::BINARY),
        ))
    }
}

pub struct App {
    path: Option<Utf8PathBuf>,
    list: List<FileItem>,
}

impl App {
    pub fn new<'a, P>(
        dimensions: (u16, u16),
        path: Option<P>,
        files: Vec<FileItem>,
    ) -> Self
    where
        P: Into<Cow<'a, Utf8Path>>,
    {
        let layout = compute_layout(Rect {
            x: 0, y: 0,
            width: dimensions.0, height: dimensions.1
        });
        App {
            path: path.map(|p| p.into().into_owned()),
            list: List::new(layout[1].height, files, true),
        }
    }

    pub fn handle_event<E>(
        &mut self,
        get_files: impl FnOnce(Option<&Utf8Path>) -> Result<Vec<FileItem>, E>,
        event: Event,
    ) -> Result<Action, E>
    {
        use Event::*;
        match event {
            Quit => Ok(Action::Quit),
            Left => {
                let parent = self.path.take().and_then(|p| p
                    .parent()
                    .map(ToOwned::to_owned)
                );
                self.list.set_items(get_files(parent.as_deref())?);
                self.path = parent;
                Ok(Action::Render)
            },
            Right => {
                if let Some(FileItem{name, ..}) = self.list.selected_item() {
                    self.path
                        .get_or_insert_default()
                        .push(name);
                    let files = get_files(self.path.as_deref())?;
                    if ! files.is_empty() {
                        self.list.set_items(files);
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
}

impl WidgetRef for App {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let layout = compute_layout(area);
        self.list.render_ref(layout[1], buf);
    }
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
