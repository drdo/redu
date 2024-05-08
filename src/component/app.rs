use std::borrow::Cow;
use std::cmp::{max, min};
use std::rc::Rc;

use camino::{Utf8Path, Utf8PathBuf};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Line;
use ratatui::style::{Style, Stylize};
use ratatui::text::Span;
use ratatui::widgets::{Paragraph, WidgetRef};

use crate::component::{Action, Event, shorten_to, ToLine};
use crate::component::list::List;
use crate::types::{Directory, Entry, File};

struct ListItem {
    name: Utf8PathBuf,
    size: usize,
    relative_size: f64,
    is_dir: bool,
}

impl ToLine for ListItem {
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
                && self.name
                    .iter().last()
                    .and_then(|s| s.chars().last())
                    != Some('/')
            {
                let mut name = self.name.as_str().to_owned();
                name.push('/');
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
    path: Option<Utf8PathBuf>,
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
        let list = {
            let layout = compute_layout(Rect {
                x: 0, y: 0,
                width: dimensions.0, height: dimensions.1
            });
            List::new(layout[1].height, to_listitems(files), true)
        };
        App {
            path: path.map(|p| p.into().into_owned()),
            files: list,
        }
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
        path_pop(&mut self.path);
        self.files.set_items(to_listitems(get_files(self.path.as_deref())?));
        log::debug!("path is now {:?}", self.path.as_deref());
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
                path_push(&mut self.path, name);
                let files = get_files(self.path.as_deref())?;
                self.files.set_items(to_listitems(files));
                Ok(Action::Render)
            }
            _ =>
                Ok(Action::Nothing)
        }
    }
}

impl WidgetRef for App {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let layout = compute_layout(area);

        { // Heading
            let mut string = "--- ".to_string();
            string.push_str(
                shorten_to(
                    match &self.path {
                        None => "#",
                        Some(path) => path.as_str(),
                    },
                    area.width as usize - string.len()
                ).as_ref()
            );
            let mut remaining_width = max(
                0,
                area.width as isize - 4 - string.len() as isize
            ) as usize;
            if remaining_width > 0 {
                string.push(' ');
                remaining_width -= 1;
            }
            string.push_str(&"-".repeat(remaining_width));
            Paragraph::new(string).render_ref(area, buf);
        }

        self.files.render_ref(layout[1], buf);
    }
}

/// `files` is expected to be sorted by size, largest first.
fn to_listitems(files: Vec<Entry>) -> Vec<ListItem> {
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