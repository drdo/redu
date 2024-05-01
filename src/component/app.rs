use std::borrow::Cow;
use std::cmp::{max, min};
use std::fmt::{Display, Formatter};
use std::rc::Rc;

use camino::{Utf8Path, Utf8PathBuf};
use log::trace;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Line;
use ratatui::widgets::WidgetRef;
use unicode_segmentation::UnicodeSegmentation;

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

impl FileItem {
    fn to_line_string(&self, width: u16) -> String {
        let mut text =
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
        {
            let available_width = max(0, width as isize - text.len() as isize) as usize;
            text.push_str(shorten_to(self.name.as_str(), available_width).as_ref());
        }
        text
    }
}

impl ToLine for FileItem {
    fn to_line(&self, width: u16) -> Line {
        Line::raw(self.to_line_string(width))
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
    trace!("shorten_to({}, {}) -> {}", s, width, res);
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn shorten_to_len_lt_width() {
        let s = "12345";
        assert_eq!(shorten_to(s, 6), Cow::Borrowed(s));
    }

    #[test]
    fn shorten_to_width_lt_3() {
        let s = "12345";
        assert_eq!(shorten_to(s, 2), Cow::Owned::<str>("..".to_owned()));
    }

    #[test]
    fn shorten_to_width_lte_len() {
        let s = "123456789";
        assert_eq!(shorten_to(s, 3), Cow::Owned::<str>("...".to_owned()));
        assert_eq!(shorten_to(s, 4), Cow::Owned::<str>("...9".to_owned()));
        assert_eq!(shorten_to(s, 5), Cow::Owned::<str>("1...9".to_owned()));
        assert_eq!(shorten_to(s, 8), Cow::Owned::<str>("12...789".to_owned()));
        assert_eq!(shorten_to(s, 9), Cow::Owned::<str>("123456789".to_owned()));
    }

    #[test]
    fn fileitem_to_line() {
        let f = FileItem {
            name: "1234567890123456789012345678901234567890".into(),
            size: 999 * 1024 + 1010,
            relative_size: 0.9,
        };
        assert_eq!(
            f.to_line_string(80),
            " 999.99 KiB [##############  ] 1234567890123456789012345678901234567890".to_owned()
        );
        assert_eq!(
            f.to_line_string(2),
            " 999.99 KiB [##############  ] ".to_owned()
        );
    }
}