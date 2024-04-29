use std::borrow::Cow;

use camino::{Utf8Path, Utf8PathBuf};

pub struct State {
    screen: (u16, u16),
    path: Option<Utf8PathBuf>,
    files: Vec<(Utf8PathBuf, usize)>,
    selected: Option<usize>,
    pub offset: usize,
}

impl State {
    pub fn new<'a, P>(
        width: u16,
        height: u16,
        path: Option<P>,
        files: Vec<(Utf8PathBuf, usize)>,
    ) -> Self
    where
        P: Into<Cow<'a, Utf8Path>>,
    {
        State {
            screen: (width, height),
            selected: if files.is_empty() { None } else { Some(0) },
            offset: 0,
            path: path.map(|p| p.into().into_owned()),
            files,
        }
    }

    fn fix_selected_visibility(&mut self) {
        if let Some(selected) = self.selected() {
            let offset = self.offset as isize;
            let selected = selected as isize;
            let h = self.screen.1 as isize;
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

    pub fn resize(&mut self, w: u16, h: u16) {
        self.screen = (w, h);
        self.fix_selected_visibility()
    }

    pub fn set_files<'a, P>(
        &mut self,
        path: Option<P>,
        files: Vec<(Utf8PathBuf, usize)>,
    )
    where
        P: Into<Cow<'a, Utf8Path>>,
    {
        self.selected = if files.is_empty() { None } else { Some(0) };
        self.offset = 0;
        self.path = path.map(|p| p.into().into_owned());
        self.files = files;
    }
    pub fn files(&self) -> &[(Utf8PathBuf, usize)] {
        &self.files
    }

    pub fn path(&self) -> Option<&Utf8Path> {
        self.path.as_deref()
    }

    pub fn selected_file(&self) -> Option<(&Utf8Path, usize)> {
        self.selected.map(|i| {
            let (name, size) = &self.files[i];
            (&**name, *size)
        })
    }

    pub fn selected(&self) -> Option<usize> {
        self.selected
    }

    pub fn is_selected(&self, index: usize) -> bool {
        Some(index) == self.selected
    }

    pub fn move_selection(&mut self, delta: isize) {
        if delta == 0 { return }
        let len = match self.files.len() {
            0 => return,
            n => n,
        };
        let new_index = match self.selected {
            None =>
                if delta < 0 {
                    (len as isize) + delta
                } else {
                    delta - 1
                }
            Some(selected) => selected as isize + delta
        };
        self.selected = Some(new_index.rem_euclid(len as isize) as usize);
        self.fix_selected_visibility();
    }
}