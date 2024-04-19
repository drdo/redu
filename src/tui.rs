use std::io::{Stdout, stdout};
use std::ops::{Deref, DerefMut};

use crossterm::ExecutableCommand;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use scopeguard::ScopeGuard;

pub struct Tui {
    _alternate_screen: ScopeGuard<(), Box<dyn FnOnce(()) + Send>>,
    _raw_mode: ScopeGuard<(), Box<dyn FnOnce(()) + Send>>,
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl Deref for Tui {
    type Target = Terminal<CrosstermBackend<Stdout>>;

    fn deref(&self) -> &Self::Target {
        &self.terminal
    }
}

impl DerefMut for Tui {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.terminal
    }
}

impl Tui {
    pub fn new() -> Result<Self, std::io::Error> {
        stdout().execute(EnterAlternateScreen)?;
        let alternate_screen = guard(|| {
            stdout().execute(LeaveAlternateScreen).unwrap();
        });
        enable_raw_mode()?;
        let raw_mode = guard(|| {
            disable_raw_mode().unwrap();
        });
        let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
        terminal.clear()?;
        Ok(Tui {
            _alternate_screen: alternate_screen,
            _raw_mode: raw_mode,
            terminal,
        })
    }
}

fn guard<'a>(dropfn: impl FnOnce() + Send + 'a) -> ScopeGuard<(), Box<dyn FnOnce(()) + Send + 'a>> {
    scopeguard::guard((), Box::new(|()| dropfn()))
}
