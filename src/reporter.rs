use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

pub trait Reporter {
    fn print(&self, msg: &str);

    fn add_loader(&self, level: usize, msg: &str) -> Box<dyn Item + Send>;

    fn add_counter(
        &self,
        level: usize,
        prefix: &str,
        suffix: &str,
    ) -> Box<dyn Counter + Send>;

    fn add_bar(
        &self,
        level: usize,
        prefix: &str,
        total: u64,
    ) -> Box<dyn Counter + Send>;
}

impl<T: Reporter> Reporter for &T {
    fn print(&self, msg: &str) {
        (*self).print(msg);
    }

    fn add_loader(&self, level: usize, msg: &str) -> Box<dyn Item + Send> {
        (*self).add_loader(level, msg)
    }

    fn add_counter(
        &self,
        level: usize,
        prefix: &str,
        suffix: &str,
    ) -> Box<dyn Counter + Send> {
        (*self).add_counter(level, prefix, suffix)
    }

    fn add_bar(
        &self,
        level: usize,
        prefix: &str,
        total: u64,
    ) -> Box<dyn Counter + Send> {
        (*self).add_bar(level, prefix, total)
    }
}

pub trait Item {
    fn end(self: Box<Self>);
}

pub trait Counter {
    fn end(self: Box<Self>);
    fn inc(&mut self, delta: u64);
}

////////// Term ////////////////////////////////////////////////////////////////
#[derive(Clone)]
pub struct TermReporter(MultiProgress);

impl TermReporter {
    pub fn new() -> Self {
        Self(MultiProgress::new())
    }

    fn add<T: AsRef<str>>(&self, template: T, total: Option<u64>) -> TermItem {
        let pb = new_pb(template.as_ref());
        if let Some(total) = total {
            pb.set_length(total);
        }
        let pb = self.0.add(pb);
        pb.enable_steady_tick(PB_TICK_INTERVAL);
        TermItem { parent: self.clone(), pb }
    }
}

impl Default for TermReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Reporter for TermReporter {
    fn print(&self, msg: &str) {
        eprintln!("{msg}");
    }

    fn add_loader(&self, level: usize, msg: &str) -> Box<dyn Item + Send> {
        Box::new(
            self.add(format!("{}{{spinner}} {}", " ".repeat(level), msg), None),
        )
    }

    fn add_counter(
        &self,
        level: usize,
        prefix: &str,
        suffix: &str,
    ) -> Box<dyn Counter + Send> {
        Box::new(self.add(
            format!(
                "{}{{spinner}} {}{{pos}}{}",
                " ".repeat(level),
                prefix,
                suffix,
            ),
            None,
        ))
    }

    fn add_bar(
        &self,
        level: usize,
        prefix: &str,
        total: u64,
    ) -> Box<dyn Counter + Send> {
        Box::new(self.add(
            format!(
                "{}{{spinner}} {}{{wide_bar}} [{{pos}}/{{len}}]",
                " ".repeat(level),
                prefix,
            ),
            Some(total),
        ))
    }
}

struct TermItem {
    parent: TermReporter,
    pb: ProgressBar,
}

impl Drop for TermItem {
    fn drop(&mut self) {
        self.pb.abandon();
        self.parent.0.remove(&self.pb);
    }
}

impl Item for TermItem {
    fn end(self: Box<Self>) {}
}

impl Counter for TermItem {
    fn end(self: Box<Self>) {
        <Self as Item>::end(self)
    }
    fn inc(&mut self, delta: u64) {
        self.pb.inc(delta);
    }
}

fn new_pb(template: &str) -> ProgressBar {
    let frames = &[
        "(●    )",
        "( ●   )",
        "(  ●  )",
        "(   ● )",
        "(   ● )",
        "(  ●  )",
        "( ●   )",
        "(●    )",
        "(●    )",
    ];
    let style =
        ProgressStyle::with_template(template).unwrap().tick_strings(frames);
    ProgressBar::new_spinner().with_style(style)
}

const PB_TICK_INTERVAL: Duration = Duration::from_millis(100);

////////// NullReporter ////////////////////////////////////////////////////////
pub struct NullReporter;

impl NullReporter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NullReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Reporter for NullReporter {
    fn print(&self, _msg: &str) {}

    fn add_loader(&self, _level: usize, _msg: &str) -> Box<dyn Item + Send> {
        Box::new(NullItem)
    }

    fn add_counter(
        &self,
        _level: usize,
        _prefix: &str,
        _suffix: &str,
    ) -> Box<dyn Counter + Send> {
        Box::new(NullItem)
    }

    fn add_bar(
        &self,
        _level: usize,
        _prefix: &str,
        _total: u64,
    ) -> Box<dyn Counter + Send> {
        Box::new(NullItem)
    }
}

struct NullItem;

impl Item for NullItem {
    fn end(self: Box<Self>) {}
}

impl Counter for NullItem {
    fn end(self: Box<Self>) {
        <Self as Item>::end(self)
    }

    fn inc(&mut self, _delta: u64) {}
}
