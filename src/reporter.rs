use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

pub trait Reporter {
    type Loader: Item;
    type Counter: Item + Counter;
    type Bar: Item + Counter;

    fn print<M: AsRef<str>>(&self, msg: M);

    fn add_loader<M: AsRef<str>>(&self, level: usize, msg: M) -> Self::Loader;

    fn add_counter<P: AsRef<str>, S: AsRef<str>>(
        &self,
        level: usize,
        prefix: P,
        suffix: S,
    ) -> Self::Counter;

    fn add_bar<P: AsRef<str>>(
        &self,
        level: usize,
        prefix: P,
        total: u64,
    ) -> Self::Bar;
}

impl<T: Reporter> Reporter for &T {
    type Loader = T::Loader;
    type Counter = T::Counter;
    type Bar = T::Bar;

    fn print<M: AsRef<str>>(&self, msg: M) {
        (*self).print(msg);
    }

    fn add_loader<M: AsRef<str>>(&self, level: usize, msg: M) -> Self::Loader {
        (*self).add_loader(level, msg)
    }

    fn add_counter<P: AsRef<str>, S: AsRef<str>>(
        &self,
        level: usize,
        prefix: P,
        suffix: S,
    ) -> Self::Counter {
        (*self).add_counter(level, prefix, suffix)
    }

    fn add_bar<P: AsRef<str>>(
        &self,
        level: usize,
        prefix: P,
        total: u64,
    ) -> Self::Bar {
        (*self).add_bar(level, prefix, total)
    }
}

pub trait Item {
    fn end(self);
}

pub trait Counter {
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
    type Loader = TermItem;
    type Counter = TermItem;
    type Bar = TermItem;

    fn print<M: AsRef<str>>(&self, msg: M) {
        self.0.suspend(|| eprintln!("{}", msg.as_ref()));
    }

    fn add_loader<M: AsRef<str>>(&self, level: usize, msg: M) -> Self::Loader {
        self.add(
            format!("{}{{spinner}} {}", " ".repeat(level), msg.as_ref()),
            None,
        )
    }

    fn add_counter<P: AsRef<str>, S: AsRef<str>>(
        &self,
        level: usize,
        prefix: P,
        suffix: S,
    ) -> Self::Counter {
        self.add(
            format!(
                "{}{{spinner}} {}{{pos}}{}",
                " ".repeat(level),
                prefix.as_ref(),
                suffix.as_ref()
            ),
            None,
        )
    }

    fn add_bar<P: AsRef<str>>(
        &self,
        level: usize,
        prefix: P,
        total: u64,
    ) -> Self::Bar {
        self.add(
            format!(
                "{}{{spinner}} {}{{wide_bar}} [{{pos}}/{{len}}]",
                " ".repeat(level),
                prefix.as_ref(),
            ),
            Some(total),
        )
    }
}

pub struct TermItem {
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
    fn end(self) {}
}

impl Counter for TermItem {
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
