use crossterm::event::KeyCode;
use ratatui::text::Line;
use std::borrow::Cow;
use unicode_segmentation::UnicodeSegmentation;

mod list;
pub mod app;
mod heading;

#[derive(Debug)]
pub enum Event {
    Resize(u16, u16),
    KeyPress(KeyCode),
}

#[derive(Debug)]
pub enum Action {
    Nothing,
    Render,
    Quit,
}

trait ToLine {
    fn to_line(&self, width: u16, selected: bool) -> Line;
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
    res
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use crate::component::shorten_to;

    use super::*;

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
}
