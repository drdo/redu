use std::time::SystemTime;

pub struct Writer<W: std::io::Write> {
    writer: W,
    context: Vec<String>,
}

impl<W: std::io::Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Writer { writer, context: Vec::new() }
    }

    fn indent(&mut self) -> std::io::Result<()> {
        write!(self.writer, "{}", "    ".repeat(self.context.len()))
    }

    fn enter_rel(&mut self, rel_dir: &[&str]) -> std::io::Result<()> {
        for component in rel_dir {
            self.context.push(component.to_string());
            self.indent()?;
            writeln!(self.writer, r#",[{{"name": "{}"}}"#, escape_double_quote(component))?;
        }
        Ok(())
    }

    fn leave(&mut self, n: usize) -> std::io::Result<()> {
        for _ in 0..n {
            self.indent()?;
            writeln!(self.writer, "]")?;
            self.context.pop();
        }
        Ok(())
    }

    pub fn header(&mut self) -> std::io::Result<()>{
        let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        writeln!(self.writer, r#"[1,2,{{"progname": "dorestic", "progver": "0", "timestamp": {timestamp}}}, [{{"name": "/FAKEROOT"}}"#)
    }

    pub fn finish(&mut self) -> std::io::Result<()> {
        self.leave(self.context.len())?;
        writeln!(self.writer, "]]")
    }

    pub fn change_dir(&mut self, dir: &[&str]) -> std::io::Result<()> {
        let common_prefix_len = longest_common_prefix(&self.context, dir);
        self.leave(self.context.len() - common_prefix_len)?;
        if dir.len() - common_prefix_len > 0 {
            self.enter_rel(&dir[common_prefix_len..])?;
        }
        Ok(())
    }

    pub fn file(&mut self, file: &str, size: u64) -> std::io::Result<()> {
        self.indent()?;
        writeln!(self.writer, r#",{{"name": "{}", "asize": {}}}"#, escape_double_quote(file), size)
    }
}

fn longest_common_prefix<A,B>(a: &[A], b: &[B]) -> usize
where
    A: PartialEq<B>
{
    a.iter().zip(b).take_while(|(x,y)| x == y).count()
}

fn escape_double_quote(s: &str) -> String {
    let mut res = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '"' { res.push_str("\\\""); }
        else { res.push(c); }
    }
    res
}

