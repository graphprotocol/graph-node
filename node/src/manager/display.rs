use std::io::{self, Write};

const LINE_WIDTH: usize = 78;

pub struct List {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl List {
    pub fn new(headers: Vec<&str>) -> Self {
        let headers = headers.into_iter().map(|s| s.to_string()).collect();
        Self {
            headers,
            rows: Vec::new(),
        }
    }

    pub fn append(&mut self, row: Vec<String>) {
        if row.len() != self.headers.len() {
            panic!(
                "there are {} headers but the row has {} entries: {:?}",
                self.headers.len(),
                row.len(),
                row
            );
        }
        self.rows.push(row);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn render(&self) {
        let header_width = self.headers.iter().map(|h| h.len()).max().unwrap_or(0);
        let header_width = if header_width < 5 { 5 } else { header_width };
        let mut first = true;
        for row in &self.rows {
            if !first {
                println!(
                    "{:-<width$}-+-{:-<rest$}",
                    "",
                    "",
                    width = header_width,
                    rest = LINE_WIDTH - 3 - header_width
                );
            }
            first = false;

            for (header, value) in self.headers.iter().zip(row) {
                println!("{:width$} | {}", header, value, width = header_width);
            }
        }
    }
}

/// A more general list of columns than `List`. In practical terms, this is
/// a very simple table with two columns, where both columns are
/// left-aligned
pub struct Columns {
    widths: Vec<usize>,
    rows: Vec<Row>,
}

impl Columns {
    pub fn push_row<R: Into<Row>>(&mut self, row: R) {
        let row = row.into();
        for (idx, width) in row.widths().iter().enumerate() {
            if idx >= self.widths.len() {
                self.widths.push(*width);
            } else {
                self.widths[idx] = (*width).max(self.widths[idx]);
            }
        }
        self.rows.push(row);
    }

    pub fn render(&self, out: &mut dyn Write) -> io::Result<()> {
        for row in &self.rows {
            row.render(out, &self.widths)?;
        }
        Ok(())
    }
}

impl Default for Columns {
    fn default() -> Self {
        Self {
            widths: Vec::new(),
            rows: Vec::new(),
        }
    }
}

pub enum Row {
    Cells(Vec<String>),
    Separator,
}

impl Row {
    pub fn separator() -> Self {
        Self::Separator
    }

    fn widths(&self) -> Vec<usize> {
        match self {
            Row::Cells(cells) => cells.iter().map(|cell| cell.len()).collect(),
            Row::Separator => vec![],
        }
    }

    fn render(&self, out: &mut dyn Write, widths: &[usize]) -> io::Result<()> {
        match self {
            Row::Cells(cells) => {
                for (idx, cell) in cells.iter().enumerate() {
                    if idx > 0 {
                        write!(out, " | ")?;
                    }
                    write!(out, "{cell:width$}", width = widths[idx])?;
                }
            }
            Row::Separator => {
                let total_width = widths.iter().sum::<usize>();
                let extra_width = if total_width >= LINE_WIDTH {
                    0
                } else {
                    LINE_WIDTH - total_width
                };
                for (idx, width) in widths.iter().enumerate() {
                    if idx > 0 {
                        write!(out, "-+-")?;
                    }
                    if idx == widths.len() - 1 {
                        write!(out, "{:-<width$}", "", width = width + extra_width)?;
                    } else {
                        write!(out, "{:-<width$}", "")?;
                    }
                }
            }
        }
        writeln!(out)
    }
}

impl From<[&str; 2]> for Row {
    fn from(row: [&str; 2]) -> Self {
        Self::Cells(row.iter().map(|s| s.to_string()).collect())
    }
}
