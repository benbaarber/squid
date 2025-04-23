use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::writer::MakeWriter;

#[derive(Clone)]
pub struct StdoutBuffer {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl StdoutBuffer {
    pub fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn flush(&self) -> io::Result<()> {
        let buf = std::mem::take(&mut *self.buf.lock().unwrap());
        let mut stdout = io::stdout();
        stdout.write_all(&buf)?;
        stdout.flush()
    }
}

impl<'a> MakeWriter<'a> for StdoutBuffer {
    type Writer = StdoutBufferGuard<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        StdoutBufferGuard {
            lock: self.buf.lock().unwrap(),
        }
    }
}

pub struct StdoutBufferGuard<'a> {
    lock: std::sync::MutexGuard<'a, Vec<u8>>,
}

impl<'a> Write for StdoutBufferGuard<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.lock.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.lock.flush()
    }
}
