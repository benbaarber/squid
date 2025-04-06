use std::{
    io::{self, Write, stdout},
    sync::{Arc, Mutex},
};

use log::{Level, LevelFilter, Log, Metadata, Record};

pub struct TUILog {
    pub level: Level,
    pub content: String,
}

struct TUILogger {
    logs: Arc<Mutex<Vec<TUILog>>>,
}

impl Log for TUILogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        let log = TUILog {
            level: record.level(),
            content: record.args().to_string(),
        };

        self.logs.lock().unwrap().push(log);
    }

    fn flush(&self) {}
}

pub fn init() -> Arc<Mutex<Vec<TUILog>>> {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let logger = TUILogger {
        logs: Arc::clone(&logs),
    };
    log::set_boxed_logger(Box::new(logger)).unwrap();
    log::set_max_level(LevelFilter::Debug);
    logs
}

pub fn flush(logs: Arc<Mutex<Vec<TUILog>>>) -> io::Result<()> {
    let logs = logs.lock().unwrap();
    let stdout = stdout();
    let mut handle = stdout.lock();
    for log in logs.iter() {
        writeln!(handle, "{}", log.content)?;
    }

    Ok(())
}
