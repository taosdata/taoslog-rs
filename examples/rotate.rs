use std::{io::BufRead, thread};

use crossbeam::sync::WaitGroup;
use taoslog::{layer::TaosLayer, writer::RollingFileAppender, QidManager};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
struct Qid(u64);

impl QidManager for Qid {
    fn init() -> Self {
        Self(9223372036854775807)
    }

    fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Qid {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

fn main() {
    let appender = RollingFileAppender::builder(".", "taosx", 16)
        .compress(true)
        .reserved_disk_size("1GB")
        .rotation_count(3)
        .rotation_size("1GB")
        .build()
        .unwrap();

    tracing_subscriber::registry()
        .with(TaosLayer::<Qid>::new(appender))
        .try_init()
        .unwrap();

    let wg = WaitGroup::new();
    for _ in 0..100 {
        let wg = wg.clone();
        thread::spawn(|| {
            for _ in 0..1_000 {
                tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
                    tracing::info!(a = "aaa", b = "bbb", "outer example");

                    tracing::info_span!("inner").in_scope(inner);
                });
            }
            drop(wg)
        });
    }
    wg.wait();
    println!("finish bench");

    let stdin = std::io::stdin();
    let mut stdin_lock = stdin.lock();
    loop {
        tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
            tracing::info!(a = "aaa", b = "bbb", "outer example");

            tracing::info_span!("inner").in_scope(inner);
        });

        stdin_lock.read_line(&mut String::new()).unwrap();
    }
}

fn inner() {
    tracing::debug!("inner info log example");
    tracing::error!(c = "ccc", d = "ddd", "inner example");
}
