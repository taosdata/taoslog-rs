use taoslog::{layer::TaosLayer, writer::RollingFileAppender, QidManager};
use tracing_subscriber::{
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

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
    let mut layers = Vec::with_capacity(2);
    let appender = RollingFileAppender::builder(".", "explorer", 16)
        .compress(true)
        .reserved_disk_size("1GB")
        .rotation_count(3)
        .rotation_size("1GB")
        .build()
        .unwrap();
    layers.push(TaosLayer::<Qid>::new(appender).boxed());

    if cfg!(debug_assertions) {
        layers.push(
            TaosLayer::<Qid, _, _>::new(std::io::stdout)
                // .with_ansi()
                .boxed(),
        );
    }

    tracing_subscriber::registry().with(layers).init();

    tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
        tracing::info!(a = "aaa", b = "bbb", "outer example");

        tracing::info_span!("inner").in_scope(|| {
            tracing::trace!("trace example");
            tracing::warn!("warn example");
            tracing::debug!("inner info log example");
            tracing::error!(c = "ccc", d = "ddd", "inner example");
        });
    });
}
