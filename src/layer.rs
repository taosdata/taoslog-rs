use std::marker::PhantomData;

use chrono::{DateTime, Local};
use tracing::{
    field::{self, Visit},
    Event,
};
use tracing_subscriber::{
    fmt::MakeWriter,
    registry::{LookupSpan, Scope},
    Registry,
};

use crate::{writer::RollingFileAppender, QidManager};

#[derive(Clone)]
struct RecordFields(Vec<String>, Option<String>);

pub struct TaosLayer<Q, S = Registry, M = RollingFileAppender> {
    make_writer: M,
    _s: PhantomData<fn(S)>,
    _q: PhantomData<Q>,
}

impl<Q, S, M> TaosLayer<S, Q, M> {
    pub fn new(make_writer: M) -> Self {
        Self {
            make_writer,
            _s: PhantomData,
            _q: PhantomData,
        }
    }
}

impl<Q, S, M> tracing_subscriber::Layer<S> for TaosLayer<Q, S, M>
where
    S: tracing::subscriber::Subscriber + for<'a> LookupSpan<'a>,
    M: for<'writer> MakeWriter<'writer> + 'static,
    Q: QidManager,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx
            .span(id)
            .expect("Span not found, this is a bug in tracing");
        let qid = match span
            .parent()
            .as_ref()
            .and_then(|p| p.extensions().get::<Q>().cloned())
        {
            Some(qid) => qid,
            None => Q::init(),
        };
        let mut extensions = span.extensions_mut();
        extensions.replace(qid);

        if extensions.get_mut::<RecordFields>().is_none() {
            let mut fields = Vec::new();
            let mut message = None;
            attrs
                .values()
                .record(&mut RecordVisit(&mut fields, &mut message));
            extensions.replace(RecordFields(fields, message));
        }
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx
            .span(id)
            .expect("Span not found, this is a bug in tracing");
        let mut extensions = span.extensions_mut();
        match extensions.get_mut::<RecordFields>() {
            Some(RecordFields(fields, message)) => {
                values.record(&mut RecordVisit(fields, message));
            }
            None => {
                let mut fields = Vec::new();
                let mut message = None;
                values.record(&mut RecordVisit(&mut fields, &mut message));
                extensions.replace(RecordFields(fields, message));
            }
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        thread_local! {
            static BUF: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
        }

        BUF.with(|buf| {
            let borrow = buf.try_borrow_mut();
            let mut a;
            let mut b;
            let buf = match borrow {
                Ok(buf) => {
                    a = buf;
                    &mut *a
                }
                _ => {
                    b = String::new();
                    &mut b
                }
            };

            // Part 1: timestamp
            fmt_timestamp(buf);
            // Part 2: process id
            fmt_thread_id(buf);
            // Part 3: level
            let metadata = event.metadata();
            fmt_level(buf, metadata.level());
            // Part 4 and Part 5:  span and TID or DTID
            let Some(scope) = ctx.event_scope(event) else {
                return
            };
            fmt_fields_and_qid::<S, Q>(buf, event, scope);
            // Part 6: write event content
            buf.push('\n');
            // put all to writer
            let mut writer = self.make_writer.make_writer_for(metadata);
            let res = std::io::Write::write_all(&mut writer, buf.as_bytes());
            if let Err(e) = res {
                eprintln!("[TaosLayer] Unable to write an event to the Writer for this Subscriber! Error: {}\n", e);
            }
            buf.clear();
        });
    }
}

fn fmt_timestamp(buf: &mut String) {
    let local: DateTime<Local> = Local::now();
    let s = local.format("%m/%d %H:%M:%S.%6f ").to_string();
    buf.push_str(s.as_str())
}

fn fmt_thread_id(buf: &mut String) {
    buf.push_str(&format!("{:0>8}", thread_id::get()))
}

fn fmt_level(buf: &mut String, level: &tracing::Level) {
    buf.push(' ');
    let level = match *level {
        tracing::Level::TRACE => "TRACE",
        tracing::Level::DEBUG => "DEBUG",
        tracing::Level::INFO => "INFO ",
        tracing::Level::WARN => "WARN ",
        tracing::Level::ERROR => "ERROR",
    };
    buf.push_str(level);
    buf.push(' ');
}

pub struct RecordVisit<'a>(&'a mut Vec<String>, &'a mut Option<String>);

impl<'a> Visit for RecordVisit<'a> {
    fn record_str(&mut self, field: &field::Field, value: &str) {
        if field.name() == "message" {
            self.1.replace(value.to_string());
        } else {
            self.0.push(format!(
                "{}:{}",
                format_str(field.name()),
                format_str(value)
            ));
        }
    }

    fn record_debug(&mut self, field: &field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.1.replace(format!("{value:?}"));
        } else {
            self.0
                .push(format!("{}:{value:?}", format_str(field.name())));
        }
    }
}

fn fmt_fields_and_qid<'a, S, Q>(buf: &mut String, event: &Event, scope: Scope<S>)
where
    S: for<'s> LookupSpan<'s>,
    Q: QidManager,
{
    let mut kvs = Vec::new();
    let mut message = None;
    event.record(&mut RecordVisit(&mut kvs, &mut message));

    let mut qid_field = None;

    let print_stacktrace = event.metadata().level() >= &tracing::Level::DEBUG;

    let mut spans = vec![];
    for span in scope.from_root() {
        if print_stacktrace {
            spans.push(format_str(span.name()));
        }

        {
            if let Some(qid) = span.extensions().get::<Q>().cloned() {
                qid_field.replace(qid.get());
            }
        }
        {
            if let Some(fields) = span.extensions_mut().remove::<RecordFields>() {
                for s in fields.0 {
                    kvs.push(s)
                }
            }
        }
    }

    if let Some(qid) = qid_field {
        buf.push_str(&format!("qid:{:#018x}", qid));
        buf.push(' ');
    }

    if !kvs.is_empty() {
        buf.push_str(&kvs.join(", "));
        buf.push(' ');
    }

    if let Some(message) = message {
        buf.push_str(&message);
    }

    if print_stacktrace {
        buf.push(' ');
        buf.push_str(&format!("stack:{}", spans.join("->")));
    }
}

fn format_str(value: &str) -> String {
    if value.contains(' ') {
        format!("{value:?}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::{
        fake::Qid,
        layer::TaosLayer,
        utils::{QidMetadataGetter, QidMetadataSetter, Span},
        QidManager,
    };

    use super::fmt_thread_id;

    #[test]
    fn fmt_thread_id_test() {
        let mut buf = String::new();
        fmt_thread_id(&mut buf);
        dbg!(buf);
    }

    #[test]
    fn layer_test() {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
        tracing_subscriber::registry()
            .with(TaosLayer::<Qid, _, _>::new(Mutex::new(std::io::empty())))
            .try_init()
            .unwrap();

        tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
            // test qid init
            let qid: Qid = Span.get_qid().unwrap();
            assert_eq!(qid.get(), 9223372036854775807);
            Span.set_qid(Qid::from(999));
            tracing::info_span!("inner").in_scope(|| {
                // test qid inherit
                let qid: Qid = Span.get_qid().unwrap();
                assert_eq!(qid.get(), 999);
            })
        });
    }
}
