use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};
use arrow_schema::Schema;
use tracing_subscriber::{registry::LookupSpan, Registry};

use crate::QidManager;

const QID_HEADER_KEY: &str = "x-qid";

pub fn set_qid<'a, M, Q>(meta: M, qid: Q)
where
    M: Into<QidMetadataMut<'a>>,
    Q: QidManager,
{
    let meta = meta.into();
    meta.set_qid(qid);
}

pub fn get_qid<'a, M, Q>(meta: M) -> Option<Q>
where
    M: Into<QidMetadataRef<'a>>,
    Q: QidManager,
{
    let meta = meta.into();
    meta.get_qid()
}

pub struct Span;

pub enum QidMetadataMut<'a> {
    Span,
    HttpHeader(&'a mut HeaderMap),
    RecordBatchSchema(&'a mut Schema),
}

impl<'a> From<Span> for QidMetadataMut<'a> {
    fn from(_: Span) -> Self {
        Self::Span
    }
}

impl<'a> From<&'a mut Schema> for QidMetadataMut<'a> {
    fn from(schema: &'a mut Schema) -> Self {
        Self::RecordBatchSchema(schema)
    }
}

impl<'a> From<&'a mut HeaderMap> for QidMetadataMut<'a> {
    fn from(header: &'a mut HeaderMap) -> Self {
        Self::HttpHeader(header)
    }
}

impl<'a> QidMetadataMut<'a> {
    fn set_qid<Q>(self, qid: Q)
    where
        Q: QidManager,
    {
        match self {
            QidMetadataMut::Span => tracing::dispatcher::get_default(|dispatch| {
                let registry = dispatch
                    .downcast_ref::<Registry>()
                    .expect("no global default dispatcher found");
                if let Some((id, _meta)) = dispatch.current_span().into_inner() {
                    let span = registry.span(&id).unwrap();
                    let mut ext = span.extensions_mut();
                    ext.replace(qid.clone());
                }
            }),
            QidMetadataMut::HttpHeader(headers) => {
                headers.insert(
                    HeaderName::from_static(QID_HEADER_KEY),
                    HeaderValue::from_str(&format!("{:#018x}", qid.get())).unwrap(),
                );
            }
            QidMetadataMut::RecordBatchSchema(schema) => {
                schema
                    .metadata
                    .insert(QID_HEADER_KEY.to_owned(), format!("{:#018x}", qid.get()));
            }
        }
    }
}

pub enum QidMetadataRef<'a> {
    Span,
    HttpHeader(&'a HeaderMap),
    RecordBatchSchema(&'a Schema),
}

impl<'a> From<Span> for QidMetadataRef<'a> {
    fn from(_: Span) -> Self {
        Self::Span
    }
}

impl<'a> From<&'a Schema> for QidMetadataRef<'a> {
    fn from(schema: &'a Schema) -> Self {
        Self::RecordBatchSchema(schema)
    }
}

impl<'a> From<&'a HeaderMap> for QidMetadataRef<'a> {
    fn from(header: &'a HeaderMap) -> Self {
        Self::HttpHeader(header)
    }
}

impl<'a> QidMetadataRef<'a> {
    pub fn get_qid<Q>(self) -> Option<Q>
    where
        Q: QidManager,
    {
        match self {
            QidMetadataRef::Span => tracing::dispatcher::get_default(|dispatch| {
                let registry = dispatch
                    .downcast_ref::<Registry>()
                    .expect("no global default dispatcher found");
                dispatch.current_span().into_inner().and_then(|(id, _)| {
                    let span = registry.span(&id).unwrap();
                    let ext = span.extensions();
                    ext.get::<Q>().cloned()
                })
            }),
            QidMetadataRef::HttpHeader(headers) => headers
                .get(QID_HEADER_KEY)
                .and_then(|x| x.to_str().ok())
                .and_then(|x| x.get(2..))
                .and_then(|x| u64::from_str_radix(x, 16).ok())
                .map(|x| Q::from(x)),
            QidMetadataRef::RecordBatchSchema(schema) => schema
                .metadata
                .get(QID_HEADER_KEY)
                .and_then(|x| x.get(2..))
                .and_then(|x| u64::from_str_radix(x, 16).ok())
                .map(|x| Q::from(x)),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::fake::Qid;

    use super::*;

    #[test]
    fn qid_set_get_test() {
        let qid_u64 = 9223372036854775807;
        let qid = Qid::from(qid_u64);

        {
            let mut header = HeaderMap::new();
            set_qid(&mut header, qid.clone());

            assert_eq!(header.get(QID_HEADER_KEY).unwrap(), "0x7fffffffffffffff");

            let qid: Qid = get_qid(&header).unwrap();
            assert_eq!(qid.get(), qid_u64);
        }

        {
            let mut schema = Schema::empty();
            set_qid(&mut schema, qid.clone());

            assert_eq!(
                schema.metadata.get(QID_HEADER_KEY).unwrap(),
                "0x7fffffffffffffff"
            );

            let qid: Qid = get_qid(&schema).unwrap();
            assert_eq!(qid.get(), qid_u64);
        }

        {
            use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .try_init()
                .unwrap();

            tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
                set_qid::<'_, _, Qid>(Span, qid);
                let qid: Qid = get_qid(Span).unwrap();
                assert_eq!(qid.get(), qid_u64);
            });
        }
    }
}
