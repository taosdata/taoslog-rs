#![allow(dead_code)]

use std::path::PathBuf;

pub mod layer;
pub mod middleware;
pub mod utils;
pub mod writer;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Create log dir {} error: {source}", path.display()))]
    CreateLogDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Open log file {} error: {source}", path.display()))]
    OpenLogFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Get file {} size error: {source}", path.display()))]
    GetFileSize {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Compress file {} error: {source}", path.display()))]
    Compress {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("List dir {} error: {source}", path.display()))]
    ReadDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Parse date error: {source}"))]
    ParseDate { source: chrono::ParseError },
    #[snafu(display("Invalid rotation size: {size}"))]
    InvalidRotationSize { size: String },
    #[snafu(display("Get disk space error"))]
    DiskMountPointNotFound,
    #[snafu(display("Get log absolute path error: {source}"))]
    GetLogAbsolutePath { source: std::io::Error },
}

pub trait QidManager: Send + Sync + 'static + Clone + From<u64> {
    fn init() -> Self;

    fn get(&self) -> u64;
}

#[cfg(test)]
pub(crate) mod fake {
    use crate::QidManager;

    #[derive(Clone)]
    pub(crate) struct Qid(u64);

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
}
