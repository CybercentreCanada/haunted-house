//! Error handling tools. Mostly methods to transform library errors into a common enum type.
use crate::types::FilterID;

/// enumeration of all error codes
#[derive(Debug, PartialEq)]
pub enum ErrorKinds {
    /// A requested blob won't fit within the limit specified for a cache
    BlobTooLargeForCache,
    /// A blob which does not exist was requested.
    BlobNotFound,
    // InvalidHashProduced,
    // IndexHasInvalidMagic,
    // IndexHasUnsupportedVersion,
    // IndexHasUnsupportedType,
    // IndexCorruptTable,
    // VarintIncomplete,
    /// A filter was requested which does not exist
    FilterUnknown(FilterID),
    /// An otherwise not distinguished error occurred with S3
    OtherS3Error(String),
    // CorruptFilterID,
    /// An otherwise not distinguished error occurred with database access
    DatabaseError(String),
    /// Invalid data was provided for a sha256
    Sha256Corrupt,
    /// An error occurred trying to collect and build the trigram set for a file
    UnableToBuildTrigrams,
    /// An otherwise not distinguished error occurred serializing data
    Serialization(String),
    /// A tokio error occurred trying to join a task
    JoinError,
    /// An otherwise not distinguished IO error
    IOError(String),
    /// An error occurred within the tokio channels
    ChannelError(String),
    /// An access control could not be parsed from the given string
    CouldNotParseAccessString(String, String),
    /// An access control could not be parsed from the given string because part of the string couldn't be consumed
    CouldNotParseAccessStringTrailing(String, String),
    /// Yara signature error
    YaraRuleError(String),
}

impl std::fmt::Display for ErrorKinds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{self:?}"))
    }
}

impl std::error::Error for ErrorKinds {

}

impl From<sqlx::Error> for ErrorKinds {
    fn from(value: sqlx::Error) -> Self {
        Self::DatabaseError(format!("{value:?}"))
    }
}

impl From<aws_smithy_client::SdkError<aws_sdk_s3::error::GetObjectError>> for ErrorKinds {
    fn from(value: aws_smithy_client::SdkError<aws_sdk_s3::error::GetObjectError>) -> Self {
        let error = value.into_service_error();
        if error.is_no_such_key() {
            ErrorKinds::BlobNotFound
        } else {
            ErrorKinds::OtherS3Error(error.to_string())
        }
    }
}

impl From<postcard::Error> for ErrorKinds {
    fn from(value: postcard::Error) -> Self {
        ErrorKinds::Serialization(value.to_string())
    }
}

impl From<tokio::task::JoinError> for ErrorKinds {
    fn from(_value: tokio::task::JoinError) -> Self {
        Self::JoinError
    }
}

impl From<std::io::Error> for ErrorKinds {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value.to_string())
    }
}

impl From<tempfile::PersistError> for ErrorKinds {
    fn from(value: tempfile::PersistError) -> Self {
        Self::IOError(value.to_string())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for ErrorKinds {
    fn from(value: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::ChannelError(value.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ErrorKinds {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::ChannelError(value.to_string())
    }
}

impl From<boreal_parser::error::Error> for ErrorKinds {
    fn from(value: boreal_parser::error::Error) -> Self {
        Self::YaraRuleError(value.to_diagnostic().message)
    }
}

impl poem::error::ResponseError for ErrorKinds {
    fn status(&self) -> http::StatusCode {
        http::StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// A result that always uses the crate's error type
pub type Result<T> = std::result::Result<T, ErrorKinds>;