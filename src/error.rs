use crate::types::FilterID;



#[derive(Debug, PartialEq)]
pub enum ErrorKinds {
    BlobTooLargeForCache,
    BlobNotFound,
    // InvalidHashProduced,
    // IndexHasInvalidMagic,
    // IndexHasUnsupportedVersion,
    // IndexHasUnsupportedType,
    // IndexCorruptTable,
    // VarintIncomplete,
    FilterUnknown(FilterID),
    OtherS3Error(String),
    // CorruptFilterID,
    DatabaseError,
    Sha256Corrupt,
    UnableToBuildTrigrams,
    Serialization(String),
}

impl std::fmt::Display for ErrorKinds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{self:?}"))
    }
}

impl std::error::Error for ErrorKinds {

}

impl From<sqlx::Error> for ErrorKinds {
    fn from(_value: sqlx::Error) -> Self {
        Self::DatabaseError
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