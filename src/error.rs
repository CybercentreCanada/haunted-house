

#[derive(Debug, PartialEq)]
pub enum ErrorKinds {
    BlobTooLargeForCache,
    // InvalidHashProduced,
    IndexHasInvalidMagic,
    IndexHasUnsupportedVersion,
    IndexHasUnsupportedType,
    IndexCorruptTable,
    VarintIncomplete,
    FilterUnknown,
    CorruptFilterID,
    DatabaseError,
    Sha256Corrupt
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
        Self::DatabaseError
    }
}