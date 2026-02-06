use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    NotFound,
    PermissionDenied,
    Conflict,
    InvalidArgument,
    Internal,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl ApiError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::PermissionDenied(_) => ErrorCode::PermissionDenied,
            Self::Conflict(_) => ErrorCode::Conflict,
            Self::InvalidArgument(_) => ErrorCode::InvalidArgument,
            Self::Internal(_) => ErrorCode::Internal,
        }
    }
}
