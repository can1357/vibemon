use std::{error::Error as StdError, fmt, io};

#[derive(Debug)]
pub struct CliError {
	message: String,
}

impl CliError {
	pub fn new(message: impl Into<String>) -> Self {
		Self { message: message.into() }
	}
}

impl fmt::Display for CliError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.message)
	}
}

impl StdError for CliError {}

impl From<io::Error> for CliError {
	fn from(error: io::Error) -> Self {
		Self::new(error.to_string())
	}
}

impl From<serde_json::Error> for CliError {
	fn from(error: serde_json::Error) -> Self {
		Self::new(error.to_string())
	}
}

impl From<vmond::EngineError> for CliError {
	fn from(error: vmond::EngineError) -> Self {
		Self::new(error.to_string())
	}
}

pub type Result<T> = std::result::Result<T, CliError>;

pub fn err<T>(message: impl Into<String>) -> Result<T> {
	Err(CliError::new(message))
}
