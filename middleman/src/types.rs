use serde_derive::{Deserialize, Serialize};
use strum_macros::{IntoStaticStr, VariantNames};

// XXX: Support other content types
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ContentType {
    #[serde(rename = "application/json")]
    Json,
}

impl std::fmt::Display for ContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                ContentType::Json => "application/json",
            }
        )
    }
}
