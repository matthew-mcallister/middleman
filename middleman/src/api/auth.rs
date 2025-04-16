use axum::extract::FromRequestParts;
use http::request::Parts;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;

use jsonwebtoken::{decode, DecodingKey, Validation};
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::Application;

use super::consumer::ConsumerApi;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Claims {
    pub(crate) sub: String,
}

/// JWT-based auth token validation.
pub(crate) struct TokenValidator {
    validation: Validation,
    decoding_key: DecodingKey,
}

impl std::fmt::Debug for TokenValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenValidator")
            .field("validation", &self.validation)
            .field("decoding_key", &"..")
            .finish()
    }
}

impl TokenValidator {
    pub(crate) fn new(app: &Application) -> Result<Self> {
        let secret = app
            .config
            .consumer_auth_secret
            .as_ref()
            .ok_or(Error::with_cause(ErrorKind::Unexpected, "Missing consumer auth secret"))?
            .as_bytes();
        let mut required_spec_claims = HashSet::new();
        required_spec_claims.insert("sub".to_owned());
        let mut validation = Validation::default();
        validation.required_spec_claims = required_spec_claims;

        Ok(Self {
            decoding_key: DecodingKey::from_secret(secret),
            validation,
        })
    }

    /// Returns the tag for which the requester is authenticated
    // TODO?: subject with multiple tags
    pub(crate) fn authorize_request(&self, parts: &Parts) -> Result<Uuid> {
        let auth_header_value = parts
            .headers
            .get("Authorization")
            .ok_or(ErrorKind::Unauthenticated)?
            .to_str()?;
        if !auth_header_value.starts_with("Bearer ") {
            Err(ErrorKind::Unauthenticated)?;
        }
        let encoded = &auth_header_value["Bearer ".len()..];
        let claims: Claims = decode(&encoded, &self.decoding_key, &self.validation)?.claims;
        let tag = Uuid::from_str(&claims.sub).map_err(|_| ErrorKind::Unauthenticated)?;
        Ok(tag)
    }
}

/// Extractor that validates HTTP auth
#[derive(Debug)]
pub(crate) struct ConsumerAuth(pub Uuid);

impl FromRequestParts<Arc<ConsumerApi>> for ConsumerAuth {
    type Rejection = Box<Error>;

    fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<ConsumerApi>,
    ) -> impl Future<Output = Result<Self>> + Send {
        async {
            let tag = state.token_validator.authorize_request(parts)?;
            Ok(Self(tag))
        }
    }
}
