use regex::Regex;
use url::Url;

use crate::delivery::Delivery;
use crate::error::DynResult;
use crate::event::Event;
use crate::types::{DbTransaction, Prefix, Tag};
use crate::util::ByteCast;
use crate::{impl_byte_cast_unsized, make_dst, make_key};

/// Subscriber that receives events.
///
/// # Layout
///
/// ```ignore
///   [fixed header]
///   [0 destination_url]
///   [1 stream_regex]
/// ```
#[derive(Debug)]
#[repr(C)]
pub struct SubscriberT<T: ?Sized> {
    tag: Tag,
    _flags: u32,
    _reserved: u32,
    // Allows adding extensions in the future
    _next_header: u32,
    offsets: [u32; 1],
    content: T,
}
impl_byte_cast_unsized!(SubscriberT, content);

pub type Subscriber = SubscriberT<[u8]>;

impl Subscriber {
    pub fn destination_url(&self) -> &str {
        let bytes = &self.content[..self.offsets[0] as _];
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    pub fn stream_regex(&self) -> &str {
        // XXX: Cache the compiled regex
        let bytes = &self.content[self.offsets[0] as _..];
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    /// Iterates over all subscribers of the given stream.
    pub fn iter_stream_subscribers<'a>(
        txn: &'a DbTransaction,
        tag: Tag,
        stream: &'a str,
    ) -> impl Iterator<Item = Result<(u128, Box<Self>), rocksdb::Error>> + 'a {
        let prefix = make_key!(u8: Prefix::Subscriber as u8, u128: tag);
        txn.prefix_iterator(&prefix).filter_map(|item| {
            let (key, bytes) = match item {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            let subscriber: Box<Subscriber> = unsafe { ByteCast::from_bytes_owned(&*bytes) };

            let regex = Regex::new(subscriber.stream_regex()).unwrap();
            if !regex.is_match(stream) {
                return None;
            }

            let key = u128::from_ne_bytes(<[u8; 16]>::try_from(&key[17..]).unwrap());
            Some(Ok((key, subscriber)))
        })
    }

    pub fn create_deliveries_for_event(
        txn: &DbTransaction,
        event_id: u64,
        event: &Event,
    ) -> DynResult<()> {
        let subscribers = Subscriber::iter_stream_subscribers(txn, event.tag(), event.stream());
        for item in subscribers {
            let (id, _) = item?;
            Delivery::create(txn, id, event_id)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SubscriberBuilder {
    tag: u128,
    destination_url: Option<Url>,
    stream_regex: Option<Regex>,
}

impl SubscriberBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn tag(&mut self, tag: u128) -> &mut Self {
        self.tag = tag;
        self
    }

    pub fn destination_url(&mut self, destination_url: Url) -> &mut Self {
        self.destination_url = Some(destination_url);
        self
    }

    pub fn stream_regex(&mut self, regex: Regex) -> &mut Self {
        self.stream_regex = Some(regex);
        self
    }

    pub fn build(&mut self) -> DynResult<Box<Subscriber>> {
        let destination_url = self
            .destination_url
            .take()
            .ok_or("Missing subscriber URL")?;
        if destination_url.scheme() != "http" && destination_url.scheme() != "https" {
            return Err("Invalid subscriber URL".into());
        }

        let stream_regex = self.stream_regex.take().ok_or("Missing subscriber regex")?;

        unsafe {
            Ok(make_dst!(SubscriberT[u8] {
                tag: self.tag,
                _reserved: 0,
                _flags: 0,
                _next_header: 0,
                offsets: [destination_url.as_ref().len() as _],
                [content]: (
                    destination_url.as_ref().as_bytes(),
                    stream_regex.as_str().as_bytes(),
                ),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;
    use url::Url;

    use crate::subscriber::SubscriberT;
    use crate::util::ByteCast;

    use super::SubscriberBuilder;

    #[test]
    fn test_build_subscriber() {
        let url = "https://example.com/webhook";
        let regex = "^hello";
        let subscriber = SubscriberBuilder::new()
            .tag(1)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new(regex).unwrap())
            .build()
            .unwrap();
        assert_eq!(subscriber.stream_regex(), regex);
        assert_eq!(subscriber.destination_url(), url);
        let bytes = unsafe { ByteCast::as_bytes(&*subscriber) };
        assert_eq!(
            bytes.len(),
            std::mem::size_of::<SubscriberT<()>>() + regex.len() + url.len()
        );
    }
}
