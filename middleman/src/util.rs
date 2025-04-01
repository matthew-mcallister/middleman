use std::str::FromStr;

pub fn init_logging() {
    // TODO: need to configure logging by logger name
    let s = std::env::var("LOG_LEVEL").unwrap_or("warn".to_owned());
    let level = tracing::Level::from_str(&s).unwrap_or(tracing::Level::WARN);
    tracing_subscriber::fmt().with_max_level(level).init();
}
