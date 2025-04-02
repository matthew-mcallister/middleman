use std::str::FromStr;

use tokio::time::Instant;

pub fn init_logging() {
    // TODO: need to configure logging by logger name
    let s = std::env::var("LOG_LEVEL").unwrap_or("warn".to_owned());
    let level = tracing::Level::from_str(&s).unwrap_or(tracing::Level::WARN);
    tracing_subscriber::fmt().with_max_level(level).init();
}

pub async fn sleep_until_next_tick(reference: Instant, period: f64) {
    let now = tokio::time::Instant::now();
    let elapsed = now.duration_since(reference);
    let offset = ((elapsed.as_secs_f64() / period).floor() + 1.0) * period;
    let offset = tokio::time::Duration::from_secs_f64(offset);
    let deadline = reference.checked_add(offset).unwrap();
    tokio::time::sleep_until(deadline).await;
}
