pub fn init_logging() {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
    env_logger::Builder::from_env(env).init();
}
