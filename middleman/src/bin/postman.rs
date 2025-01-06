use std::error::Error;

use middleman::config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _config = config::load_config();
    println!("Hello, world!");
    Ok(())
}
