//! `glider-server`: serve one Glider collection over HTTP.
//! Configuration comes from environment variables; see
//! `glider::server::ServerConfig::from_env`.

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let config = match glider::server::ServerConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("glider-server: {error}");
            std::process::exit(2);
        }
    };
    eprintln!("glider-server: listening on {}", config.listen);
    if let Err(error) = glider::server::run(config).await {
        eprintln!("glider-server: {error}");
        std::process::exit(1);
    }
}
