#![cfg(feature = "server")]
use glider::server::{EmbedConfig, StoreConfig};
fn parse(values: &[(&str, &str)]) -> glider::Result<EmbedConfig> {
    EmbedConfig::from_lookup(
        |name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, v)| v.to_string())
        },
        &StoreConfig::Local("data".into()),
    )
}
#[test]
fn disabled_default_and_invalid_combinations() {
    assert!(matches!(parse(&[]).unwrap(), EmbedConfig::Disabled));
    assert!(matches!(
        parse(&[("GLIDER_EMBED_PROVIDER", "none")]).unwrap(),
        EmbedConfig::Disabled
    ));
    for values in [
        vec![("GLIDER_EMBED_PROVIDER", "other")],
        vec![("GLIDER_EMBED_MODEL", "model")],
        vec![
            ("GLIDER_EMBED_PROVIDER", "local"),
            ("GLIDER_EMBED_URL", "http://localhost"),
        ],
        vec![
            ("GLIDER_EMBED_PROVIDER", "local"),
            ("GLIDER_EMBED_API_KEY", "secret"),
        ],
        vec![
            ("GLIDER_EMBED_PROVIDER", "openai"),
            ("GLIDER_EMBED_CACHE_DIR", "cache"),
        ],
        vec![("GLIDER_EMBED_PROVIDER", "openai")],
        vec![
            ("GLIDER_EMBED_PROVIDER", "openai"),
            ("GLIDER_EMBED_MODEL", "m"),
        ],
        vec![
            ("GLIDER_EMBED_PROVIDER", "openai"),
            ("GLIDER_EMBED_MODEL", " "),
        ],
    ] {
        assert!(parse(&values).is_err());
    }
}
#[cfg(not(feature = "embed-local"))]
#[test]
fn local_missing_feature_is_explicit() {
    let error = parse(&[("GLIDER_EMBED_PROVIDER", "local")]).err().unwrap();
    assert!(error.to_string().contains("embed-local"));
}
#[cfg(not(feature = "embed-openai"))]
#[test]
fn openai_missing_feature_is_explicit() {
    let error = parse(&[
        ("GLIDER_EMBED_PROVIDER", "openai"),
        ("GLIDER_EMBED_MODEL", "m"),
        ("GLIDER_EMBED_URL", "http://localhost/v1"),
    ])
    .err()
    .unwrap();
    assert!(error.to_string().contains("embed-openai"));
}
#[cfg(feature = "embed-openai")]
#[test]
fn openai_url_and_secret_validation() {
    for url in [
        "ftp://localhost",
        "http://user:secret@localhost/v1",
        "http://localhost/v1?key=secret",
        "http://localhost/v1#secret",
        "secret",
    ] {
        let error = parse(&[
            ("GLIDER_EMBED_PROVIDER", "openai"),
            ("GLIDER_EMBED_MODEL", "m"),
            ("GLIDER_EMBED_URL", url),
        ])
        .err()
        .unwrap()
        .to_string();
        assert!(!error.contains("secret"));
        assert!(error.contains("GLIDER_EMBED_URL"));
    }
    let error = parse(&[
        ("GLIDER_EMBED_PROVIDER", "openai"),
        ("GLIDER_EMBED_MODEL", "m"),
        ("GLIDER_EMBED_URL", "http://localhost/v1"),
        ("GLIDER_EMBED_API_KEY", "secret\n"),
    ])
    .err()
    .unwrap()
    .to_string();
    assert!(!error.contains("secret"));
    assert!(parse(&[
        ("GLIDER_EMBED_PROVIDER", "openai"),
        ("GLIDER_EMBED_MODEL", "m"),
        ("GLIDER_EMBED_URL", "http://localhost/v1")
    ])
    .is_ok());
}
#[cfg(feature = "embed-local")]
#[test]
fn local_model_selection_is_lazy() {
    let cache = tempfile::tempdir().unwrap();
    let dir = cache.path().join("models");
    for model in [
        "BAAI/bge-small-en-v1.5",
        "BAAI/bge-base-en-v1.5",
        "intfloat/multilingual-e5-small",
        "nomic-ai/nomic-embed-text-v1.5",
    ] {
        assert!(glider::server::Embedder::new(EmbedConfig::Local {
            model: model.into(),
            cache_dir: dir.clone()
        })
        .is_ok());
        assert!(!dir.exists());
    }
    assert!(parse(&[
        ("GLIDER_EMBED_PROVIDER", "local"),
        ("GLIDER_EMBED_MODEL", "unknown")
    ])
    .is_err());
    assert!(
        matches!(parse(&[("GLIDER_EMBED_PROVIDER","local")]).unwrap(),EmbedConfig::Local {cache_dir, ..} if cache_dir == std::path::PathBuf::from("data/models"))
    );
}
