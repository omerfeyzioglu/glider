//! Optional HTTP-layer embedding. No engine writes or persisted state.
use super::{
    config::StoreConfig,
    error::{bad_request, ApiError},
};
use crate::Error;
#[cfg(any(feature = "embed-local", feature = "embed-openai"))]
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
#[cfg(any(feature = "embed-local", feature = "embed-openai"))]
use std::sync::Arc;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

pub const MAX_BATCH: usize = 64;
pub const MAX_TEXT_BYTES: usize = 32_768;
const MAX_TOTAL_BYTES: usize = 262_144;

/// Secrets deliberately have no Debug or Serialize implementation.
#[derive(Clone, Default)]
pub enum EmbedConfig {
    #[default]
    Disabled,
    Local {
        model: String,
        cache_dir: PathBuf,
    },
    OpenAi {
        model: String,
        base_url: String,
        api_key: Option<String>,
    },
}

impl EmbedConfig {
    /// Parse embedding settings without mutating the process environment.
    /// Validation never downloads model files.
    pub fn from_lookup(
        get: impl Fn(&str) -> Option<String>,
        store: &StoreConfig,
    ) -> crate::Result<Self> {
        let provider = get("GLIDER_EMBED_PROVIDER").unwrap_or_else(|| "none".into());
        let reject = |names: &[&str]| -> crate::Result<()> {
            for name in names {
                if get(name).is_some() {
                    return Err(Error::Invalid(format!(
                        "{name} is not valid with GLIDER_EMBED_PROVIDER={provider}"
                    )));
                }
            }
            Ok(())
        };
        if get("GLIDER_EMBED_MODEL").is_some_and(|value| value.trim().is_empty()) {
            return Err(Error::Invalid("GLIDER_EMBED_MODEL must be nonblank".into()));
        }
        let config = match provider.as_str() {
            "none" => {
                reject(&[
                    "GLIDER_EMBED_MODEL",
                    "GLIDER_EMBED_URL",
                    "GLIDER_EMBED_API_KEY",
                    "GLIDER_EMBED_CACHE_DIR",
                ])?;
                Self::Disabled
            }
            "local" => {
                reject(&["GLIDER_EMBED_URL", "GLIDER_EMBED_API_KEY"])?;
                if get("GLIDER_EMBED_CACHE_DIR").is_some_and(|value| value.is_empty()) {
                    return Err(Error::Invalid(
                        "GLIDER_EMBED_CACHE_DIR must be nonempty".into(),
                    ));
                }
                Self::Local {
                    model: get("GLIDER_EMBED_MODEL")
                        .unwrap_or_else(|| "BAAI/bge-small-en-v1.5".into()),
                    cache_dir: get("GLIDER_EMBED_CACHE_DIR")
                        .map(PathBuf::from)
                        .unwrap_or_else(|| match store {
                            StoreConfig::Local(path) if get("GLIDER_DIMENSIONS").is_none() => {
                                path.join("models")
                            }
                            StoreConfig::Local(_) | StoreConfig::S3 { .. } => PathBuf::from(
                                get("GLIDER_CACHE_DIR").unwrap_or_else(|| "glider-cache".into()),
                            )
                            .join("models"),
                        }),
                }
            }
            "openai" => {
                reject(&["GLIDER_EMBED_CACHE_DIR"])?;
                Self::OpenAi {
                    model: get("GLIDER_EMBED_MODEL").ok_or_else(|| {
                        Error::Invalid("set GLIDER_EMBED_MODEL for openai embedding".into())
                    })?,
                    base_url: get("GLIDER_EMBED_URL").ok_or_else(|| {
                        Error::Invalid("set GLIDER_EMBED_URL for openai embedding".into())
                    })?,
                    api_key: get("GLIDER_EMBED_API_KEY"),
                }
            }
            _ => {
                return Err(Error::Invalid(
                    "GLIDER_EMBED_PROVIDER must be none, local or openai".into(),
                ))
            }
        };
        // Validate model names, URLs, credentials and feature availability without downloads.
        Embedder::new(config.clone())?;
        Ok(config)
    }
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Kind {
    Query,
    #[default]
    Document,
}

#[derive(Serialize)]
pub(super) struct Embeddings {
    model: String,
    dimensions: usize,
    pub vectors: Vec<Vec<f32>>,
}

pub struct Embedder {
    backend: Backend,
    model: Option<String>,
    dimensions: AtomicUsize,
    #[cfg(any(feature = "embed-local", feature = "embed-openai"))]
    permits: Arc<tokio::sync::Semaphore>,
}
enum Backend {
    Disabled,
    #[cfg(feature = "embed-local")]
    Local {
        model: fastembed::EmbeddingModel,
        cache_dir: PathBuf,
        session: Arc<std::sync::Mutex<Option<fastembed::TextEmbedding>>>,
    },
    #[cfg(feature = "embed-openai")]
    OpenAi {
        client: reqwest::Client,
        url: reqwest::Url,
        authorization: Option<reqwest::header::HeaderValue>,
    },
}

impl Embedder {
    pub fn disabled() -> Self {
        Self {
            backend: Backend::Disabled,
            model: None,
            dimensions: AtomicUsize::new(0),
            #[cfg(any(feature = "embed-local", feature = "embed-openai"))]
            permits: Arc::new(tokio::sync::Semaphore::new(2)),
        }
    }
    pub fn new(config: EmbedConfig) -> crate::Result<Self> {
        #[cfg(any(feature = "embed-local", feature = "embed-openai"))]
        let mut this = Self::disabled();
        #[cfg(not(any(feature = "embed-local", feature = "embed-openai")))]
        let this = Self::disabled();
        match config {
            EmbedConfig::Disabled => {}
            EmbedConfig::Local { model, cache_dir } => {
                #[cfg(not(feature = "embed-local"))]
                {
                    let _ = (model, cache_dir);
                    return Err(Error::Invalid(
                        "local embedding requires cargo feature embed-local".into(),
                    ));
                }
                #[cfg(feature = "embed-local")]
                {
                    let info = local_model(&model)?;
                    this.dimensions.store(info.dim, Ordering::Relaxed);
                    this.model = Some(model);
                    this.backend = Backend::Local {
                        model: info.model,
                        cache_dir,
                        session: Arc::new(std::sync::Mutex::new(None)),
                    };
                }
            }
            EmbedConfig::OpenAi {
                model,
                base_url,
                api_key,
            } => {
                #[cfg(not(feature = "embed-openai"))]
                {
                    let _ = (model, base_url, api_key);
                    return Err(Error::Invalid(
                        "openai embedding requires cargo feature embed-openai".into(),
                    ));
                }
                #[cfg(feature = "embed-openai")]
                {
                    let invalid_url = || {
                        Error::Invalid("GLIDER_EMBED_URL must be an http(s) base URL without credentials, query or fragment".into())
                    };
                    let mut url = reqwest::Url::parse(&base_url).map_err(|_| invalid_url())?;
                    if !matches!(url.scheme(), "http" | "https")
                        || url.host_str().is_none()
                        || !url.username().is_empty()
                        || url.password().is_some()
                        || url.query().is_some()
                        || url.fragment().is_some()
                    {
                        return Err(invalid_url());
                    }
                    url.set_path(&format!("{}/embeddings", url.path().trim_end_matches('/')));
                    let authorization = api_key
                        .map(|key| {
                            let mut header =
                                reqwest::header::HeaderValue::from_str(&format!("Bearer {key}"))
                                    .map_err(|_| {
                                        Error::Invalid("invalid GLIDER_EMBED_API_KEY header".into())
                                    })?;
                            header.set_sensitive(true);
                            Ok::<_, Error>(header)
                        })
                        .transpose()?;
                    let client = reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(60))
                        .redirect(reqwest::redirect::Policy::none())
                        .build()
                        .map_err(|_| {
                            Error::Invalid("cannot initialize embedding HTTP client".into())
                        })?;
                    this.model = Some(model);
                    this.backend = Backend::OpenAi {
                        client,
                        url,
                        authorization,
                    };
                }
            }
        }
        Ok(this)
    }
    pub(super) fn status(&self) -> Value {
        let provider: Option<&str> = match &self.backend {
            Backend::Disabled => None,
            #[cfg(feature = "embed-local")]
            Backend::Local { .. } => Some("local"),
            #[cfg(feature = "embed-openai")]
            Backend::OpenAi { .. } => Some("openai"),
        };
        let Some(provider) = provider else {
            return Value::Null;
        };
        let dimensions = self.dimensions.load(Ordering::Relaxed);
        json!({"provider":provider,"model":self.model,"dimensions":if dimensions == 0 { None } else { Some(dimensions) }})
    }
    pub(super) async fn embed(
        &self,
        input: Vec<String>,
        kind: Kind,
    ) -> Result<Embeddings, ApiError> {
        if matches!(self.backend, Backend::Disabled) {
            return Err(bad_request(
                "text embedding is disabled; set GLIDER_EMBED_PROVIDER",
            ));
        }
        validate(&input)?;
        #[cfg(not(any(feature = "embed-local", feature = "embed-openai")))]
        {
            let _ = kind;
            Err(bad_request(
                "text embedding is disabled; set GLIDER_EMBED_PROVIDER",
            ))
        }
        #[cfg(any(feature = "embed-local", feature = "embed-openai"))]
        {
            // Reject overload instead of retaining unbounded pending inference work.
            let permit = self.permits.clone().try_acquire_owned().map_err(|_| {
                ApiError(
                    StatusCode::TOO_MANY_REQUESTS,
                    "embedding capacity is full".into(),
                )
            })?;
            let count = input.len();
            let vectors: Vec<Vec<f32>> = match &self.backend {
                Backend::Disabled => unreachable!(),
                #[cfg(feature = "embed-local")]
                Backend::Local {
                    model,
                    cache_dir,
                    session,
                } => {
                    let model = model.clone();
                    let cache_dir = cache_dir.clone();
                    let session = session.clone();
                    tokio::task::spawn_blocking(move || {
                    // Permit stays with inference even if the HTTP request is cancelled.
                    let _permit = permit;
                    let mut session = session.lock().map_err(|_| unavailable("local embedding session failed"))?;
                    if session.is_none() {
                        *session = Some(fastembed::TextEmbedding::try_new(fastembed::TextInitOptions::new(model.clone()).with_cache_dir(cache_dir).with_show_download_progress(false)).map_err(|_| unavailable("local embedding model initialization failed; check model cache and download access"))?);
                    }
                    let texts: Vec<String> = input.into_iter().map(|text| local_input(&model, kind, &text)).collect();
                    session.as_mut().unwrap().embed(texts, Some(32)).map_err(|_| unavailable("local embedding inference failed"))
                }).await.map_err(|_| unavailable("local embedding worker failed"))??
                }
                #[cfg(feature = "embed-openai")]
                Backend::OpenAi {
                    client,
                    url,
                    authorization,
                } => {
                    let _permit = permit;
                    // OpenAI-compatible endpoints have no standard query/document parameter.
                    let _ = kind;
                    let mut request = client
                        .post(url.clone())
                        .json(&json!({"model":self.model,"input":input,"encoding_format":"float"}));
                    if let Some(auth) = authorization {
                        request = request.header(reqwest::header::AUTHORIZATION, auth.clone());
                    }
                    let mut response = request
                        .send()
                        .await
                        .map_err(|_| unavailable("embedding endpoint request failed"))?;
                    if !response.status().is_success() {
                        return Err(unavailable(format!(
                            "embedding endpoint returned HTTP {}",
                            response.status().as_u16()
                        )));
                    }
                    // Never forward remote error bodies or reqwest errors: they can contain secrets.
                    let mut bytes = Vec::new();
                    while let Some(chunk) = response
                        .chunk()
                        .await
                        .map_err(|_| unavailable("embedding endpoint response failed"))?
                    {
                        if bytes.len() + chunk.len() > 16 * 1024 * 1024 {
                            return Err(unavailable("embedding endpoint response exceeds 16 MiB"));
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    #[derive(Deserialize)]
                    struct Row {
                        index: usize,
                        embedding: Vec<f32>,
                    }
                    #[derive(Deserialize)]
                    struct Reply {
                        data: Vec<Row>,
                    }
                    let mut reply: Reply = serde_json::from_slice(&bytes)
                        .map_err(|_| unavailable("invalid embedding endpoint response"))?;
                    reply.data.sort_by_key(|row| row.index);
                    if reply.data.len() != count
                        || reply.data.iter().enumerate().any(|(i, row)| row.index != i)
                    {
                        return Err(unavailable(
                            "embedding endpoint returned invalid indices or batch size",
                        ));
                    }
                    reply.data.into_iter().map(|row| row.embedding).collect()
                }
            };
            let dimensions = validate_vectors(&vectors, count)?;
            self.dimensions.store(dimensions, Ordering::Relaxed);
            Ok(Embeddings {
                model: self.model.clone().unwrap(),
                dimensions,
                vectors,
            })
        }
    }
}
#[cfg(any(feature = "embed-local", feature = "embed-openai"))]
fn unavailable(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::SERVICE_UNAVAILABLE, message.into())
}
fn validate(input: &[String]) -> Result<(), ApiError> {
    if input.is_empty() || input.len() > MAX_BATCH {
        return Err(bad_request("input must contain between 1 and 64 texts"));
    }
    if input
        .iter()
        .any(|text| text.trim().is_empty() || text.len() > MAX_TEXT_BYTES)
    {
        return Err(bad_request(
            "each text must be nonblank and at most 32768 UTF-8 bytes",
        ));
    }
    if input.iter().map(String::len).sum::<usize>() > MAX_TOTAL_BYTES {
        return Err(bad_request("input exceeds 262144 UTF-8 bytes"));
    }
    Ok(())
}
#[cfg(any(feature = "embed-local", feature = "embed-openai"))]
fn validate_vectors(vectors: &[Vec<f32>], count: usize) -> Result<usize, ApiError> {
    let dimensions = vectors.first().map_or(0, Vec::len);
    if vectors.len() != count
        || dimensions == 0
        || dimensions > 65_536
        || vectors.iter().any(|vector| {
            vector.len() != dimensions
                || vector.iter().any(|v| !v.is_finite())
                || vector.iter().all(|v| *v == 0.0)
        })
    {
        return Err(unavailable("embedding provider returned invalid vectors"));
    }
    Ok(dimensions)
}

#[cfg(feature = "embed-local")]
fn local_model(name: &str) -> crate::Result<fastembed::ModelInfo<fastembed::EmbeddingModel>> {
    // The exported ONNX repository can differ from the original model name.
    let alias = match name {
        "BAAI/bge-small-en-v1.5" => Some("BGESmallENV15"),
        "BAAI/bge-base-en-v1.5" => Some("BGEBaseENV15"),
        "BAAI/bge-large-en-v1.5" => Some("BGELargeENV15"),
        "BAAI/bge-small-zh-v1.5" => Some("BGESmallZHV15"),
        "BAAI/bge-large-zh-v1.5" => Some("BGELargeZHV15"),
        "BAAI/bge-m3" => Some("BGEM3"),
        "nomic-ai/nomic-embed-text-v1" => Some("NomicEmbedTextV1"),
        "nomic-ai/nomic-embed-text-v1.5" => Some("NomicEmbedTextV15"),
        "intfloat/multilingual-e5-small" => Some("MultilingualE5Small"),
        "intfloat/multilingual-e5-base" => Some("MultilingualE5Base"),
        "intfloat/multilingual-e5-large" => Some("MultilingualE5Large"),
        "sentence-transformers/all-MiniLM-L6-v2" => Some("AllMiniLML6V2"),
        "sentence-transformers/all-MiniLM-L12-v2" => Some("AllMiniLML12V2"),
        "sentence-transformers/all-mpnet-base-v2" => Some("AllMpnetBaseV2"),
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2" => {
            Some("ParaphraseMLMiniLML12V2")
        }
        "sentence-transformers/paraphrase-multilingual-mpnet-base-v2" => {
            Some("ParaphraseMLMpnetBaseV2")
        }
        "mixedbread-ai/mxbai-embed-large-v1" => Some("MxbaiEmbedLargeV1"),
        "Alibaba-NLP/gte-base-en-v1.5" => Some("GTEBaseENV15"),
        "Alibaba-NLP/gte-large-en-v1.5" => Some("GTELargeENV15"),
        _ => None,
    };
    fastembed::TextEmbedding::list_supported_models().into_iter().find(|info| info.model_code == name || format!("{:?}",info.model) == alias.unwrap_or(name)).ok_or_else(|| Error::Invalid("unsupported GLIDER_EMBED_MODEL; use a fastembed model name or ONNX repository name".into()))
}
#[cfg(feature = "embed-local")]
fn local_input(model: &fastembed::EmbeddingModel, kind: Kind, text: &str) -> String {
    let name = format!("{model:?}");
    let prefix = if name.starts_with("MultilingualE5") {
        match kind {
            Kind::Query => "query: ",
            Kind::Document => "passage: ",
        }
    } else if name.starts_with("NomicEmbedText") || name == "ModernBertEmbedLarge" {
        match kind {
            Kind::Query => "search_query: ",
            Kind::Document => "search_document: ",
        }
    } else if name.starts_with("EmbeddingGemma") {
        match kind {
            Kind::Query => "task: search result | query: ",
            Kind::Document => "title: none | text: ",
        }
    } else if matches!(kind, Kind::Query) {
        if name.starts_with("BGE") && name.contains("ENV15")
            || name.starts_with("Mxbai")
            || name.starts_with("SnowflakeArctic")
        {
            "Represent this sentence for searching relevant passages: "
        } else if name.starts_with("BGE") && name.contains("ZHV15") {
            "为这个句子生成表示以用于检索相关文章："
        } else {
            ""
        }
    } else {
        ""
    };
    format!("{prefix}{text}")
}
