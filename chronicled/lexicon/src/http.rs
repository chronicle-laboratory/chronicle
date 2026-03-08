//! HTTP handler — RESTful API
//!
//! 路由:
//!   POST   /v1/subjects                                → register_subject
//!   GET    /v1/subjects                                → list_subjects
//!   GET    /v1/subjects/{subject}                      → get_subject
//!   POST   /v1/subjects/{subject}/versions             → register_schema
//!   GET    /v1/subjects/{subject}/versions/latest       → get_latest_schema
//!   GET    /v1/subjects/{subject}/versions/{version}    → get_schema_version
//!   POST   /v1/subjects/{subject}/compatibility         → check_compatibility

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::json;

use crate::service::LexiconService;
use crate::types::*;

type AppState = Arc<LexiconService>;

pub fn router(service: Arc<LexiconService>) -> Router {
    Router::new()
        .route("/v1/subjects", post(register_subject).get(list_subjects))
        .route("/v1/subjects/{subject}", get(get_subject))
        .route(
            "/v1/subjects/{subject}/versions",
            post(register_schema),
        )
        .route(
            "/v1/subjects/{subject}/versions/latest",
            get(get_latest_schema),
        )
        .route(
            "/v1/subjects/{subject}/versions/{version}",
            get(get_schema_version),
        )
        .route(
            "/v1/subjects/{subject}/compatibility",
            post(check_compatibility),
        )
        .with_state(service)
}

/// POST /v1/subjects
async fn register_subject(
    State(svc): State<AppState>,
    Json(req): Json<HttpRegisterSubjectRequest>,
) -> impl IntoResponse {
    match svc
        .register_subject(req.subject, req.wire_format, req.schema, req.compatibility)
        .await
    {
        Ok((record, meta)) => (
            StatusCode::CREATED,
            Json(json!({
                "schema": record,
                "meta": meta,
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

/// GET /v1/subjects
async fn list_subjects(State(svc): State<AppState>) -> impl IntoResponse {
    match svc.list_subjects().await {
        Ok(subjects) => (StatusCode::OK, Json(json!({ "subjects": subjects }))).into_response(),
        Err(e) => error_response(e),
    }
}

/// GET /v1/subjects/{subject}
async fn get_subject(
    State(svc): State<AppState>,
    Path(subject): Path<String>,
) -> impl IntoResponse {
    match svc.get_subject(&subject).await {
        Ok(meta) => (StatusCode::OK, Json(json!(meta))).into_response(),
        Err(e) => error_response(e),
    }
}

/// POST /v1/subjects/{subject}/versions
async fn register_schema(
    State(svc): State<AppState>,
    Path(subject): Path<String>,
    Json(req): Json<HttpRegisterSchemaRequest>,
) -> impl IntoResponse {
    match svc.register_schema(&subject, req.schema).await {
        Ok((record, is_new)) => {
            let status = if is_new {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            (
                status,
                Json(json!({
                    "schema": record,
                    "is_new_version": is_new,
                })),
            )
                .into_response()
        }
        Err(e) => error_response(e),
    }
}

/// GET /v1/subjects/{subject}/versions/latest
async fn get_latest_schema(
    State(svc): State<AppState>,
    Path(subject): Path<String>,
) -> impl IntoResponse {
    match svc.get_latest(&subject).await {
        Ok(record) => (StatusCode::OK, Json(json!(record))).into_response(),
        Err(e) => error_response(e),
    }
}

/// GET /v1/subjects/{subject}/versions/{version}
async fn get_schema_version(
    State(svc): State<AppState>,
    Path((subject, version)): Path<(String, u32)>,
) -> impl IntoResponse {
    match svc.get_version(&subject, version).await {
        Ok(record) => (StatusCode::OK, Json(json!(record))).into_response(),
        Err(e) => error_response(e),
    }
}

/// POST /v1/subjects/{subject}/compatibility
async fn check_compatibility(
    State(svc): State<AppState>,
    Path(subject): Path<String>,
    Json(req): Json<HttpCheckCompatibilityRequest>,
) -> impl IntoResponse {
    match svc.check_compatibility(&subject, req.schema).await {
        Ok((compatible, violations)) => (
            StatusCode::OK,
            Json(json!({
                "compatible": compatible,
                "violations": violations,
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

fn error_response(e: crate::error::LexiconError) -> axum::response::Response {
    use crate::error::LexiconError;

    let (status, message) = match &e {
        LexiconError::SubjectAlreadyExists(_) => (StatusCode::CONFLICT, e.to_string()),
        LexiconError::SubjectNotFound(_) | LexiconError::VersionNotFound { .. } => {
            (StatusCode::NOT_FOUND, e.to_string())
        }
        LexiconError::WireFormatMismatch { .. }
        | LexiconError::SchemaInputMismatch { .. }
        | LexiconError::InvalidSchema(_)
        | LexiconError::InvalidBasicType(_) => (StatusCode::BAD_REQUEST, e.to_string()),
        LexiconError::IncompatibleSchema { violations } => (
            StatusCode::CONFLICT,
            format!("incompatible schema: {}", violations.join("; ")),
        ),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    (status, Json(json!({ "error": message }))).into_response()
}
