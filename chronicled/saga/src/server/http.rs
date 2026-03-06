use crate::config::SagaConfig;
use crate::storage::memtable::Memtable;
use crate::query::engine;
use crate::storage::saga_catalog::SagaCatalog;
use crate::types::TopicConfig;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct AppState {
    pub catalog: Arc<SagaCatalog>,
    pub memtables: Arc<parking_lot::RwLock<HashMap<String, Arc<Memtable>>>>,
    pub config: SagaConfig,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/v1/topics", post(create_topic).get(list_topics))
        .route("/v1/query", post(execute_query))
        .route("/v1/status", get(status))
        .with_state(state)
}

#[derive(Deserialize)]
struct CreateTopicRequest {
    #[serde(flatten)]
    config: TopicConfig,
}

#[derive(Serialize)]
struct CreateTopicResponse {
    name: String,
    status: String,
}

async fn create_topic(
    State(state): State<AppState>,
    Json(req): Json<CreateTopicRequest>,
) -> impl IntoResponse {
    let name = req.config.name.clone();
    match state.catalog.register_topic(req.config) {
        Ok(()) => {
            info!(topic = %name, "topic created");
            (
                StatusCode::CREATED,
                Json(CreateTopicResponse {
                    name,
                    status: "created".into(),
                }),
            )
                .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e.to_string() }))
            .into_response(),
    }
}

#[derive(Serialize)]
struct TopicInfo {
    name: String,
    fields: usize,
}

async fn list_topics(State(state): State<AppState>) -> impl IntoResponse {
    let topics: Vec<TopicInfo> = state
        .catalog
        .list_topics()
        .into_iter()
        .map(|name| {
            let fields = state
                .catalog
                .schema(&name)
                .map(|s| s.fields().len())
                .unwrap_or(0);
            TopicInfo { name, fields }
        })
        .collect();
    Json(topics)
}

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    row_count: usize,
}

async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    match engine::execute_sql(&req.sql, &state.catalog, &state.memtables).await {
        Ok(batches) => {
            let (columns, rows) = batches_to_json(&batches);
            let row_count = rows.len();
            Json(QueryResponse {
                columns,
                rows,
                row_count,
            })
            .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e.to_string() }))
            .into_response(),
    }
}

fn batches_to_json(
    batches: &[arrow::array::RecordBatch],
) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    if batches.is_empty() {
        return (vec![], vec![]);
    }

    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let mut rows = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let val = column_value_to_json(col, row_idx);
                row.push(val);
            }
            rows.push(row);
        }
    }

    (columns, rows)
}

fn column_value_to_json(
    col: &arrow::array::ArrayRef,
    row: usize,
) -> serde_json::Value {
    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    if col.is_null(row) {
        return serde_json::Value::Null;
    }

    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            serde_json::Value::Number(arr.value(row).into())
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(row))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            serde_json::Value::Number(arr.value(row).into())
        }
        _ => serde_json::Value::String(format!("<unsupported: {:?}>", col.data_type())),
    }
}

#[derive(Serialize)]
struct StatusResponse {
    status: String,
    topics: usize,
}

async fn status(State(state): State<AppState>) -> impl IntoResponse {
    Json(StatusResponse {
        status: "ok".into(),
        topics: state.catalog.list_topics().len(),
    })
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        AppState {
            catalog: Arc::new(SagaCatalog::new()),
            memtables: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            config: SagaConfig::default(),
        }
    }

    #[tokio::test]
    async fn status_endpoint() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_topics_empty() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/topics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn create_topic_endpoint() {
        let state = test_state();
        let app = build_router(state.clone());
        let body = serde_json::json!({
            "name": "events",
            "schema": [
                {"name": "timestamp", "data_type": "TimestampMillis", "nullable": false},
                {"name": "value", "data_type": "Int64", "nullable": true}
            ],
            "sort_keys": ["timestamp"],
            "partition_granularity": "Day"
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/topics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(state.catalog.list_topics().len(), 1);
    }
}
