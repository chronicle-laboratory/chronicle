use catalog::{CatalogOptions, build_catalog};
use chronicled::unit::unit::Unit;
use libchronicle::{Cursor, Writer};
use libchronicle::chronicle::{Chronicle, ChronicleConfig};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    // Build catalog from options (defaults to memory backend).
    let catalog = build_catalog(&CatalogOptions::default()).await?;

    // Start a Chronicle unit in-process (registers itself in the catalog).
    let _unit = Unit::new(Default::default(), catalog.clone()).await?;

    // Build client (discovers units from catalog).
    let chronicle = Chronicle::new(
        catalog,
        ChronicleConfig {
            replication_factor: 1,
            meter: None,
        },
    )
    .await?;

    // Create a timeline.
    info!("creating timeline");
    let timeline = chronicle.create_timeline("my-events").await?;
    info!(id = timeline.timeline_id(), "timeline created");

    // Record 10 events.
    info!("recording events");
    let num_events = 10i64;
    for i in 1..=num_events {
        let event = format!("hello-{}", i);
        let offset = timeline.record(event.into_bytes()).await?;
        info!(offset = offset.offset, "sent");
    }

    // Read events back.
    info!("fetching events");
    let mut cursor = chronicle.open_cursor("my-events").await?;
    while let Some((offset, payload)) = cursor.fetch().await? {
        let text = String::from_utf8_lossy(&payload);
        info!(offset = offset.offset, payload = %text, "received");
    }

    info!("done");
    Ok(())
}
