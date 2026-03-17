use futures_util::TryStreamExt;
use libchronicle::chronicle::{Chronicle, ChronicleOptions};
use libchronicle::{Event, FetchOptions, TimelineOptions, Writer};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Arc::new(
        catalog::build_catalog(&catalog::CatalogOptions::default()).await?,
    );
    let chronicle = Chronicle::new(catalog, ChronicleOptions::new());
    let timeline = Arc::new(
        chronicle
            .open_timeline("tail-example", TimelineOptions::new().replication_factor(1))
            .await?,
    );

    // Spawn a writer that produces events every 500ms
    let writer = timeline.clone();
    let writer_task = tokio::spawn(async move {
        for i in 0..20 {
            let payload = format!("live-event-{}", i);
            match writer.record(Event::new(payload.into_bytes())).await {
                Ok(offset) => println!("[writer] recorded at offset {}", offset.0),
                Err(e) => eprintln!("[writer] error: {}", e),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    // Tail from beginning — unbounded stream, follows new events
    let reader = timeline.clone();
    let reader_task = tokio::spawn(async move {
        let mut stream = reader.fetch(FetchOptions::earliest());
        let mut count = 0;
        while let Some(event) = stream.try_next().await.unwrap() {
            println!(
                "[reader] offset={} payload={}",
                event.offset.unwrap_or(-1),
                String::from_utf8_lossy(&event.payload)
            );
            count += 1;
            if count >= 20 {
                break;
            }
        }
    });

    writer_task.await?;
    reader_task.await?;

    Ok(())
}
