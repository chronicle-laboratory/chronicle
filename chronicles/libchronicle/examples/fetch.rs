use futures_util::TryStreamExt;
use libchronicle::chronicle::{Chronicle, ChronicleOptions};
use libchronicle::{Event, FetchOptions, TimelineOptions, Writer};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Arc::new(
        catalog::build_catalog(&catalog::CatalogOptions::default()).await?,
    );
    let chronicle = Chronicle::new(catalog, ChronicleOptions::new());
    let timeline = chronicle
        .open_timeline("fetch-example", TimelineOptions::new().replication_factor(1))
        .await?;

    // Write some events first
    for i in 0..10 {
        let payload = format!("event-{}", i);
        timeline.record(Event::new(payload.into_bytes())).await?;
    }

    // Fetch from beginning — bounded by limit
    println!("--- fetch earliest, limit 5 ---");
    let mut stream = timeline.fetch(FetchOptions::earliest().limit(5));
    while let Some(event) = stream.try_next().await? {
        println!(
            "offset={} payload={}",
            event.offset.unwrap_or(-1),
            String::from_utf8_lossy(&event.payload)
        );
    }

    // Fetch from specific offset
    println!("--- fetch from offset 5, limit 3 ---");
    let mut stream = timeline.fetch(FetchOptions::offset(5).limit(3));
    while let Some(event) = stream.try_next().await? {
        println!(
            "offset={} payload={}",
            event.offset.unwrap_or(-1),
            String::from_utf8_lossy(&event.payload)
        );
    }

    // Fetch from latest — gets only new events
    println!("--- fetch latest (will get new events only) ---");
    let mut stream = timeline.fetch(
        FetchOptions::latest()
            .limit(2)
            .timeout(std::time::Duration::from_secs(3)),
    );

    // Write more events while fetching
    timeline.record(Event::new(b"new-event-1".to_vec())).await?;
    timeline.record(Event::new(b"new-event-2".to_vec())).await?;

    while let Some(event) = stream.try_next().await? {
        println!(
            "offset={} payload={}",
            event.offset.unwrap_or(-1),
            String::from_utf8_lossy(&event.payload)
        );
    }

    timeline.close().await;
    Ok(())
}
