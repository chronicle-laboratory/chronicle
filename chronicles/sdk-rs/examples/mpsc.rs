use chronicle::chronicle::{Chronicle, ChronicleOptions};
use chronicle::parallel_timeline::ParallelTimelineOptions;
use chronicle::parallel_timeline_reader::{ParallelTimelineReaderOptions, ReaderType};
use chronicle::{Acknowledgeable, Appendable, Fetchable};

#[tokio::main]
async fn main() {
    let chronicle = Chronicle::new(ChronicleOptions {
        catalog_address: String::from("127.0.0.1:7654"),
    });

    let parallel_timeline = chronicle
        .open_parallel_timeline(
            String::from("random"),
            String::from("parallel-1"),
            ParallelTimelineOptions { key_compact: false },
        )
        .await
        .unwrap();

    let parallel_timeline_shadow = chronicle
        .open_parallel_timeline(
            String::from("random"),
            String::from("parallel-2"),
            ParallelTimelineOptions { key_compact: false },
        )
        .await
        .unwrap();

    let reader = parallel_timeline
        .open_reader(
            String::from("reader"),
            ParallelTimelineReaderOptions {
                cursor_name: Some(String::from("cursor-1")),
                reader_type: ReaderType::Full,
                secondary_offset_name: None,
            },
        )
        .await
        .unwrap();

    parallel_timeline.append(vec![]).await.unwrap();
    parallel_timeline_shadow.append(vec![]).await.unwrap();

    let (read_offset, entry) = reader.fetch_next().await.unwrap();
    reader.acknowledge(read_offset).await.unwrap();
}
