use chronicle::chronicle::{Chronicle, ChronicleOptions};
use chronicle::parallel_timeline::ParallelTimelineOptions;
use chronicle::parallel_timeline_reader::{ParallelTimelineReaderOptions, ReadType};
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

    let reader_1 = parallel_timeline
        .open_reader(
            String::from("reader-1"),
            ParallelTimelineReaderOptions {
                cursor_name: Some(String::from("cursor-1")),
                read_type: ReadType::Offset,
                secondary_offset_name: None,
            },
        )
        .await
        .unwrap();

    let reader_2 = parallel_timeline
        .open_reader(
            String::from("reader-2"),
            ParallelTimelineReaderOptions {
                cursor_name: Some(String::from("cursor-1")),
                read_type: ReadType::Offset,
                secondary_offset_name: None,
            },
        )
        .await
        .unwrap();

    parallel_timeline.append(vec![]).await.unwrap();
    parallel_timeline_shadow.append(vec![]).await.unwrap();

    let (read_offset, entry) = reader_1.fetch_next().await.unwrap();
    reader_1.acknowledge(read_offset).await.unwrap();

    let (read_offset, entry) = reader_2.fetch_next().await.unwrap();
    reader_2.acknowledge(read_offset).await.unwrap();
}