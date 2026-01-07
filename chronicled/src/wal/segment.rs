use std::io::Error;
use tokio::fs::File;

pub struct Segment {
    file: File,
}

impl Segment {
    pub async fn sync(&self) -> Result<(), Error> {
        self.file.sync_data().await
    }

}
