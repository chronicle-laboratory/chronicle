use crate::conn::ConnPool;
use crate::cursor::EventStream;
use crate::error::ChronicleError;
use crate::state_machine::StateMachine;
use crate::write_group::WriteGroup;
use crate::{Event as UserEvent, FetchOptions, Offset, StartPosition, TimelineOptions, Writer};
use catalog::Catalog;
use std::sync::Arc;
use tracing::info;

pub struct Timeline {
    state_machine: StateMachine,
    #[allow(dead_code)]
    options: TimelineOptions,
    pool: Arc<ConnPool>,
    write_group: WriteGroup,
}

impl Timeline {
    pub async fn open(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
        options: TimelineOptions,
    ) -> Result<Self, ChronicleError> {
        let state_machine = StateMachine::open(
            &catalog,
            &pool,
            name,
            options.replication_factor,
            options.schema_id.clone(),
        )
        .await?;

        let write_group = WriteGroup::start(
            state_machine.shared(),
            options.max_batch_size,
            options.linger,
        );

        Ok(Self {
            state_machine,
            options,
            pool,
            write_group,
        })
    }

    pub fn fetch(&self, opts: FetchOptions) -> EventStream {
        let start_offset = match opts.start {
            StartPosition::Earliest => 1,
            StartPosition::Latest => self.state_machine.lra() + 1,
            StartPosition::Offset(o) => o,
            StartPosition::Index { .. } => 1,
        };

        let mut conns = std::collections::HashMap::new();
        for seg in &self.state_machine.segments {
            for ep in &seg.ensemble {
                if !conns.contains_key(ep) {
                    if let Ok(conn) = self.pool.get_or_connect(ep) {
                        conns.insert(ep.clone(), conn);
                    }
                }
            }
        }

        let mut stream = EventStream::new(
            self.state_machine.timeline_id(),
            self.state_machine.segments.clone(),
            &conns,
            start_offset,
        );

        if let Some(limit) = opts.limit {
            stream = stream.with_limit(limit);
        }
        if let Some(timeout) = opts.timeout {
            stream = stream.with_timeout(timeout);
        }
        if opts.limit.is_none() && opts.timeout.is_none() {
            stream = stream.with_tail();
        }

        stream
    }

    pub async fn close(&self) {
        self.write_group.close().await;
        self.state_machine.close().await;
        info!(timeline_id = self.state_machine.timeline_id(), "timeline closed");
    }
}

#[async_trait::async_trait]
impl Writer for Timeline {
    async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        self.write_group.record(event).await
    }
}
