use std::sync::atomic::{AtomicU64, Ordering};

use koharu_pipeline::{Pipeline, ResourceSnapshot};
use koharu_rasterizer::Rasterizer;
use koharu_renderer::Renderer;
use tokio::sync::{Semaphore, SemaphorePermit, watch};

use crate::error::ApiError;

/// Shared service state. The pipeline's internal mutex is the single-worker
/// FIFO queue; `queue` only bounds how many requests wait before rejection.
/// `resources` is the pipeline's resource watch channel, started once here.
pub struct ServerState {
    pub pipeline: Pipeline,
    pub renderer: Renderer,
    pub rasterizer: Rasterizer,
    pub queue: Semaphore,
    pub resources: watch::Receiver<ResourceSnapshot>,
    pub received: AtomicU64,
    pub completed: AtomicU64,
    pub rejected: AtomicU64,
}

impl ServerState {
    pub fn new(
        pipeline: Pipeline,
        renderer: Renderer,
        rasterizer: Rasterizer,
        max_queued: usize,
    ) -> Self {
        let resources = pipeline.subscribe_resources();
        Self {
            pipeline,
            renderer,
            rasterizer,
            queue: Semaphore::new(max_queued),
            resources,
            received: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }

    /// Counts an admitted request; called when a handler finishes.
    pub fn finish(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}

/// Admits a request or rejects it with 503 once all `max_queued` permits are
/// held. The permit is held for the whole request: queue wait + inference.
pub fn acquire(queue: &Semaphore) -> Result<SemaphorePermit<'_>, ApiError> {
    queue.try_acquire().map_err(|_| ApiError::QueueFull)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_gate_rejects_beyond_limit_and_releases_on_drop() {
        let queue = tokio::sync::Semaphore::new(1);
        let first = acquire(&queue).expect("first permit is available");
        assert!(matches!(acquire(&queue), Err(ApiError::QueueFull)));
        drop(first);
        assert!(acquire(&queue).is_ok());
    }
}
