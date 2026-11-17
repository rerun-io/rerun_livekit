//! Reports how old messages are when they reach the operator side.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use re_protos::common::v1alpha1::StoreId;
use re_protos::sdk_comms::v1alpha1::WriteMessagesRequest;
use rerun::log::{Chunk, RowId};
use rerun::{EntityPath, Scalars, TimePoint};
use tokio::sync::mpsc;

/// How often to log a summary.
const REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// How often to send the points of a latency plot to the viewer.
const PLOT_INTERVAL: Duration = Duration::from_secs(1);

/// Entity under which latency plots go, in the recording they measure.
///
/// These are the only entities the bridge adds to a recording.
const PLOT_ROOT: &str = "livekit/latency_ms";

/// Whether to plot latency in the viewer, next to the data it measures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LatencyPlots {
    On,
    Off,
}

/// Measures the time from the robot's log call to arrival here, for one lane or track.
///
/// The two ends are different machines, so the result includes the difference of their clocks.
pub struct LatencyReport {
    name: String,
    started: Instant,
    num_messages: u32,
    sum: Duration,
    max: Duration,

    /// Messages that arrived before they were created, going by the two clocks.
    num_from_the_future: u32,
    plot: Option<Plot>,
}

impl LatencyReport {
    /// Reports to the log under `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            started: Instant::now(),
            num_messages: 0,
            sum: Duration::ZERO,
            max: Duration::ZERO,
            num_from_the_future: 0,
            plot: None,
        }
    }

    /// If `plots` is on, also plots every measurement in the viewer, as scalars at `series`
    /// below the plot root.
    pub fn with_plots(
        mut self,
        plots: LatencyPlots,
        series: &EntityPath,
        viewer_tx: &mpsc::Sender<WriteMessagesRequest>,
    ) -> Self {
        self.plot = match plots {
            LatencyPlots::On => Some(Plot {
                entity_path: EntityPath::from(PLOT_ROOT).join(series),
                viewer_tx: viewer_tx.clone(),
                store_id: None,
                points: Vec::new(),
                flushed: Instant::now(),
            }),
            LatencyPlots::Off => None,
        };
        self
    }

    /// Records the arrival, now, of a message of the recording `store_id` that was created
    /// at `created_nanos` since the Unix epoch.
    ///
    /// `timepoint` returns the message's own times, which is where its point goes in the plot,
    /// so that the plot lines up with the data on every timeline. It is only called when plotting.
    pub fn record(
        &mut self,
        created_nanos: u64,
        store_id: &StoreId,
        timepoint: impl FnOnce() -> Option<TimePoint>,
    ) {
        let created = UNIX_EPOCH + Duration::from_nanos(created_nanos);
        match SystemTime::now().duration_since(created) {
            Ok(latency) => {
                self.num_messages += 1;
                self.sum += latency;
                self.max = self.max.max(latency);
                if let Some(plot) = &mut self.plot
                    && let Some(timepoint) = timepoint()
                {
                    plot.push(store_id, Point { timepoint, latency });
                }
            }
            // The robot's clock is ahead of this one. Any number would be wrong.
            Err(_) => self.num_from_the_future += 1,
        }

        if REPORT_INTERVAL <= self.started.elapsed() {
            self.report_and_reset();
        }
    }

    fn report_and_reset(&mut self) {
        let Self {
            name,
            started,
            num_messages,
            sum,
            max,
            num_from_the_future,
            plot: _,
        } = self;
        if 0 < *num_messages {
            let mean = *sum / *num_messages;
            log::info!("{name}: {num_messages} messages, latency mean {mean:.1?}, max {max:.1?}");
        }
        if 0 < *num_from_the_future {
            log::warn!(
                "{name}: {num_from_the_future} messages arrived before they were sent. \
                 The clocks of the two machines differ, so latency cannot be measured"
            );
        }
        *started = Instant::now();
        *num_messages = 0;
        *sum = Duration::ZERO;
        *max = Duration::ZERO;
        *num_from_the_future = 0;
    }
}

struct Point {
    /// The times of the message this point measures.
    timepoint: TimePoint,
    latency: Duration,
}

/// Collects points and sends them to the viewer, one chunk per [`PLOT_INTERVAL`].
struct Plot {
    entity_path: EntityPath,
    viewer_tx: mpsc::Sender<WriteMessagesRequest>,

    /// The recording the collected points belong to.
    store_id: Option<StoreId>,

    /// Points of one recording that all have the same timelines, so that they fit in one chunk.
    points: Vec<Point>,
    flushed: Instant,
}

impl Plot {
    fn push(&mut self, store_id: &StoreId, point: Point) {
        let fits = self.store_id.as_ref() == Some(store_id)
            && self.points.last().is_none_or(|last| {
                last.timepoint
                    .timeline_names()
                    .eq(point.timepoint.timeline_names())
            });
        if !fits {
            self.flush();
            self.store_id = Some(store_id.clone());
        }
        self.points.push(point);
        if PLOT_INTERVAL <= self.flushed.elapsed() {
            self.flush();
        }
    }

    fn flush(&mut self) {
        self.flushed = Instant::now();
        let points = std::mem::take(&mut self.points);
        let Some(store_id) = &self.store_id else {
            return;
        };
        if points.is_empty() {
            return;
        }

        let mut chunk = Chunk::builder(self.entity_path.clone());
        for Point { timepoint, latency } in points {
            let latency_millis = latency.as_secs_f64() * 1e3;
            chunk = chunk.with_archetype(RowId::new(), timepoint, &Scalars::single(latency_millis));
        }
        let log_msg = chunk
            .build()
            .map_err(anyhow::Error::from)
            .and_then(|chunk| crate::proxy::encode_chunk(store_id.clone(), &chunk));
        match log_msg {
            // A full queue means the viewer is behind. The plot matters less than the data.
            Ok(log_msg) => {
                self.viewer_tx
                    .try_send(WriteMessagesRequest {
                        log_msg: Some(log_msg),
                    })
                    .ok();
            }
            Err(err) => log::warn!("Failed to plot latency: {err:#}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_from_the_future_is_not_a_fast_message() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let now_nanos = u64::try_from(now.as_nanos()).unwrap();
        let hour_nanos = 3_600_000_000_000;
        let mut report = LatencyReport::new("test");

        report.record(now_nanos - hour_nanos, &StoreId::default(), || None);
        report.record(now_nanos + hour_nanos, &StoreId::default(), || None);

        assert_eq!(report.num_messages, 1);
        assert_eq!(report.num_from_the_future, 1);
        assert!(Duration::from_secs(3599) < report.max);
    }
}
