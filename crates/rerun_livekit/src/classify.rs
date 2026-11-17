//! Decides how each log message travels to the room.

use anyhow::Context as _;
use re_protos::common::v1alpha1::{StoreId, StoreKind};
use re_protos::log_msg::v1alpha1::LogMsg;
use re_protos::log_msg::v1alpha1::log_msg::Msg;
use rerun::external::re_log_encoding::ToApplication as _;
use rerun::log::Chunk;
use rerun::{Archetype as _, ArchetypeName, ComponentIdentifier, EntityPath, archetypes};

use crate::rules::{Action, Delivery, Rules, Video};
use crate::video::is_video_candidate;

/// Archetypes whose temporal data is guaranteed by default, because losing one message
/// leaves the viewer wrong until something else corrects it.
fn is_guaranteed_by_default(archetype: ArchetypeName) -> bool {
    [
        // A lost `Clear` leaves stale data on screen.
        archetypes::Clear::name(),
        // Events: each message is the only one that says what it says.
        archetypes::StateChange::name(),
        archetypes::TextDocument::name(),
        archetypes::TextLog::name(),
        archetypes::VideoFrameReference::name(),
        // A lost sample breaks decoding until the next keyframe.
        archetypes::VideoStream::name(),
    ]
    .contains(&archetype)
}

/// How a message travels.
#[derive(Clone, Debug, PartialEq)]
pub enum Route {
    /// Never dropped, delivered in order.
    Guaranteed(Retention),

    /// Dropped when late or when the link cannot keep up.
    Latest {
        stream: StreamKey,

        /// Send at most this many messages of `stream` per second.
        max_hz: Option<f64>,
    },

    /// Image rows that travel as video frames. Dropped like [`Self::Latest`].
    Video {
        stream: StreamKey,
        max_hz: Option<f64>,
        store_id: StoreId,
        chunk: Box<Chunk>,
    },

    Drop,
}

/// Whether to keep a guaranteed message for operators who join later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retention {
    /// Store info and blueprints: kept forever.
    Persistent,

    /// Static data: kept up to a memory limit.
    Static,

    /// Temporal data: not kept.
    None,
}

/// Identifies a sequence of chunks in which a newer one supersedes an older one.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamKey {
    recording_id: String,
    entity_path: EntityPath,
    components: Vec<ComponentIdentifier>,
}

impl StreamKey {
    #[cfg(test)]
    pub fn for_test(entity_path: &str) -> Self {
        Self {
            recording_id: "test".to_owned(),
            entity_path: entity_path.into(),
            components: Vec::new(),
        }
    }
}

impl std::fmt::Display for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.entity_path.fmt(f)
    }
}

pub fn classify(log_msg: &LogMsg, rules: &Rules) -> anyhow::Result<Route> {
    let arrow_msg = match log_msg.msg.as_ref().context("Empty log message")? {
        Msg::SetStoreInfo(_) | Msg::BlueprintActivationCommand(_) => {
            return Ok(Route::Guaranteed(Retention::Persistent));
        }
        Msg::ArrowMsg(arrow_msg) => arrow_msg,
    };
    let store_id = arrow_msg
        .store_id
        .as_ref()
        .context("Chunk without a store id")?;
    if store_id.kind() == StoreKind::Blueprint {
        return Ok(Route::Guaranteed(Retention::Persistent));
    }

    let chunk = arrow_msg
        .to_application(())
        .ok()
        .and_then(|arrow_msg| Chunk::from_arrow_msg(&arrow_msg).ok())
        .context("Failed to decode a chunk")?;
    if chunk.is_static() {
        return Ok(Route::Guaranteed(Retention::Static));
    }

    let (max_hz, delivery, video) = match rules.action_for(chunk.entity_path()) {
        Action::Drop => return Ok(Route::Drop),
        Action::Forward {
            max_hz,
            delivery,
            video,
        } => (max_hz, delivery, video),
    };

    let mut components: Vec<_> = chunk.components_identifiers().collect();
    components.sort();
    let stream = StreamKey {
        recording_id: store_id.recording_id.clone(),
        entity_path: chunk.entity_path().clone(),
        components,
    };
    if video == Video::Allowed && is_video_candidate(&chunk) {
        return Ok(Route::Video {
            stream,
            max_hz,
            store_id: store_id.clone(),
            chunk: Box::new(chunk),
        });
    }

    let delivery = delivery.unwrap_or_else(|| {
        let is_guaranteed = chunk
            .components()
            .component_descriptors()
            .filter_map(|descriptor| descriptor.archetype)
            .any(is_guaranteed_by_default);
        if is_guaranteed {
            Delivery::Guaranteed
        } else {
            Delivery::Latest
        }
    });
    Ok(match delivery {
        Delivery::Guaranteed => Route::Guaranteed(Retention::None),
        Delivery::Latest => Route::Latest { stream, max_hz },
    })
}

#[cfg(test)]
mod tests {
    use rerun::external::re_log_encoding::ToTransport as _;
    use rerun::external::re_log_encoding::rrd::Compression;
    use rerun::log::RowId;
    use rerun::{AsComponents, StoreId, TimePoint, Timeline};

    use super::*;

    fn log_msg(
        kind: rerun::StoreKind,
        timepoint: TimePoint,
        archetype: &dyn AsComponents,
    ) -> LogMsg {
        let arrow_msg = Chunk::builder("/robot/arm")
            .with_archetype(RowId::new(), timepoint, archetype)
            .build()
            .unwrap()
            .to_arrow_msg()
            .unwrap();
        rerun::log::LogMsg::ArrowMsg(StoreId::random(kind, "test"), arrow_msg)
            .to_transport(Compression::LZ4)
            .unwrap()
            .into()
    }

    fn at_step_1() -> TimePoint {
        TimePoint::STATIC.with(Timeline::new_sequence("step"), 1)
    }

    fn rules(toml: &str) -> Rules {
        Rules::parse(toml).unwrap()
    }

    #[test]
    fn temporal_data_is_latest_by_default() {
        let msg = log_msg(
            rerun::StoreKind::Recording,
            at_step_1(),
            &rerun::Scalars::single(1.0),
        );
        let route = classify(&msg, &Rules::default()).unwrap();
        assert!(
            matches!(route, Route::Latest { max_hz: None, .. }),
            "{route:?}"
        );
    }

    #[test]
    fn events_are_guaranteed_by_default() {
        let msg = log_msg(
            rerun::StoreKind::Recording,
            at_step_1(),
            &rerun::TextLog::new("hi"),
        );
        assert_eq!(
            classify(&msg, &Rules::default()).unwrap(),
            Route::Guaranteed(Retention::None)
        );
    }

    #[test]
    fn static_data_and_blueprints_ignore_rules() {
        let drop_everything = rules("[[rule]]\nentities = \"/**\"\ndrop = true");
        let scalar = rerun::Scalars::single(1.0);

        let static_msg = log_msg(rerun::StoreKind::Recording, TimePoint::STATIC, &scalar);
        assert_eq!(
            classify(&static_msg, &drop_everything).unwrap(),
            Route::Guaranteed(Retention::Static)
        );

        let blueprint_msg = log_msg(rerun::StoreKind::Blueprint, at_step_1(), &scalar);
        assert_eq!(
            classify(&blueprint_msg, &drop_everything).unwrap(),
            Route::Guaranteed(Retention::Persistent)
        );

        let temporal_msg = log_msg(rerun::StoreKind::Recording, at_step_1(), &scalar);
        assert_eq!(
            classify(&temporal_msg, &drop_everything).unwrap(),
            Route::Drop
        );
    }

    #[test]
    fn images_are_video_unless_a_rule_says_otherwise() {
        let image = rerun::Image::from_color_model_and_bytes(
            vec![0; 12],
            [2, 2],
            rerun::ColorModel::RGB,
            rerun::ChannelDatatype::U8,
        );
        let msg = log_msg(rerun::StoreKind::Recording, at_step_1(), &image);

        let route = classify(&msg, &Rules::default()).unwrap();
        assert!(matches!(route, Route::Video { .. }), "{route:?}");

        let without_video = rules("[[rule]]\nentities = \"/robot/**\"\nvideo = false");
        let route = classify(&msg, &without_video).unwrap();
        assert!(matches!(route, Route::Latest { .. }), "{route:?}");

        let with_opacity = log_msg(
            rerun::StoreKind::Recording,
            at_step_1(),
            &image.with_opacity(0.5),
        );
        let route = classify(&with_opacity, &Rules::default()).unwrap();
        assert!(matches!(route, Route::Latest { .. }), "{route:?}");
    }

    #[test]
    fn rules_override_delivery_and_rate() {
        let scalar = log_msg(
            rerun::StoreKind::Recording,
            at_step_1(),
            &rerun::Scalars::single(1.0),
        );

        let guaranteed = rules("[[rule]]\nentities = \"/robot/**\"\ndelivery = \"guaranteed\"");
        assert_eq!(
            classify(&scalar, &guaranteed).unwrap(),
            Route::Guaranteed(Retention::None)
        );

        let limited = rules("[[rule]]\nentities = \"/robot/**\"\nmax_hz = 5");
        let route = classify(&scalar, &limited).unwrap();
        assert!(
            matches!(
                route,
                Route::Latest {
                    max_hz: Some(5.0),
                    ..
                }
            ),
            "{route:?}"
        );
    }
}
