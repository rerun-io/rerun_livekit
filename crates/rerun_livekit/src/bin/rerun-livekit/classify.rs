//! Decides how each log message travels to the room.

use anyhow::Context as _;
use re_protos::common::v1alpha1::{StoreId, StoreKind};
use re_protos::log_msg::v1alpha1::LogMsg;
use re_protos::log_msg::v1alpha1::log_msg::Msg;
use rerun::external::re_log_encoding::ToApplication as _;
use rerun::log::Chunk;
use rerun::{Archetype as _, ArchetypeName, ComponentIdentifier, EntityPath, archetypes};

use crate::rules::{Action, Delivery, Rules, Video, glob_matches};
use crate::video::image_columns;

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
    /// Forwarded as it is. Never dropped, delivered in order.
    Guaranteed(Retention),

    /// A chunk of temporal data, which the rules may thin out, trim, or send as video.
    Temporal(Box<Temporal>),

    Drop,
}

/// A chunk of temporal data and how the rules say it travels.
#[derive(Clone, Debug, PartialEq)]
pub struct Temporal {
    pub stream: StreamKey,

    /// Send at most this many chunks of `stream` per second, unless delivery is guaranteed.
    pub max_hz: Option<f64>,

    /// How the chunk travels, apart from any images that go as video.
    pub delivery: Delivery,
    pub store_id: StoreId,

    /// The chunk without the components the rules drop.
    pub chunk: Chunk,

    /// Whether the rules dropped nothing, so that the original message can be forwarded as it is.
    pub is_whole: bool,

    /// The columns of `chunk` that may travel as video, if it has images and the rules allow it.
    pub image_columns: Option<Vec<ComponentIdentifier>>,
}

/// Whether to keep a guaranteed message for operators who join later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retention {
    /// Store info and blueprints: kept forever.
    Persistent,

    /// Static data, and temporal data the rules call latched: kept up to a memory limit.
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

    let Action::Forward {
        max_hz,
        delivery,
        video,
        drop_components,
    } = rules.action_for(chunk.entity_path())
    else {
        return Ok(Route::Drop);
    };

    let dropped: Vec<_> = chunk
        .components_identifiers()
        .filter(|component| {
            drop_components
                .iter()
                .any(|pattern| glob_matches(pattern, component.as_str()))
        })
        .collect();
    let is_whole = dropped.is_empty();
    let chunk = if is_whole {
        chunk
    } else {
        chunk.components_dropped(&dropped)
    };
    if chunk.components().is_empty() {
        return Ok(Route::Drop);
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
    let image_columns = match video {
        Video::Allowed => image_columns(&chunk),
        Video::Off => None,
    };

    let mut components: Vec<_> = chunk.components_identifiers().collect();
    components.sort();
    Ok(Route::Temporal(Box::new(Temporal {
        stream: StreamKey {
            recording_id: store_id.recording_id.clone(),
            entity_path: chunk.entity_path().clone(),
            components,
        },
        max_hz: *max_hz,
        delivery,
        store_id: store_id.clone(),
        chunk,
        is_whole,
        image_columns,
    })))
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
        Rules::parse(toml, Video::Allowed).unwrap()
    }

    fn no_rules() -> Rules {
        Rules::new(Video::Allowed)
    }

    fn temporal(log_msg: &LogMsg, rules: &Rules) -> Temporal {
        match classify(log_msg, rules).unwrap() {
            Route::Temporal(temporal) => *temporal,
            route @ (Route::Guaranteed(_) | Route::Drop) => panic!("not temporal: {route:?}"),
        }
    }

    #[test]
    fn temporal_data_is_latest_by_default() {
        let msg = log_msg(
            rerun::StoreKind::Recording,
            at_step_1(),
            &rerun::Scalars::single(1.0),
        );
        let temporal = temporal(&msg, &no_rules());
        assert_eq!(temporal.delivery, Delivery::Latest);
        assert_eq!(temporal.max_hz, None);
        assert!(temporal.is_whole);
        assert_eq!(temporal.image_columns, None);
    }

    #[test]
    fn events_are_guaranteed_by_default() {
        let msg = log_msg(
            rerun::StoreKind::Recording,
            at_step_1(),
            &rerun::TextLog::new("hi"),
        );
        assert_eq!(temporal(&msg, &no_rules()).delivery, Delivery::Guaranteed);
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

        assert!(temporal(&msg, &no_rules()).image_columns.is_some());

        let without_video = rules("[[rule]]\nentities = \"/robot/**\"\nvideo = false");
        assert_eq!(temporal(&msg, &without_video).image_columns, None);
        assert_eq!(
            temporal(&msg, &Rules::new(Video::Off)).image_columns,
            None,
            "video is off for everything"
        );
    }

    #[test]
    fn images_with_other_components_are_still_video() {
        let image = rerun::Image::from_color_model_and_bytes(
            vec![0; 12],
            [2, 2],
            rerun::ColorModel::RGB,
            rerun::ChannelDatatype::U8,
        )
        .with_opacity(0.5);
        let msg = log_msg(rerun::StoreKind::Recording, at_step_1(), &image);

        let temporal = temporal(&msg, &no_rules());
        let image_columns = temporal.image_columns.unwrap();
        assert_eq!(image_columns.len(), 2);
        assert_eq!(temporal.chunk.components().len(), 3);
        assert_eq!(
            temporal
                .chunk
                .components_dropped(&image_columns)
                .components()
                .len(),
            1,
            "the opacity is left to travel as data"
        );
    }

    #[test]
    fn rules_drop_components() {
        let image = rerun::Image::from_color_model_and_bytes(
            vec![0; 12],
            [2, 2],
            rerun::ColorModel::RGB,
            rerun::ChannelDatatype::U8,
        )
        .with_opacity(0.5);
        let msg = log_msg(rerun::StoreKind::Recording, at_step_1(), &image);

        let without_opacity =
            rules("[[rule]]\nentities = \"/robot/**\"\ndrop_components = [\"*:opacity\"]");
        let temporal = temporal(&msg, &without_opacity);
        assert!(!temporal.is_whole);
        assert_eq!(temporal.chunk.components().len(), 2);

        let without_anything =
            rules("[[rule]]\nentities = \"/robot/**\"\ndrop_components = [\"Image:*\"]");
        assert_eq!(classify(&msg, &without_anything).unwrap(), Route::Drop);
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
            temporal(&scalar, &guaranteed).delivery,
            Delivery::Guaranteed
        );

        let latched = rules("[[rule]]\nentities = \"/robot/**\"\ndelivery = \"latched\"");
        assert_eq!(temporal(&scalar, &latched).delivery, Delivery::Latched);

        let limited = rules("[[rule]]\nentities = \"/robot/**\"\nmax_hz = 5");
        assert_eq!(temporal(&scalar, &limited).max_hz, Some(5.0));
    }
}
