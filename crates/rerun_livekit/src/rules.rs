//! User rules for what the robot side sends, and how.

use std::path::Path;

use anyhow::Context as _;
use rerun::EntityPath;
use rerun::external::re_log_types::{EntityPathFilter, ResolvedEntityPathFilter};

/// How temporal data travels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Delivery {
    /// Every message arrives, in order. Data queues up when the link is slow.
    Guaranteed,

    /// Like [`Self::Guaranteed`], and operators who join later get every message so far.
    ///
    /// For data that is sent once and has to stay known, like a latched ROS topic.
    Latched,

    /// Messages that are late or do not fit are dropped, so what arrives is always recent.
    Latest,
}

/// Whether images may travel on a video track.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Video {
    /// Lossy pixels, but far less bandwidth and the link's congestion control.
    Allowed,

    /// Images travel unchanged, like any other data.
    Off,
}

impl From<bool> for Video {
    fn from(is_allowed: bool) -> Self {
        if is_allowed { Self::Allowed } else { Self::Off }
    }
}

/// What to do with the temporal data of the entities a rule matches.
///
/// Store info, blueprints and static data are always sent, whatever the rules say:
/// without them the operator's viewer cannot show the scene at all.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    Drop,
    Forward {
        /// Send at most this many messages per second.
        max_hz: Option<f64>,

        /// Overrides the default, which depends on the archetype.
        delivery: Option<Delivery>,

        /// Whether images may travel as video.
        video: Video,

        /// Components that are not sent, as names in which `*` stands for any text.
        drop_components: Vec<String>,
    },
}

/// Whether `text` matches `pattern`, in which each `*` stands for any text, including none.
pub fn glob_matches(pattern: &str, text: &str) -> bool {
    let Some((prefix, rest)) = pattern.split_once('*') else {
        return pattern == text;
    };
    let Some(mut text) = text.strip_prefix(prefix) else {
        return false;
    };
    let mut parts = rest.split('*');
    let suffix = parts.next_back().unwrap_or_default();
    for part in parts {
        let Some(at) = text.find(part) else {
            return false;
        };
        text = &text[at + part.len()..];
    }
    text.ends_with(suffix)
}

struct Rule {
    entities: ResolvedEntityPathFilter,
    action: Action,
}

/// Rules in file order. The first rule that matches an entity decides.
pub struct Rules {
    rules: Vec<Rule>,

    /// What happens to entities that no rule matches.
    default_action: Action,
}

impl Rules {
    /// No rules: everything is sent, and `default_video` says whether images travel as video.
    pub fn new(default_video: Video) -> Self {
        Self {
            rules: Vec::new(),
            default_action: Action::Forward {
                max_hz: None,
                delivery: None,
                video: default_video,
                drop_components: Vec::new(),
            },
        }
    }

    /// Reads the rules file at `path`. See [`Self::parse`].
    pub fn load(path: &Path, default_video: Video) -> anyhow::Result<Self> {
        let toml = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read the rules file {}", path.display()))?;
        Self::parse(&toml, default_video)
            .with_context(|| format!("Invalid rules file {}", path.display()))
    }

    /// `default_video` applies to the rules that do not set `video`, and to the entities that
    /// no rule matches.
    pub fn parse(toml: &str, default_video: Video) -> anyhow::Result<Self> {
        let RulesFile { rule } = toml::from_str(toml)?;
        let rules = rule
            .into_iter()
            .map(|entry| Rule::new(entry, default_video))
            .collect::<anyhow::Result<_>>()?;
        Ok(Self {
            rules,
            ..Self::new(default_video)
        })
    }

    pub fn action_for(&self, entity_path: &EntityPath) -> &Action {
        self.rules
            .iter()
            .find(|rule| rule.entities.matches(entity_path))
            .map_or(&self.default_action, |rule| &rule.action)
    }
}

// ----------------------------------------------------------------------------

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RulesFile {
    #[serde(default)]
    rule: Vec<RuleEntry>,
}

/// One `[[rule]]` table, as written.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleEntry {
    /// An entity path filter in Rerun's syntax, e.g. `/camera/**`.
    entities: String,

    #[serde(default)]
    drop: bool,

    max_hz: Option<f64>,

    delivery: Option<Delivery>,

    /// Whether images travel as video. Overrides `--video`, the default for every entity.
    video: Option<bool>,

    #[serde(default)]
    drop_components: Vec<String>,
}

impl Rule {
    fn new(entry: RuleEntry, default_video: Video) -> anyhow::Result<Self> {
        let RuleEntry {
            entities,
            drop,
            max_hz,
            delivery,
            video,
            drop_components,
        } = entry;

        let action = if drop {
            anyhow::ensure!(
                max_hz.is_none()
                    && delivery.is_none()
                    && video.is_none()
                    && drop_components.is_empty(),
                "The rule for {entities:?} drops its entities, so its other settings have no effect"
            );
            Action::Drop
        } else {
            anyhow::ensure!(
                max_hz.is_none_or(|max_hz| 0.0 < max_hz),
                "`max_hz` of the rule for {entities:?} must be positive"
            );
            anyhow::ensure!(
                max_hz.is_none() || matches!(delivery, None | Some(Delivery::Latest)),
                "The rule for {entities:?} guarantees delivery, so `max_hz` cannot drop anything"
            );
            anyhow::ensure!(
                video != Some(true) || delivery.is_none(),
                "The rule for {entities:?} sets `delivery`, which video does not have"
            );
            let video = match video {
                Some(video) => video.into(),
                // Asking for a delivery means asking for the data lanes.
                None if delivery.is_some() => Video::Off,
                None => default_video,
            };
            Action::Forward {
                max_hz,
                delivery,
                video,
                drop_components,
            }
        };

        let entities = EntityPathFilter::parse_strict(&entities)
            .with_context(|| format!("Invalid entity path filter {entities:?}"))?
            .resolve_without_substitutions();
        Ok(Self { entities, action })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_matching_rule_decides() {
        let rules = Rules::parse(
            r#"
            [[rule]]
            entities = "/camera/debug/**"
            drop = true

            [[rule]]
            entities = "/camera/**"
            max_hz = 10

            [[rule]]
            entities = "/events/**"
            delivery = "guaranteed"
            "#,
            Video::Allowed,
        )
        .unwrap();

        assert_eq!(
            rules.action_for(&"/camera/debug/overlay".into()),
            &Action::Drop
        );
        assert_eq!(
            rules.action_for(&"/camera/front".into()),
            &Action::Forward {
                max_hz: Some(10.0),
                delivery: None,
                video: Video::Allowed,
                drop_components: Vec::new(),
            }
        );
        assert_eq!(
            rules.action_for(&"/events/alarm".into()),
            &Action::Forward {
                max_hz: None,
                delivery: Some(Delivery::Guaranteed),
                video: Video::Off,
                drop_components: Vec::new(),
            }
        );
        assert_eq!(
            rules.action_for(&"/lidar".into()),
            &Action::Forward {
                max_hz: None,
                delivery: None,
                video: Video::Allowed,
                drop_components: Vec::new(),
            }
        );
    }

    #[test]
    fn a_rule_overrides_the_default_for_video() {
        let toml = r#"
            [[rule]]
            entities = "/camera/front"
            video = true

            [[rule]]
            entities = "/camera/wrist"
            video = false

            [[rule]]
            entities = "/camera/rear"
            max_hz = 10
            "#;
        let video_for = |default_video, entity_path: &str| {
            let rules = Rules::parse(toml, default_video).unwrap();
            match rules.action_for(&entity_path.into()) {
                Action::Forward { video, .. } => *video,
                Action::Drop => panic!("{entity_path} is dropped"),
            }
        };

        for default_video in [Video::Allowed, Video::Off] {
            assert_eq!(video_for(default_video, "/camera/front"), Video::Allowed);
            assert_eq!(video_for(default_video, "/camera/wrist"), Video::Off);
            assert_eq!(
                video_for(default_video, "/camera/rear"),
                default_video,
                "a rule that does not set `video`"
            );
            assert_eq!(
                video_for(default_video, "/camera/side"),
                default_video,
                "no rule"
            );
        }
    }

    #[test]
    fn globs_match_any_text_for_each_star() {
        for (pattern, text, expected) in [
            ("Image:buffer", "Image:buffer", true),
            ("Image:buffer", "Image:format", false),
            ("*:message", "sensor_msgs.msg.CompressedImage:message", true),
            ("*:message", "EncodedImage:blob", false),
            ("sensor_msgs.*", "sensor_msgs.msg.JointState:message", true),
            ("*.msg.*:message", "tf2_msgs.msg.TFMessage:message", true),
            ("*.msg.*:message", "msg:message", false),
            ("*", "", true),
            ("a*a", "a", false),
        ] {
            assert_eq!(glob_matches(pattern, text), expected, "{pattern} on {text}");
        }
    }

    #[test]
    fn the_example_file_is_valid() {
        Rules::parse(include_str!("../example-rules.toml"), Video::Allowed).unwrap();
    }

    #[test]
    fn contradictory_rules_are_errors() {
        for toml in [
            "[[rule]]\nentities = \"/a\"\ndrop = true\nmax_hz = 5",
            "[[rule]]\nentities = \"/a\"\nmax_hz = 0",
            "[[rule]]\nentities = \"/a\"\nmax_hz = 5\ndelivery = \"guaranteed\"",
            "[[rule]]\nentities = \"/a\"\nmax_hz = 5\ndelivery = \"latched\"",
            "[[rule]]\nentities = \"/a\"\ndrop = true\ndrop_components = [\"*:message\"]",
            "[[rule]]\nentities = \"/a\"\nvideo = true\ndelivery = \"latest\"",
            "[[rule]]\nentities = \"/a\"\nvidoe = true",
        ] {
            assert!(Rules::parse(toml, Video::Allowed).is_err(), "{toml}");
        }
    }
}
