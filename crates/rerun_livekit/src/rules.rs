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

/// What to do with the temporal data of the entities a rule matches.
///
/// Store info, blueprints and static data are always sent, whatever the rules say:
/// without them the operator's viewer cannot show the scene at all.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Action {
    Drop,
    Forward {
        /// Send at most this many messages per second.
        max_hz: Option<f64>,

        /// Overrides the default, which depends on the archetype.
        delivery: Option<Delivery>,

        /// Whether images may travel as video.
        video: Video,
    },
}

impl Action {
    /// What happens to entities that no rule matches.
    pub const DEFAULT: Self = Self::Forward {
        max_hz: None,
        delivery: None,
        video: Video::Allowed,
    };
}

struct Rule {
    entities: ResolvedEntityPathFilter,
    action: Action,
}

/// Rules in file order. The first rule that matches an entity decides.
#[derive(Default)]
pub struct Rules(Vec<Rule>);

impl Rules {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let toml = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read the rules file {}", path.display()))?;
        Self::parse(&toml).with_context(|| format!("Invalid rules file {}", path.display()))
    }

    pub fn parse(toml: &str) -> anyhow::Result<Self> {
        let RulesFile { rule } = toml::from_str(toml)?;
        rule.into_iter()
            .map(Rule::try_from)
            .collect::<anyhow::Result<_>>()
            .map(Self)
    }

    pub fn action_for(&self, entity_path: &EntityPath) -> Action {
        self.0
            .iter()
            .find(|rule| rule.entities.matches(entity_path))
            .map_or(Action::DEFAULT, |rule| rule.action)
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

    /// `false` sends images unchanged instead of as video.
    video: Option<bool>,
}

impl TryFrom<RuleEntry> for Rule {
    type Error = anyhow::Error;

    fn try_from(entry: RuleEntry) -> anyhow::Result<Self> {
        let RuleEntry {
            entities,
            drop,
            max_hz,
            delivery,
            video,
        } = entry;

        let action = if drop {
            anyhow::ensure!(
                max_hz.is_none() && delivery.is_none() && video.is_none(),
                "The rule for {entities:?} drops its entities, so its other settings have no effect"
            );
            Action::Drop
        } else {
            anyhow::ensure!(
                max_hz.is_none_or(|max_hz| 0.0 < max_hz),
                "`max_hz` of the rule for {entities:?} must be positive"
            );
            anyhow::ensure!(
                max_hz.is_none() || delivery != Some(Delivery::Guaranteed),
                "The rule for {entities:?} guarantees delivery, so `max_hz` cannot drop anything"
            );
            anyhow::ensure!(
                video != Some(true) || delivery.is_none(),
                "The rule for {entities:?} sets `delivery`, which video does not have"
            );
            // Asking for a delivery means asking for the data lanes.
            let video = if video.unwrap_or_else(|| delivery.is_none()) {
                Video::Allowed
            } else {
                Video::Off
            };
            Action::Forward {
                max_hz,
                delivery,
                video,
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
        )
        .unwrap();

        assert_eq!(
            rules.action_for(&"/camera/debug/overlay".into()),
            Action::Drop
        );
        assert_eq!(
            rules.action_for(&"/camera/front".into()),
            Action::Forward {
                max_hz: Some(10.0),
                delivery: None,
                video: Video::Allowed,
            }
        );
        assert_eq!(
            rules.action_for(&"/events/alarm".into()),
            Action::Forward {
                max_hz: None,
                delivery: Some(Delivery::Guaranteed),
                video: Video::Off,
            }
        );
        assert_eq!(rules.action_for(&"/lidar".into()), Action::DEFAULT);
    }

    #[test]
    fn the_example_file_is_valid() {
        Rules::parse(include_str!("../example-rules.toml")).unwrap();
    }

    #[test]
    fn contradictory_rules_are_errors() {
        for toml in [
            "[[rule]]\nentities = \"/a\"\ndrop = true\nmax_hz = 5",
            "[[rule]]\nentities = \"/a\"\nmax_hz = 0",
            "[[rule]]\nentities = \"/a\"\nmax_hz = 5\ndelivery = \"guaranteed\"",
            "[[rule]]\nentities = \"/a\"\nvideo = true\ndelivery = \"latest\"",
            "[[rule]]\nentities = \"/a\"\nvidoe = true",
        ] {
            assert!(Rules::parse(toml).is_err(), "{toml}");
        }
    }
}
