//! Log setup for the bridge.

/// Log filters for LiveKit, used when `RUST_LOG` is not set.
///
/// `libwebrtc`, which LiveKit is built on, logs warnings and errors on every healthy connection
/// (duplicate codecs, a software decoder it does not need), which would bury this crate's logs.
const QUIET_LIVEKIT: &str = "libwebrtc=off,livekit=warn";

/// Starts logging to stderr.
///
/// `RUST_LOG` decides what is logged. Without it, that is everything from `default_level` up,
/// except LiveKit's own chatter. `RUST_LOG=debug` shows that too.
pub fn init_logging(default_level: log::LevelFilter) {
    let default_filter = format!("{default_level},{QUIET_LIVEKIT}");
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_filter))
        .init();
}
