//! Client for the gRPC proxy that every Rerun viewer and `rerun --serve-grpc` hosts.

use anyhow::Context as _;
use re_protos::common::v1alpha1::StoreId;
use re_protos::log_msg::v1alpha1::LogMsg;
use re_protos::sdk_comms::v1alpha1::message_proxy_service_client::MessageProxyServiceClient;
use rerun::external::re_log_encoding::ToTransport as _;
use rerun::external::re_log_encoding::rrd::Compression;
use rerun::external::re_uri::ProxyUri;
use rerun::log::Chunk;
use tonic::transport::{Channel, Endpoint};

/// Connects to the proxy at `uri`, e.g. `rerun+http://127.0.0.1:9876/proxy`.
pub async fn connect(uri: &ProxyUri) -> anyhow::Result<MessageProxyServiceClient<Channel>> {
    let channel = Endpoint::new(uri.origin.as_url())
        .map(|endpoint| endpoint.http2_adaptive_window(true))
        .with_context(|| format!("Invalid Rerun proxy address {uri}"))?
        .connect()
        .await
        .with_context(|| format!("Failed to connect to the Rerun proxy at {uri}"))?;

    // A single chunk can hold a whole mesh or point cloud, so there is no useful size limit.
    Ok(MessageProxyServiceClient::new(channel)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX))
}

/// Encodes `chunk` as a message of the recording `store_id`.
pub fn encode_chunk(
    store_id: StoreId,
    chunk: &Chunk,
    compression: Compression,
) -> anyhow::Result<LogMsg> {
    let arrow_msg = chunk.to_arrow_msg().context("Failed to encode a chunk")?;
    let store_id = rerun::StoreId::try_from(store_id).context("Invalid store id")?;
    let msg = rerun::log::LogMsg::ArrowMsg(store_id, arrow_msg)
        .to_transport(compression)
        .context("Failed to encode a chunk")?;
    Ok(msg.into())
}
