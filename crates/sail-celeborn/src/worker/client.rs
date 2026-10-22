use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use prost::Message;

use crate::common::{
    BatchHeader, CompressionCodec, PartitionLocation, PartitionReadOptions, PartitionSplitMode,
    PushDataSize, UserIdentifier, WorkerIdentity,
};
use crate::endpoint::EndpointResolver;
use crate::error::{CelebornError, CelebornResult};
use crate::protocol::StatusCode;
use crate::protocol::proto::{
    MessageType, PbCommitFiles, PbCommitFilesResponse, PbOpenStream, PbReserveSlots,
    PbReserveSlotsResponse, PbStreamHandler,
};
use crate::protocol::transport::{TransportConnection, TransportMessage};

const WORKER_ENDPOINT_NAME: &str = "WorkerEndpoint";

/// Metrics reported by a worker after committing shuffle files.
#[derive(Debug, Clone, Default)]
pub struct CommitMetrics {
    pub total_written: i64,
    pub file_count: i64,
    /// Committed files that this client cannot read, as `(location ID, storage type)`.
    /// Workers serve files on distributed storage without chunks and expect readers to
    /// read them from the storage directly, which this client does not support.
    pub unreadable_files: Vec<(String, &'static str)>,
}

/// Returns the committed files of a commit response that are on distributed storage.
fn unreadable_files(response: &PbCommitFilesResponse) -> Vec<(String, &'static str)> {
    let mut files = response
        .committed_primary_storage_infos
        .iter()
        .chain(&response.committed_replica_storage_infos)
        .filter_map(|(id, info)| {
            // Celeborn's `StorageInfo.Type` codes.
            let storage = match info.r#type {
                3 => "HDFS",
                4 => "OSS",
                5 => "S3",
                _ => return None,
            };
            Some((id.clone(), storage))
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

/// A small async client for a Celeborn worker endpoint.
#[derive(Clone)]
pub struct WorkerClient {
    location: PartitionLocation,
    rpc: TransportConnection,
    push: TransportConnection,
    fetch: TransportConnection,
}

/// Reuses transport connections for partition locations on the same Celeborn worker.
///
/// A worker can own many partitions, but each worker needs only one RPC, push, and fetch
/// connection. Returned clients retain their individual partition location for wire metadata.
#[derive(Clone, Default)]
pub struct WorkerClientPool {
    clients: Arc<Mutex<HashMap<WorkerIdentity, WorkerClient>>>,
}

impl WorkerClientPool {
    pub fn client(&self, options: WorkerClientOptions) -> WorkerClient {
        let worker_identity = options.location.worker_identity();
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match clients.entry(worker_identity) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                entry.get().with_location(options.location)
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let client = WorkerClient::new(options);
                entry.insert(client.clone());
                client
            }
        }
    }

    /// Release cached connections for workers no longer used by active shuffles.
    /// Clients held by an in-flight request or delivered reader keep their own
    /// transport references, so eviction does not interrupt those operations.
    pub(crate) fn retain_workers(&self, workers: impl IntoIterator<Item = WorkerIdentity>) {
        let workers = workers.into_iter().collect::<HashSet<_>>();
        self.clients
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|worker, _| workers.contains(worker));
    }
}

/// Options for a [`WorkerClient`].
#[derive(Debug, Clone)]
pub struct WorkerClientOptions {
    pub location: PartitionLocation,
    pub endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    pub timeout: Duration,
}

impl WorkerClientOptions {
    pub fn new(location: PartitionLocation) -> Self {
        Self {
            location,
            endpoint_resolver: None,
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_endpoint_resolver(
        mut self,
        endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    ) -> Self {
        self.endpoint_resolver = endpoint_resolver;
        self
    }
}

impl WorkerClient {
    pub fn new(options: WorkerClientOptions) -> Self {
        let (rpc_host, rpc_port) = endpoint(
            &options.location,
            options.endpoint_resolver.as_ref(),
            options.location.rpc_port,
        );
        let (push_host, push_port) = endpoint(
            &options.location,
            options.endpoint_resolver.as_ref(),
            options.location.push_port,
        );
        let (fetch_host, fetch_port) = endpoint(
            &options.location,
            options.endpoint_resolver.as_ref(),
            options.location.fetch_port,
        );
        Self {
            location: options.location,
            rpc: TransportConnection::new(rpc_host, rpc_port, options.timeout),
            push: TransportConnection::new(push_host, push_port, options.timeout),
            fetch: TransportConnection::new(fetch_host, fetch_port, options.timeout),
        }
    }

    fn with_location(&self, location: PartitionLocation) -> Self {
        Self {
            location,
            rpc: self.rpc.clone(),
            push: self.push.clone(),
            fetch: self.fetch.clone(),
        }
    }

    /// Reserve a worker-side file writer for this partition location.
    pub async fn reserve_slots(
        &self,
        application_id: String,
        shuffle_id: i32,
        primary_locations: Vec<PartitionLocation>,
        replica_locations: Vec<PartitionLocation>,
        user_identifier: UserIdentifier,
        split_threshold: i64,
        split_mode: PartitionSplitMode,
    ) -> CelebornResult<()> {
        let request = PbReserveSlots {
            application_id,
            shuffle_id,
            primary_locations: primary_locations.into_iter().map(Into::into).collect(),
            replica_locations: replica_locations.into_iter().map(Into::into).collect(),
            split_threshold,
            split_mode: i32::from(split_mode),
            partition_type: 0,
            range_read_filter: false,
            user_identifier: Some(user_identifier.into()),
            push_data_timeout: 30_000,
            partition_split_enabled: true,
            available_storage_types: 0,
            partition_locations_pair: None,
            is_segment_granularity_visible: false,
        };
        let response = self
            .request_worker(MessageType::ReserveSlots, request.encode_to_vec())
            .await?;
        if response.message_type != MessageType::ReserveSlotsResponse {
            return Err(CelebornError::Protocol(
                "invalid reserve slots response".to_string(),
            ));
        }
        let response = PbReserveSlotsResponse::decode(response.payload.as_slice())?;
        if response.status != i32::from(StatusCode::Success as u8) {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        Ok(())
    }

    /// Commit a mapper's output on this worker so it can be read by downstream tasks.
    pub async fn commit_files(
        &self,
        application_id: String,
        shuffle_id: i32,
        primary_locations: Vec<PartitionLocation>,
        replica_locations: Vec<PartitionLocation>,
        map_attempts: Vec<i32>,
    ) -> CelebornResult<CommitMetrics> {
        let request = PbCommitFiles {
            application_id,
            shuffle_id,
            primary_ids: primary_locations
                .iter()
                .map(PartitionLocation::unique_id)
                .collect(),
            replica_ids: replica_locations
                .iter()
                .map(PartitionLocation::unique_id)
                .collect(),
            map_attempts,
            epoch: 0,
            mock_failure: false,
        };
        let response = self
            .request_worker(MessageType::CommitFiles, request.encode_to_vec())
            .await?;
        if response.message_type != MessageType::CommitFilesResponse {
            return Err(CelebornError::Protocol(
                "invalid commit files response".to_string(),
            ));
        }
        let response = PbCommitFilesResponse::decode(response.payload.as_slice())?;
        if response.status != i32::from(StatusCode::Success as u8) {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        Ok(CommitMetrics {
            total_written: response.total_written,
            file_count: i64::from(response.file_count),
            unreadable_files: unreadable_files(&response),
        })
    }

    /// Open a lazy stream of the raw (still compressed) batches of this primary
    /// partition, optionally restricted to the mappers in `map_range`.
    pub async fn read_partition_stream(
        &self,
        shuffle_key: &str,
        map_range: Option<std::ops::Range<i32>>,
    ) -> BoxStream<'static, CelebornResult<(BatchHeader, Bytes)>> {
        // `(0, i32::MAX)` reads the whole file. Any other range makes the worker sort
        // the file by map ID and return only the chunks of mappers in `[start, end)`.
        let (start_index, end_index) = match &map_range {
            Some(range) => (range.start, range.end),
            None => (0, i32::MAX),
        };
        let request = PbOpenStream {
            shuffle_key: shuffle_key.to_string(),
            file_name: format!("{}-{}-0", self.location.id, self.location.epoch),
            start_index,
            end_index,
            initial_credit: 0,
            read_local_shuffle: false,
        };
        let response = match self
            .request_fetch(MessageType::OpenStream, request.encode_to_vec())
            .await
        {
            Ok(response) => response,
            Err(error) => return Box::pin(stream::once(async move { Err(error) })),
        };
        if response.message_type != MessageType::StreamHandler {
            return Box::pin(stream::once(async {
                Err(CelebornError::Protocol(
                    "invalid open stream response".to_string(),
                ))
            }));
        }
        let handler = match PbStreamHandler::decode(response.payload.as_slice()) {
            Ok(handler) => handler,
            Err(error) => return Box::pin(stream::once(async move { Err(error.into()) })),
        };
        let guard = WorkerStreamGuard::new(self.fetch.clone(), handler.stream_id);
        if handler.num_chunks < 0 {
            guard.close().await;
            return Box::pin(stream::once(async {
                Err(CelebornError::Protocol(
                    "negative stream chunk count".to_string(),
                ))
            }));
        }
        let client = self.clone();
        Box::pin(stream::try_unfold(
            (
                client,
                guard,
                handler.stream_id,
                0,
                handler.num_chunks,
                BytesMut::new(),
            ),
            |(client, guard, stream_id, mut chunk_index, num_chunks, mut buffered)| async move {
                loop {
                    match take_batch(&mut buffered) {
                        Ok(Some(batch)) => {
                            return Ok(Some((
                                batch,
                                (client, guard, stream_id, chunk_index, num_chunks, buffered),
                            )));
                        }
                        Ok(None) => {}
                        Err(error) => {
                            guard.close().await;
                            return Err(error);
                        }
                    }
                    if chunk_index == num_chunks {
                        guard.close().await;
                        return if buffered.is_empty() {
                            Ok(None)
                        } else {
                            Err(CelebornError::Protocol("truncated batch data".to_string()))
                        };
                    }
                    match client
                        .fetch
                        .fetch_chunk(stream_id, chunk_index, 0, i32::MAX)
                        .await
                    {
                        Ok(chunk) => buffered.extend_from_slice(&chunk),
                        Err(error) => {
                            guard.close().await;
                            return Err(error);
                        }
                    }
                    chunk_index += 1;
                }
            },
        ))
    }

    async fn request_fetch(
        &self,
        message_type: MessageType,
        payload: Vec<u8>,
    ) -> CelebornResult<TransportMessage> {
        let response = self
            .fetch
            .send_rpc(TransportMessage::new(message_type, payload).encode()?)
            .await?;
        TransportMessage::decode(&response)
    }

    /// Push one Celeborn map-data batch to this primary partition location.
    pub async fn push_data(
        &self,
        shuffle_key: &str,
        map_id: i32,
        attempt_id: i32,
        batch_id: i32,
        data: Bytes,
        compression: CompressionCodec,
    ) -> CelebornResult<PushDataSize> {
        let uncompressed_len = data.len();
        let data = compress(data, compression)?;
        let compressed_len = data.len();
        let location_id = self.location.unique_id();
        self.push
            .push_data(
                shuffle_key,
                &location_id,
                map_id,
                attempt_id,
                batch_id,
                &data,
            )
            .await?;
        Ok(PushDataSize {
            uncompressed: uncompressed_len as u64,
            compressed: compressed_len as u64,
        })
    }

    async fn request_worker(
        &self,
        message_type: MessageType,
        payload: Vec<u8>,
    ) -> CelebornResult<TransportMessage> {
        let payload = TransportMessage::new(message_type, payload)
            .into_rpc_envelope(
                &self.location.host,
                &self.location.host,
                self.location.rpc_port,
                WORKER_ENDPOINT_NAME,
            )
            .encode()?;
        let response = self.rpc.send_rpc(payload).await?;
        TransportMessage::decode_java(&response)
    }
}

/// An OpenStream registration pins worker metadata and may keep the original
/// unsorted file alive after a range read creates its sorted replacement.
struct WorkerStreamGuard {
    fetch: TransportConnection,
    stream_id: Option<i64>,
    runtime: Option<tokio::runtime::Handle>,
}

impl WorkerStreamGuard {
    fn new(fetch: TransportConnection, stream_id: i64) -> Self {
        Self {
            fetch,
            stream_id: Some(stream_id),
            // Streams are Send and may be dropped outside their originating runtime.
            runtime: tokio::runtime::Handle::try_current().ok(),
        }
    }

    async fn close(mut self) {
        if let Some(stream_id) = self.stream_id
            && let Err(error) = self.fetch.end_chunk_stream(stream_id).await
        {
            log::debug!("failed to close Celeborn stream {stream_id}: {error}");
        }
        // Keep the ID armed across the await: if close is cancelled, Drop retries.
        self.stream_id = None;
    }
}

impl Drop for WorkerStreamGuard {
    fn drop(&mut self) {
        if let Some(stream_id) = self.stream_id.take()
            && let Some(runtime) = &self.runtime
        {
            let fetch = self.fetch.clone();
            runtime.spawn(async move {
                if let Err(error) = fetch.end_chunk_stream(stream_id).await {
                    log::debug!("failed to close Celeborn stream {stream_id}: {error}");
                }
            });
        }
    }
}

/// Compresses a payload into Celeborn's block format for the codec.
pub(crate) fn compress(data: Bytes, compression: CompressionCodec) -> CelebornResult<Bytes> {
    match compression {
        CompressionCodec::None => Ok(data),
        CompressionCodec::Lz4 => {
            const MAGIC: &[u8; 8] = b"LZ4Block";
            const HEADER_LENGTH: usize = 21;
            const RAW: u8 = 0x10;
            const COMPRESSED: u8 = 0x20;
            const SEED: u32 = 0x9747_b28c;

            let compressed = lz4_flex::block::compress(&data);
            let (method, payload) = if compressed.len() >= data.len() {
                (RAW, data.as_ref())
            } else {
                (COMPRESSED, compressed.as_slice())
            };
            let mut checksum = twox_hash::XxHash32::with_seed(SEED);
            checksum.write(&data);
            let checksum = checksum.finish() as u32;
            let payload_length = u32::try_from(payload.len()).map_err(|_| {
                CelebornError::Protocol("compressed push data is too large".to_string())
            })?;
            let data_length = u32::try_from(data.len())
                .map_err(|_| CelebornError::Protocol("push data is too large".to_string()))?;
            let mut output = Vec::with_capacity(HEADER_LENGTH + payload.len());
            output.extend_from_slice(MAGIC);
            output.push(method);
            output.extend_from_slice(&payload_length.to_le_bytes());
            output.extend_from_slice(&data_length.to_le_bytes());
            output.extend_from_slice(&checksum.to_le_bytes());
            output.extend_from_slice(payload);
            Ok(Bytes::from(output))
        }
        CompressionCodec::Zstd { level } => {
            const MAGIC: &[u8; 9] = b"ZSTDBlock";
            const HEADER_LENGTH: usize = 22;
            const RAW: u8 = 0x10;
            const COMPRESSED: u8 = 0x30;

            let compressed = zstd::bulk::compress(&data, i32::from(level))?;
            let (method, payload) = if compressed.len() >= data.len() {
                (RAW, data.as_ref())
            } else {
                (COMPRESSED, compressed.as_slice())
            };
            let payload_length = u32::try_from(payload.len()).map_err(|_| {
                CelebornError::Protocol("compressed push data is too large".to_string())
            })?;
            let data_length = u32::try_from(data.len())
                .map_err(|_| CelebornError::Protocol("push data is too large".to_string()))?;
            let mut output = Vec::with_capacity(HEADER_LENGTH + payload.len());
            output.extend_from_slice(MAGIC);
            output.push(method);
            output.extend_from_slice(&payload_length.to_le_bytes());
            output.extend_from_slice(&data_length.to_le_bytes());
            output.extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
            output.extend_from_slice(payload);
            Ok(Bytes::from(output))
        }
    }
}

fn decompress(data: Bytes, compression: CompressionCodec) -> CelebornResult<Bytes> {
    match compression {
        CompressionCodec::None => Ok(data),
        CompressionCodec::Lz4 => decompress_block(
            data,
            b"LZ4Block",
            0x10,
            0x20,
            |data, len| {
                lz4_flex::block::decompress(data, len)
                    .map_err(|error| CelebornError::Protocol(format!("invalid LZ4 block: {error}")))
            },
            |data| {
                const SEED: u32 = 0x9747_b28c;
                let mut checksum = twox_hash::XxHash32::with_seed(SEED);
                checksum.write(data);
                checksum.finish() as u32
            },
        ),
        CompressionCodec::Zstd { .. } => decompress_block(
            data,
            b"ZSTDBlock",
            0x10,
            0x30,
            |data, len| {
                zstd::bulk::decompress(data, len).map_err(|error| {
                    CelebornError::Protocol(format!("invalid Zstd block: {error}"))
                })
            },
            crc32fast::hash,
        ),
    }
}

fn decompress_block(
    data: Bytes,
    magic: &[u8],
    raw_method: u8,
    compressed_method: u8,
    decompress: impl FnOnce(&[u8], usize) -> CelebornResult<Vec<u8>>,
    checksum: impl FnOnce(&[u8]) -> u32,
) -> CelebornResult<Bytes> {
    let magic_len = magic.len();
    let header_length = magic_len + 13;
    if data.len() < header_length || &data[..magic_len] != magic {
        return Err(CelebornError::Protocol(
            "invalid compressed block header".to_string(),
        ));
    }
    let method = data[magic_len];
    let compressed_len = usize::try_from(u32::from_le_bytes(
        data[magic_len + 1..magic_len + 5]
            .try_into()
            .map_err(|_| CelebornError::Protocol("invalid compressed block length".to_string()))?,
    ))
    .map_err(|_| CelebornError::Protocol("compressed block is too large".to_string()))?;
    let original_len = usize::try_from(u32::from_le_bytes(
        data[magic_len + 5..magic_len + 9].try_into().map_err(|_| {
            CelebornError::Protocol("invalid decompressed block length".to_string())
        })?,
    ))
    .map_err(|_| CelebornError::Protocol("decompressed block is too large".to_string()))?;
    let expected_checksum =
        u32::from_le_bytes(data[magic_len + 9..header_length].try_into().map_err(|_| {
            CelebornError::Protocol("invalid compressed block checksum".to_string())
        })?);
    let payload_end = header_length
        .checked_add(compressed_len)
        .ok_or_else(|| CelebornError::Protocol("compressed block is too large".to_string()))?;
    if payload_end != data.len() {
        return Err(CelebornError::Protocol(
            "truncated compressed block".to_string(),
        ));
    }
    let output = match method {
        method if method == raw_method => {
            if compressed_len != original_len {
                return Err(CelebornError::Protocol(
                    "invalid raw compressed block".to_string(),
                ));
            }
            data.slice(header_length..)
        }
        method if method == compressed_method => {
            Bytes::from(decompress(&data[header_length..], original_len)?)
        }
        _ => {
            return Err(CelebornError::Protocol(format!(
                "unsupported compressed block method: {method}"
            )));
        }
    };
    if output.len() != original_len || checksum(&output) != expected_checksum {
        return Err(CelebornError::Protocol(
            "compressed block checksum mismatch".to_string(),
        ));
    }
    Ok(output)
}

fn take_batch(buffered: &mut BytesMut) -> CelebornResult<Option<(BatchHeader, Bytes)>> {
    if buffered.len() < BatchHeader::SIZE {
        return Ok(None);
    }
    let header = BatchHeader::decode(&buffered[..BatchHeader::SIZE])
        .ok_or_else(|| CelebornError::Protocol("invalid batch header".to_string()))?;
    let length = usize::try_from(header.length)
        .map_err(|_| CelebornError::Protocol("invalid batch length".to_string()))?;
    let end = BatchHeader::SIZE
        .checked_add(length)
        .ok_or_else(|| CelebornError::Protocol("invalid batch length".to_string()))?;
    if buffered.len() < end {
        return Ok(None);
    }
    let mut batch = buffered.split_to(end);
    Ok(Some((header, batch.split_off(BatchHeader::SIZE).freeze())))
}

/// Filters and decompresses the raw batches read from every location of one reduce
/// partition, so that each committed batch is returned exactly once:
///   - batches of attempts other than the committed attempt of their mapper are
///     skipped, since a failed attempt may have pushed partial data;
///   - a batch pushed again after a revive may be stored in two locations, so
///     batches are deduplicated by `(map ID, attempt ID, batch ID)` across locations;
///   - with a map range, batches of mappers outside the range are skipped.
pub(crate) struct BatchFilter {
    map_range: Option<std::ops::Range<i32>>,
    map_attempts: Option<Arc<[i32]>>,
    seen: std::collections::HashSet<(i32, i32, i32)>,
    compression: CompressionCodec,
}

impl BatchFilter {
    pub(crate) fn new(options: &PartitionReadOptions, compression: CompressionCodec) -> Self {
        Self {
            map_range: options.map_range.clone(),
            map_attempts: options.map_attempts.clone(),
            seen: Default::default(),
            compression,
        }
    }

    /// Returns the decompressed payload of a batch to keep, or `None` to skip it.
    pub(crate) fn accept(
        &mut self,
        header: BatchHeader,
        payload: Bytes,
    ) -> CelebornResult<Option<Bytes>> {
        if self
            .map_range
            .as_ref()
            .is_some_and(|range| !range.contains(&header.map_id))
        {
            return Ok(None);
        }
        if let Some(attempts) = &self.map_attempts {
            let committed = usize::try_from(header.map_id)
                .ok()
                .and_then(|i| attempts.get(i))
                .ok_or_else(|| {
                    CelebornError::Protocol(format!(
                        "batch of map {} is outside the {} committed mappers",
                        header.map_id,
                        attempts.len()
                    ))
                })?;
            if *committed != header.attempt_id {
                return Ok(None);
            }
        }
        if !self
            .seen
            .insert((header.map_id, header.attempt_id, header.batch_id))
        {
            return Ok(None);
        }
        decompress(payload, self.compression).map(Some)
    }
}

fn endpoint(
    location: &PartitionLocation,
    endpoint_resolver: Option<&Arc<dyn EndpointResolver>>,
    port: u16,
) -> (String, u16) {
    endpoint_resolver
        .map(|resolver| resolver.resolve(&location.host, port))
        .unwrap_or_else(|| (location.host.clone(), port))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use futures::StreamExt;
    use prost::Message;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::{
        BatchFilter, WorkerClient, WorkerClientOptions, WorkerClientPool, compress, decompress,
        take_batch, unreadable_files,
    };
    use crate::common::{BatchHeader, CompressionCodec, PartitionLocation, PartitionReadOptions};
    use crate::protocol::proto::{MessageType, PbBufferStreamEnd, PbStreamHandler, StreamType};
    use crate::protocol::transport::TransportMessage;

    async fn read_frame(socket: &mut TcpStream) -> (u8, Vec<u8>, Vec<u8>) {
        let header_length = socket.read_i32().await.unwrap() as usize;
        let frame_type = socket.read_u8().await.unwrap();
        let body_length = socket.read_i32().await.unwrap() as usize;
        let mut header = vec![0; header_length];
        let mut body = vec![0; body_length];
        socket.read_exact(&mut header).await.unwrap();
        socket.read_exact(&mut body).await.unwrap();
        (frame_type, header, body)
    }

    async fn write_frame(socket: &mut TcpStream, frame_type: u8, header: &[u8], body: &[u8]) {
        socket.write_i32(header.len() as i32).await.unwrap();
        socket.write_u8(frame_type).await.unwrap();
        socket.write_i32(body.len() as i32).await.unwrap();
        socket.write_all(header).await.unwrap();
        socket.write_all(body).await.unwrap();
    }

    async fn check_stream_cleanup(
        num_chunks: i32,
        chunks: Vec<Vec<u8>>,
        fail_fetch: bool,
        drop_after: Option<usize>,
        expected_error: Option<&str>,
    ) {
        tokio::time::timeout(Duration::from_secs(3), async move {
            const STREAM_ID: i64 = 12345;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let mut location = location(0);
            location.host = address.ip().to_string();
            location.fetch_port = address.port();
            let client = WorkerClient::new(WorkerClientOptions::new(location));
            let (closed, closed_received) = tokio::sync::oneshot::channel();
            let worker = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (frame_type, header, body) = read_frame(&mut socket).await;
                assert_eq!(frame_type, 3); // RpcRequest
                assert_eq!(
                    TransportMessage::decode(&body).unwrap().message_type,
                    MessageType::OpenStream
                );
                let response = TransportMessage::new(
                    MessageType::StreamHandler,
                    PbStreamHandler {
                        stream_id: STREAM_ID,
                        num_chunks,
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .encode()
                .unwrap();
                let mut response_header = header[..8].to_vec();
                response_header.extend_from_slice(&(response.len() as i32).to_be_bytes());
                write_frame(&mut socket, 4, &response_header, &response).await;
                let mut fetched = 0;
                loop {
                    let (frame_type, header, body) = read_frame(&mut socket).await;
                    if frame_type == 3 {
                        let message = TransportMessage::decode(&body).unwrap();
                        assert_eq!(message.message_type, MessageType::BufferStreamEnd);
                        let end = PbBufferStreamEnd::decode(message.payload.as_slice()).unwrap();
                        assert_eq!(end.stream_type, StreamType::ChunkStream as i32);
                        assert_eq!(end.stream_id, STREAM_ID);
                        closed.send(()).unwrap();
                        // FetchHandler deliberately sends no reply to this RPC. A
                        // subsequent OpenStream must still reuse the connection.
                        let (frame_type, header, body) = read_frame(&mut socket).await;
                        assert_eq!(frame_type, 3);
                        assert_eq!(
                            TransportMessage::decode(&body).unwrap().message_type,
                            MessageType::OpenStream
                        );
                        let mut response_header = header[..8].to_vec();
                        response_header.extend_from_slice(&(response.len() as i32).to_be_bytes());
                        write_frame(&mut socket, 4, &response_header, &response).await;
                        return;
                    }
                    assert_eq!(frame_type, 0); // ChunkFetchRequest
                    assert!(fetched < chunks.len());
                    write_frame(
                        &mut socket,
                        if fail_fetch { 2 } else { 1 },
                        &header,
                        &chunks[fetched],
                    )
                    .await;
                    fetched += 1;
                }
            });
            let mut stream = client.read_partition_stream("app-0", None).await;
            let _exhausted = if let Some(count) = drop_after {
                for _ in 0..count {
                    assert_eq!(stream.next().await.unwrap().unwrap().1.as_ref(), b"abc");
                }
                // Callers can release a Send stream outside its Tokio runtime.
                std::thread::spawn(move || drop(stream)).join().unwrap();
                None
            } else {
                let mut errors = 0;
                while let Some(item) = stream.next().await {
                    match item {
                        Ok((_, payload)) => assert_eq!(payload.as_ref(), b"abc"),
                        Err(error) => {
                            assert!(error.to_string().contains(expected_error.unwrap()));
                            errors += 1;
                        }
                    }
                }
                assert_eq!(errors, usize::from(expected_error.is_some()));
                // Keep the exhausted stream alive: EOF itself must release it.
                Some(stream)
            };
            // Serialize a further request after the cleanup (the server receives
            // StreamEnd before replying). Drop paths schedule cleanup asynchronously.
            closed_received.await.unwrap();
            let _next_stream = client.read_partition_stream("app-0", None).await;
            worker.await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn partition_stream_closes_worker_registration_at_eof() {
        let mut batch = header(0, 0, 0, 3).encode();
        batch.extend_from_slice(b"abc");
        check_stream_cleanup(
            2,
            vec![batch[..10].to_vec(), batch[10..].to_vec()],
            false,
            None,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn empty_partition_stream_closes_worker_registration() {
        check_stream_cleanup(0, vec![], false, None, None).await;
    }

    #[tokio::test]
    async fn failed_partition_stream_closes_worker_registration() {
        check_stream_cleanup(
            1,
            vec![vec![]],
            true,
            None,
            Some("invalid chunk fetch response"),
        )
        .await;
    }

    #[tokio::test]
    async fn dropped_unpolled_partition_stream_closes_worker_registration() {
        check_stream_cleanup(1, vec![vec![]], false, Some(0), None).await;
    }

    #[tokio::test]
    async fn dropped_partial_partition_stream_closes_worker_registration() {
        let mut batch = header(0, 0, 0, 3).encode();
        batch.extend_from_slice(b"abc");
        check_stream_cleanup(2, vec![batch, vec![]], false, Some(1), None).await;
    }

    #[tokio::test]
    async fn truncated_partition_stream_closes_worker_registration() {
        check_stream_cleanup(
            1,
            vec![header(0, 0, 0, 3).encode()],
            false,
            None,
            Some("truncated batch data"),
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_batch_closes_worker_registration() {
        check_stream_cleanup(
            1,
            vec![header(0, 0, 0, -1).encode()],
            false,
            None,
            Some("invalid batch length"),
        )
        .await;
    }

    #[tokio::test]
    async fn negative_chunk_count_closes_worker_registration() {
        check_stream_cleanup(-1, vec![], false, None, Some("negative stream chunk count")).await;
    }

    #[tokio::test]
    async fn cancelled_chunk_fetch_closes_worker_registration_on_a_new_connection() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let mut location = location(0);
            location.host = address.ip().to_string();
            location.fetch_port = address.port();
            let client = WorkerClient::new(WorkerClientOptions::new(location));
            let (fetch_started, fetch_started_received) = tokio::sync::oneshot::channel();
            let worker = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (_, header, _) = read_frame(&mut socket).await;
                let response = TransportMessage::new(
                    MessageType::StreamHandler,
                    PbStreamHandler {
                        stream_id: 12345,
                        num_chunks: 1,
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .encode()
                .unwrap();
                let mut response_header = header[..8].to_vec();
                response_header.extend_from_slice(&(response.len() as i32).to_be_bytes());
                write_frame(&mut socket, 4, &response_header, &response).await;
                let (frame_type, _, _) = read_frame(&mut socket).await;
                assert_eq!(frame_type, 0);
                fetch_started.send(()).unwrap();
                // The cancelled request discards its socket; its guard must then
                // release the known stream ID through a new connection.
                match socket.read(&mut [0]).await {
                    Ok(size) => assert_eq!(size, 0),
                    Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
                }
                let (mut socket, _) = listener.accept().await.unwrap();
                let (frame_type, _, body) = read_frame(&mut socket).await;
                assert_eq!(frame_type, 3);
                let message = TransportMessage::decode(&body).unwrap();
                assert_eq!(message.message_type, MessageType::BufferStreamEnd);
                let end = PbBufferStreamEnd::decode(message.payload.as_slice()).unwrap();
                assert_eq!(end.stream_type, StreamType::ChunkStream as i32);
                assert_eq!(end.stream_id, 12345);
            });
            let mut stream = client.read_partition_stream("app-0", None).await;
            let cancelled = tokio::spawn(async move { stream.next().await });
            fetch_started_received.await.unwrap();
            cancelled.abort();
            assert!(cancelled.await.unwrap_err().is_cancelled());
            worker.await.unwrap();
        })
        .await
        .unwrap();
    }

    fn header(map_id: i32, attempt_id: i32, batch_id: i32, length: i32) -> BatchHeader {
        BatchHeader {
            map_id,
            attempt_id,
            batch_id,
            length,
        }
    }

    #[test]
    fn batch_headers_are_little_endian() {
        let encoded = header(1, 2, 3, 4).encode();
        // Celeborn's Java client writes the header with `Platform.putInt`.
        assert_eq!(
            encoded,
            vec![1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0]
        );
        assert_eq!(BatchHeader::decode(&encoded), Some(header(1, 2, 3, 4)));
        assert_eq!(BatchHeader::decode(&encoded[..15]), None);
    }

    #[test]
    fn batches_are_split_from_chunks() {
        let mut buffered = BytesMut::new();
        buffered.extend_from_slice(&header(7, 0, 0, 3).encode());
        buffered.extend_from_slice(b"abc");
        buffered.extend_from_slice(&header(8, 1, 2, 2).encode());
        buffered.extend_from_slice(b"d");
        let (first, payload) = take_batch(&mut buffered).unwrap().unwrap();
        assert_eq!(first, header(7, 0, 0, 3));
        assert_eq!(payload.as_ref(), b"abc");
        // The second batch is incomplete until more data arrives.
        assert!(take_batch(&mut buffered).unwrap().is_none());
        buffered.extend_from_slice(b"e");
        let (second, payload) = take_batch(&mut buffered).unwrap().unwrap();
        assert_eq!(second.map_id, 8);
        assert_eq!(payload.as_ref(), b"de");
    }

    #[test]
    fn filter_keeps_committed_attempts_once() {
        let options = PartitionReadOptions {
            map_range: None,
            map_attempts: Some(Arc::from(vec![0, 1])),
        };
        let mut filter = BatchFilter::new(&options, CompressionCodec::None);
        let mut accept = |map_id, attempt_id, batch_id| {
            filter
                .accept(header(map_id, attempt_id, batch_id, 1), "x".into())
                .unwrap()
                .is_some()
        };
        assert!(accept(0, 0, 0));
        // A batch pushed again to another location after a revive.
        assert!(!accept(0, 0, 0));
        assert!(accept(0, 0, 1));
        // Partial data of a failed attempt of map 1.
        assert!(!accept(1, 0, 0));
        assert!(accept(1, 1, 0));
        // A mapper outside the committed mappers is a protocol error.
        assert!(filter.accept(header(2, 0, 0, 1), "x".into()).is_err());
    }

    #[test]
    fn filter_restricts_map_range() {
        let options = PartitionReadOptions {
            map_range: Some(1..3),
            map_attempts: None,
        };
        let mut filter = BatchFilter::new(&options, CompressionCodec::None);
        let kept = (0..4)
            .filter(|&map_id| {
                filter
                    .accept(header(map_id, 0, 0, 1), "x".into())
                    .unwrap()
                    .is_some()
            })
            .collect::<Vec<_>>();
        assert_eq!(kept, vec![1, 2]);
    }

    fn location(id: i32) -> PartitionLocation {
        PartitionLocation {
            mode: 0,
            id,
            epoch: 0,
            host: "worker".to_string(),
            rpc_port: 12000,
            push_port: 12001,
            fetch_port: 12002,
            replicate_port: 12003,
            peer: None,
        }
    }

    #[test]
    fn worker_client_pool_reuses_connections_for_partitions_on_one_worker() {
        let pool = WorkerClientPool::default();
        let first = pool.client(WorkerClientOptions::new(location(0)));
        let second = pool.client(WorkerClientOptions::new(location(1)));

        assert_eq!(first.location.id, 0);
        assert_eq!(second.location.id, 1);
        assert!(first.rpc.shares_stream_with(&second.rpc));
        assert!(first.push.shares_stream_with(&second.push));
        assert!(first.fetch.shares_stream_with(&second.fetch));
    }

    #[test]
    fn worker_client_pool_releases_unused_workers_without_invalidating_clients() {
        let pool = WorkerClientPool::default();
        let idle_location = location(0);
        let active_location = PartitionLocation {
            rpc_port: 13000,
            push_port: 13001,
            fetch_port: 13002,
            replicate_port: 13003,
            ..location(1)
        };
        let idle = pool.client(WorkerClientOptions::new(idle_location.clone()));
        let held = idle.clone();
        let active = pool.client(WorkerClientOptions::new(active_location.clone()));
        pool.retain_workers([active_location.worker_identity()]);

        let active_again = pool.client(WorkerClientOptions::new(active_location));
        assert!(active.rpc.shares_stream_with(&active_again.rpc));
        assert!(active.push.shares_stream_with(&active_again.push));
        assert!(active.fetch.shares_stream_with(&active_again.fetch));
        let fresh = pool.client(WorkerClientOptions::new(idle_location));
        assert!(!idle.rpc.shares_stream_with(&fresh.rpc));
        assert!(!idle.push.shares_stream_with(&fresh.push));
        assert!(!idle.fetch.shares_stream_with(&fresh.fetch));
        // Eviction only drops the pool's lease; an existing client remains valid.
        assert!(idle.rpc.shares_stream_with(&held.rpc));
        assert!(idle.push.shares_stream_with(&held.push));
        assert!(idle.fetch.shares_stream_with(&held.fetch));
    }

    #[test]
    fn compression_uses_celeborn_block_framing() {
        let data = Bytes::from(vec![42; 1_024]);
        let lz4 = compress(data.clone(), CompressionCodec::Lz4).unwrap();
        let zstd = compress(data.clone(), CompressionCodec::Zstd { level: 1 }).unwrap();

        assert_eq!(&lz4[..8], b"LZ4Block");
        assert_eq!(&zstd[..9], b"ZSTDBlock");
        assert_eq!(
            u32::from_le_bytes(lz4[13..17].try_into().unwrap()),
            data.len() as u32
        );
        assert_eq!(
            u32::from_le_bytes(zstd[14..18].try_into().unwrap()),
            data.len() as u32
        );
        assert_eq!(decompress(lz4, CompressionCodec::Lz4).unwrap(), data);
        assert_eq!(
            decompress(zstd, CompressionCodec::Zstd { level: 1 }).unwrap(),
            data
        );
    }

    #[test]
    fn raw_compressed_blocks_are_sliced_without_copying() {
        let mut state = 1_u32;
        let data = Bytes::from(
            (0..1_024)
                .map(|_| {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    (state >> 24) as u8
                })
                .collect::<Vec<_>>(),
        );
        let encoded = compress(data.clone(), CompressionCodec::Lz4).unwrap();
        assert_eq!(encoded[8], 0x10);

        let expected_data = encoded.slice(21..);
        let decoded = decompress(encoded, CompressionCodec::Lz4).unwrap();
        assert_eq!(decoded, data);
        assert_eq!(decoded.as_ptr(), expected_data.as_ptr());
    }

    #[test]
    fn files_on_distributed_storage_are_unreadable() {
        use crate::protocol::proto::{PbCommitFilesResponse, PbStorageInfo};

        let info = |r#type| PbStorageInfo {
            r#type,
            ..Default::default()
        };
        let response = PbCommitFilesResponse {
            committed_primary_storage_infos: [
                ("0-0".to_string(), info(1)),
                ("1-0".to_string(), info(3)),
                ("2-0".to_string(), info(0)),
            ]
            .into(),
            committed_replica_storage_infos: [("3-0".to_string(), info(5))].into(),
            ..Default::default()
        };
        assert_eq!(
            unreadable_files(&response),
            vec![("1-0".to_string(), "HDFS"), ("3-0".to_string(), "S3")]
        );
    }
}
