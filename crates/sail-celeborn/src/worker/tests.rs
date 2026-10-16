#![expect(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::client::{BatchFilter, WorkerClient, WorkerClientOptions, WorkerClientPool};
use crate::common::{BatchHeader, CompressionCodec, PartitionLocation, PartitionReadOptions};
use crate::dfs::DfsReadOptions;
use crate::protocol::StatusCode;
use crate::protocol::proto::{
    MessageType, PbBufferStreamEnd, PbCommitFiles, PbCommitFilesResponse, PbOpenStream,
    PbStorageInfo, PbStreamHandler, StreamType,
};
use crate::protocol::transport::TransportMessage;

struct Frame {
    kind: u8,
    header: Vec<u8>,
    body: Vec<u8>,
}

async fn read_frame(socket: &mut TcpStream) -> Frame {
    let header_length = usize::try_from(socket.read_i32().await.unwrap()).unwrap();
    let kind = socket.read_u8().await.unwrap();
    let body_length = usize::try_from(socket.read_i32().await.unwrap()).unwrap();
    let mut header = vec![0; header_length];
    let mut body = vec![0; body_length];
    socket.read_exact(&mut header).await.unwrap();
    socket.read_exact(&mut body).await.unwrap();
    Frame { kind, header, body }
}

async fn write_frame(socket: &mut TcpStream, kind: u8, header: &[u8], body: &[u8]) {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&i32::try_from(header.len()).unwrap().to_be_bytes());
    bytes.push(kind);
    bytes.extend_from_slice(&i32::try_from(body.len()).unwrap().to_be_bytes());
    bytes.extend_from_slice(header);
    bytes.extend_from_slice(body);
    socket.write_all(&bytes).await.unwrap();
}

async fn reply_rpc(
    socket: &mut TcpStream,
    request: &Frame,
    message_type: MessageType,
    payload: Vec<u8>,
) {
    let body = TransportMessage::new(message_type, payload)
        .encode()
        .unwrap();
    reply_rpc_body(socket, request, &body).await;
}

async fn reply_rpc_body(socket: &mut TcpStream, request: &Frame, body: &[u8]) {
    assert_eq!(request.kind, 3);
    assert_eq!(request.header.len(), 12);
    let mut header = request.header[..8].to_vec();
    header.extend_from_slice(&i32::try_from(body.len()).unwrap().to_be_bytes());
    write_frame(socket, 4, &header, body).await;
}

fn java_response(message_type: MessageType, payload: &[u8]) -> Vec<u8> {
    fn write_utf(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend_from_slice(&u16::try_from(value.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }

    // Worker endpoint RPCs use the Java-serialized V1 TransportMessage wrapper.
    let mut bytes = vec![0xac, 0xed, 0, 5, 0x73, 0x72];
    write_utf(
        &mut bytes,
        "org.apache.celeborn.common.network.protocol.TransportMessage",
    );
    bytes.extend_from_slice(&(-3_259_000_920_699_629_773_i64).to_be_bytes());
    bytes.push(2);
    bytes.extend_from_slice(&2_u16.to_be_bytes());
    bytes.push(b'I');
    write_utf(&mut bytes, "messageTypeValue");
    bytes.push(b'[');
    write_utf(&mut bytes, "payload");
    bytes.push(0x74);
    write_utf(&mut bytes, "[B");
    bytes.extend_from_slice(&[0x78, 0x70]);
    bytes.extend_from_slice(&(message_type as i32).to_be_bytes());
    bytes.extend_from_slice(&[0x75, 0x72]);
    write_utf(&mut bytes, "[B");
    bytes.extend_from_slice(&(-5_984_413_125_824_719_648_i64).to_be_bytes());
    bytes.extend_from_slice(&[2, 0, 0, 0x78, 0x70]);
    bytes.extend_from_slice(&i32::try_from(payload.len()).unwrap().to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

async fn expect_open_stream(
    socket: &mut TcpStream,
    range: Option<std::ops::Range<i32>>,
    read_local_shuffle: bool,
    handler: PbStreamHandler,
) {
    let frame = read_frame(socket).await;
    assert_eq!(frame.kind, 3);
    let message = TransportMessage::decode(&frame.body).unwrap();
    assert_eq!(message.message_type, MessageType::OpenStream);
    let request = PbOpenStream::decode(message.payload.as_slice()).unwrap();
    assert_eq!(request.shuffle_key, "application-1");
    assert_eq!(request.file_name, "0-0-0");
    assert_eq!(request.read_local_shuffle, read_local_shuffle);
    let range = range.unwrap_or(0..i32::MAX);
    assert_eq!(request.start_index, range.start);
    assert_eq!(request.end_index, range.end);
    reply_rpc(
        socket,
        &frame,
        MessageType::StreamHandler,
        handler.encode_to_vec(),
    )
    .await;
}

async fn metadata_worker(
    listener: TcpListener,
    path: &str,
    range: Option<std::ops::Range<i32>>,
    offsets: Vec<i64>,
) {
    let (mut socket, _) = listener.accept().await.unwrap();
    // Actual Celeborn DFS replies have no path and no worker-fetchable chunks.
    expect_open_stream(
        &mut socket,
        range.clone(),
        false,
        PbStreamHandler {
            stream_id: 11,
            ..Default::default()
        },
    )
    .await;
    expect_open_stream(
        &mut socket,
        range,
        true,
        PbStreamHandler {
            stream_id: 12,
            num_chunks: i32::try_from(offsets.len().saturating_sub(1)).unwrap(),
            chunk_offsets: offsets,
            full_path: path.to_string(),
        },
    )
    .await;
    expect_end_stream(&mut socket, 11).await;
    expect_end_stream(&mut socket, 12).await;
}

async fn expect_end_stream(socket: &mut TcpStream, stream_id: i64) {
    let frame = tokio::time::timeout(Duration::from_secs(5), read_frame(socket))
        .await
        .unwrap();
    // Java sends BufferStreamEnd using its no-callback sendRpc overload: it is
    // an RPC request frame, but the worker sends no response.
    assert_eq!(frame.kind, 3);
    assert_eq!(frame.header.len(), 12);
    assert_eq!(
        i32::from_be_bytes(frame.header[8..].try_into().unwrap()),
        i32::try_from(frame.body.len()).unwrap()
    );
    let message = TransportMessage::decode(&frame.body).unwrap();
    assert_eq!(message.message_type, MessageType::BufferStreamEnd);
    let end = PbBufferStreamEnd::decode(message.payload.as_slice()).unwrap();
    assert_eq!(end.stream_type, StreamType::ChunkStream as i32);
    assert_eq!(end.stream_id, stream_id);
}

fn location(port: u16) -> PartitionLocation {
    PartitionLocation {
        mode: 0,
        id: 0,
        epoch: 0,
        host: "127.0.0.1".to_string(),
        rpc_port: port,
        push_port: port,
        fetch_port: port,
        replicate_port: port,
        peer: None,
    }
}

fn options(port: u16, store: Arc<dyn ObjectStore>) -> WorkerClientOptions {
    WorkerClientOptions::new(location(port)).with_dfs_read_options(DfsReadOptions {
        object_stores: [("s3://shuffle".to_string(), store)].into(),
        read_chunk_size: 7,
        ..Default::default()
    })
}

fn batch(map_id: i32, attempt_id: i32, batch_id: i32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = BatchHeader {
        map_id,
        attempt_id,
        batch_id,
        length: i32::try_from(payload.len()).unwrap(),
    }
    .encode();
    bytes.extend_from_slice(payload);
    bytes
}

fn sorted_index(regions: &[(i32, usize, usize)]) -> Bytes {
    let mut bytes = Vec::new();
    for &(map_id, offset, length) in regions {
        bytes.extend_from_slice(&map_id.to_be_bytes());
        bytes.extend_from_slice(&1_i32.to_be_bytes());
        bytes.extend_from_slice(&i64::try_from(offset).unwrap().to_be_bytes());
        bytes.extend_from_slice(&i64::try_from(length).unwrap().to_be_bytes());
    }
    bytes.into()
}

#[tokio::test]
async fn pooled_client_uses_new_dfs_options_and_reads_sorted_whole_partition_with_filtering() {
    let store = Arc::new(InMemory::new());
    let mut data = batch(0, 0, 0, b"failed");
    data.extend(batch(0, 1, 0, b"committed"));
    data.extend(batch(0, 1, 0, b"duplicate"));
    let map_one_offset = data.len();
    data.extend(batch(1, 0, 0, b"other mapper"));
    let file_length = data.len();
    // Sorting replaces the index and can delete the original file. A later whole
    // read must use the sorted path returned by the worker, even without a range.
    store
        .put(&Path::from("application/1/0-0-0.sorted"), data.into())
        .await
        .unwrap();
    store
        .put(
            &Path::from("application/1/0-0-0.index"),
            sorted_index(&[
                (1, map_one_offset, file_length - map_one_offset),
                (0, 0, map_one_offset),
            ])
            .into(),
        )
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let pool = WorkerClientPool::default();
    let _initial_client = pool.client(WorkerClientOptions::new(location(port)));
    let client = pool.client(options(port, store));
    let worker = tokio::spawn(async move {
        metadata_worker(
            listener,
            "s3://shuffle/application/1/0-0-0.sorted",
            None,
            vec![0, i64::try_from(file_length).unwrap()],
        )
        .await;
    });
    let batches = client
        .read_partition_stream("application-1", None)
        .await
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    worker.await.unwrap();
    assert_eq!(batches.len(), 4);
    let mut filter = BatchFilter::new(
        &PartitionReadOptions {
            map_attempts: Some(Arc::from([1, 0])),
            ..Default::default()
        },
        CompressionCodec::None,
    );
    let kept = batches
        .into_iter()
        .filter_map(|(header, payload)| filter.accept(header, payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        kept,
        vec![
            Bytes::from_static(b"committed"),
            Bytes::from_static(b"other mapper")
        ]
    );
}

#[tokio::test]
async fn dfs_metadata_probe_preserves_map_range_and_reads_only_selected_sorted_region() {
    let store = Arc::new(InMemory::new());
    let mut data = batch(0, 0, 0, b"before");
    let start = data.len();
    data.extend(batch(1, 0, 0, b"selected"));
    let end = data.len();
    data.extend(batch(2, 0, 0, b"after"));
    let file_length = data.len();
    store
        .put(&Path::from("application/1/0-0-0.sorted"), data.into())
        .await
        .unwrap();
    store
        .put(
            &Path::from("application/1/0-0-0.index"),
            sorted_index(&[
                (2, end, file_length - end),
                (0, 0, start),
                (1, start, end - start),
            ])
            .into(),
        )
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = WorkerClient::new(options(listener.local_addr().unwrap().port(), store));
    let worker = tokio::spawn(async move {
        metadata_worker(
            listener,
            "s3://shuffle/application/1/0-0-0.sorted",
            Some(1..2),
            vec![i64::try_from(start).unwrap(), i64::try_from(end).unwrap()],
        )
        .await;
    });
    let batches = client
        .read_partition_stream("application-1", Some(1..2))
        .await
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    worker.await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].0.map_id, 1);
    assert_eq!(batches[0].1, Bytes::from_static(b"selected"));
}

#[tokio::test]
async fn nonempty_local_partition_uses_chunk_fetch_without_metadata_probe() {
    let data = batch(0, 0, 0, b"worker chunk");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = WorkerClient::new(WorkerClientOptions::new(location(
        listener.local_addr().unwrap().port(),
    )));
    let worker = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        expect_open_stream(
            &mut socket,
            None,
            false,
            PbStreamHandler {
                stream_id: 21,
                num_chunks: 1,
                ..Default::default()
            },
        )
        .await;
        let frame = read_frame(&mut socket).await;
        assert_eq!(frame.kind, 0);
        assert_eq!(frame.header.len(), 20);
        assert_eq!(&frame.header[..8], &21_i64.to_be_bytes());
        assert_eq!(&frame.header[8..12], &0_i32.to_be_bytes());
        assert!(frame.body.is_empty());
        write_frame(&mut socket, 1, &frame.header, &data).await;
    });
    let batches = client
        .read_partition_stream("application-1", None)
        .await
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    worker.await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].1, Bytes::from_static(b"worker chunk"));
}

#[tokio::test]
async fn unsupported_nonempty_dfs_partition_fails_instead_of_reading_as_empty() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = WorkerClient::new(WorkerClientOptions::new(location(
        listener.local_addr().unwrap().port(),
    )));
    let worker = tokio::spawn(async move {
        metadata_worker(
            listener,
            "unregistered://storage/shuffle/0-0-0",
            None,
            vec![0, 16],
        )
        .await;
    });
    let mut batches = client.read_partition_stream("application-1", None).await;
    assert!(batches.next().await.unwrap().is_err());
    assert!(batches.next().await.is_none());
    drop(batches);
    worker.await.unwrap();
}

#[tokio::test]
async fn empty_local_partition_remains_empty_after_metadata_probe() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = WorkerClient::new(WorkerClientOptions::new(location(
        listener.local_addr().unwrap().port(),
    )));
    let worker = tokio::spawn(async move {
        metadata_worker(listener, "/worker/shuffle/0-0-0", None, vec![]).await;
    });
    let mut batches = client.read_partition_stream("application-1", None).await;
    assert!(batches.next().await.is_none());
    drop(batches);
    worker.await.unwrap();
}

#[tokio::test]
async fn dropping_unpolled_dfs_partition_stream_closes_worker_streams() {
    let store = Arc::new(InMemory::new());
    let data = batch(0, 0, 0, b"unread");
    let file_length = i64::try_from(data.len()).unwrap();
    store
        .put(&Path::from("application/1/0-0-0"), data.into())
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = WorkerClient::new(options(listener.local_addr().unwrap().port(), store));
    let worker = tokio::spawn(async move {
        metadata_worker(
            listener,
            "s3://shuffle/application/1/0-0-0",
            None,
            vec![0, file_length],
        )
        .await;
    });
    let batches = client.read_partition_stream("application-1", None).await;
    drop(batches);
    worker.await.unwrap();
}

#[tokio::test]
async fn truncated_dfs_data_fails_and_closes_worker_streams() {
    let store = Arc::new(InMemory::new());
    let mut data = batch(0, 0, 0, b"truncated");
    data.pop();
    let file_length = i64::try_from(data.len()).unwrap();
    store
        .put(&Path::from("application/1/0-0-0"), data.into())
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = WorkerClient::new(options(listener.local_addr().unwrap().port(), store));
    let worker = tokio::spawn(async move {
        metadata_worker(
            listener,
            "s3://shuffle/application/1/0-0-0",
            None,
            vec![0, file_length],
        )
        .await;
    });
    assert!(
        client
            .read_partition_stream("application-1", None)
            .await
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
    worker.await.unwrap();
}

#[tokio::test]
async fn commit_accepts_dfs_files_with_a_configured_object_store() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let client = WorkerClient::new(options(port, Arc::new(InMemory::new())));
    let worker = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let frame = read_frame(&mut socket).await;
        assert_eq!(frame.kind, 3);
        // Worker RPCs wrap their control message in an addressed RPC envelope.
        let marker = frame.body.iter().position(|byte| *byte == 0xff).unwrap();
        let message = TransportMessage::decode(&frame.body[marker + 1..]).unwrap();
        assert_eq!(message.message_type, MessageType::CommitFiles);
        let request = PbCommitFiles::decode(message.payload.as_slice()).unwrap();
        assert_eq!(request.application_id, "application");
        assert_eq!(request.shuffle_id, 1);
        assert_eq!(request.map_attempts, vec![0]);
        let response = PbCommitFilesResponse {
            status: i32::from(StatusCode::Success as u8),
            total_written: 16,
            file_count: 1,
            committed_primary_storage_infos: [(
                "0-0".to_string(),
                PbStorageInfo {
                    r#type: 5,
                    file_path: "s3://shuffle/application/1/0-0-0".to_string(),
                    ..Default::default()
                },
            )]
            .into(),
            ..Default::default()
        };
        let body = java_response(MessageType::CommitFilesResponse, &response.encode_to_vec());
        reply_rpc_body(&mut socket, &frame, &body).await;
    });
    let metrics = client
        .commit_files(
            "application".to_string(),
            1,
            vec![location(port)],
            vec![],
            vec![0],
        )
        .await
        .unwrap();
    worker.await.unwrap();
    assert_eq!(metrics.total_written, 16);
    assert_eq!(metrics.file_count, 1);
    assert!(metrics.unreadable_files.is_empty());
}
