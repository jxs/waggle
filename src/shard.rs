// Copyright 2026 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use std::{collections::HashMap, fmt::Debug, num::NonZeroUsize};

use libp2p::{PeerId, swarm::ConnectionId};
use lru::LruCache;

use crate::handler::Received;

/// Application-defined strategy to split an object into pieces and to
/// reconstruct it from received partial data.
///
/// Exchange the pieces of an object on topic streams. The protocol treats the
/// object grouping and the piece metadata as opaque data. The application
/// interprets this data through this trait.
/// 1. Implement this trait to define how to split and rebuild objects.
/// 2. Use `metadata()` to advertise the pieces the local node holds and the pieces it wants.
/// 3. Use `object_id` to join all the pieces of the same object.
/// 4. Integrate each newly received piece into the application as needed.
pub trait Shard: Send + Sync {
    /// Returns the identifier of the object for this piece.
    ///
    /// Returns the same identifier from every piece of the same logical object.
    /// The protocol uses this identifier to join the pieces that form the
    /// object during reconstruction. It corresponds to the `objectID` wire field.
    fn object_id(&self) -> Vec<u8>;

    /// Returns application-defined metadata.
    ///
    /// The metadata describes the pieces of the object that are available and
    /// the pieces that the local node wants. The protocol sends the returned
    /// bytes in the `piecesMetadata` protobuf field to advertise the pieces
    /// the local node holds and the pieces the local node wants.
    fn metadata(&self) -> Box<dyn Metadata>;

    /// Generates an action from the given metadata.
    ///
    /// When a connection requests specific pieces, generates the piece data to
    /// send back. The `metadata` parameter describes the pieces the connection
    /// requests and the pieces the connection holds that the local node does
    /// not have yet.
    ///
    /// Returns an [`Action`] for the given metadata, or an error.
    fn action_from_metadata(
        &self,
        peer_id: PeerId,
        connection: ConnectionId,
        metadata: Option<&[u8]>,
    ) -> Result<Action, Error>;
}

/// Application-defined state that describes the available and requested
/// pieces of an object.
///
/// It advertises the pieces of an object
/// that are present and the pieces that are still needed. It must give a byte
/// representation for wire transmission. It must also support in-place
/// updates when remote metadata is received.
///
/// Use `update` to add remote metadata to the local state. It returns `true`
/// when the state changed. Use `update_from_data` to track the metadata that
/// the remote peer believes the local peer has. Its default does nothing.
pub trait Metadata: Debug + Send + Sync {
    /// Returns the `Metadata` as a byte slice.
    fn as_slice(&self) -> &[u8];
    /// Adds the remote metadata to this `Metadata`.
    /// Returns `true` when this `Metadata` changed.
    fn update(&mut self, data: &[u8]) -> Result<bool, Error>;
    /// Tracks the metadata that the remote peer believes the local peer has.
    /// Uses the data received from the remote peer.
    /// The default returns [`Ok(())`](Ok) when the update logic is not present.
    fn update_from_data(&mut self, _data: &[u8]) -> Result<(), Error> {
        Ok(())
    }
}

/// The action to take for the given metadata.
pub struct Action {
    /// When `true`, the peer has data that the local node needs.
    pub need: bool,
    /// The data and the updated peer metadata to send to the peer.
    pub send: Option<(Vec<u8>, Box<dyn Metadata>)>,
}

/// Per-topic engine state for piece dissemination.
///
/// Holds a bounded local cache of the objects the local node has published.
/// Uses the cache to serve pieces to peers that are missing them and to
/// reconcile with those peers. Keeps a per-connection view of what each
/// connection has and wants for each object.
///
/// The local cache is a pure LRU bounded by `Config::max_cached_objects` and
/// evicted on insert. The application manages the long-term object lifetime,
/// not this behaviour.
#[derive(Debug)]
pub(crate) struct State {
    /// Per-topic LRU cache capacity for locally stored objects.
    capacity: NonZeroUsize,
    /// Locally cached published objects, keyed by `object_id`.
    /// The cache bound per topic comes from `Config::max_cached_objects`.
    local: HashMap<Vec<u8>, LruCache<Vec<u8>, Box<dyn Shard>>>,
    /// The local node's view of a connection's state, keyed by topic,
    /// `ConnectionId`, and `object_id`.
    remote: HashMap<Vec<u8>, HashMap<ConnectionId, HashMap<Vec<u8>, RemoteView>>>,
}

impl State {
    /// Creates a new `State` instance.
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            local: HashMap::new(),
            remote: HashMap::new(),
        }
    }

    /// Stores or updates a locally published object.
    /// Returns `false` when no piece changed.
    pub(crate) fn store<P: Shard + 'static>(&mut self, topic: &[u8], shard: P) -> bool {
        let object_id = shard.object_id();
        let cache = self
            .local
            .entry(topic.to_vec())
            .or_insert_with(|| LruCache::new(self.capacity));
        let updated = match cache.get_mut(&object_id) {
            Some(existing) => existing
                .metadata()
                .update(shard.metadata().as_slice())
                .unwrap_or(false),
            None => true, // new object
        };
        cache.put(object_id, Box::new(shard));
        updated
    }

    /// Computes what to send on a connection for `object_id`.
    ///
    /// Uses the cached shard and the connection's tracked metadata. Returns
    /// `Ok(None)` when there is nothing to send on the connection. Returns
    /// `Err` when the tracked metadata is invalid.
    pub(crate) fn reconcile(
        &mut self,
        topic: &[u8],
        connection: ConnectionId,
        peer: PeerId,
        object_id: &[u8],
    ) -> Result<Option<ReconcileAction>, Error> {
        let Some(shard) = self.local.get_mut(topic).and_then(|c| c.get(object_id)) else {
            return Ok(None);
        };
        let view = self
            .remote
            .entry(topic.to_vec())
            .or_default()
            .entry(connection)
            .or_default()
            .entry(object_id.to_vec())
            .or_default();
        let action = shard.action_from_metadata(
            peer,
            connection,
            view.peer_metadata.as_ref().map(PeerView::as_bytes),
        )?;
        // The data body, when the app produced it.
        let body = match action.send {
            Some((data, updated_peer_metadata)) => {
                view.peer_metadata = Some(PeerView::Local(updated_peer_metadata));
                Some(data)
            }
            None => None,
        };
        let publish_metadata = shard.metadata().as_slice().to_vec();
        // Did the connection's view of the local node's metadata change?
        let metadata_updated = match &mut view.our_metadata {
            Some(peer_view) => peer_view.update(&publish_metadata)?,
            None => {
                view.our_metadata = Some(shard.metadata());
                true
            }
        };
        match (body, metadata_updated) {
            (Some(data), true) => Ok(Some(ReconcileAction {
                body: Some(data),
                metadata: Some(publish_metadata),
            })),
            (Some(data), false) => Ok(Some(ReconcileAction {
                body: Some(data),
                metadata: None,
            })),
            (None, true) => Ok(Some(ReconcileAction {
                body: None,
                metadata: Some(publish_metadata),
            })),
            (None, false) => Ok(None),
        }
    }

    /// Reconciles a received piece against any cached object.
    /// Updates the local node's view of the connection's metadata.
    /// Returns the action the caller must take.
    pub(crate) fn received(
        &mut self,
        connection: ConnectionId,
        peer: PeerId,
        received: &Received,
    ) -> Result<Option<ReceivedAction>, Error> {
        // 1. Record the local node's view of the connection's metadata from
        //    what it sent.
        let view = self
            .remote
            .entry(received.topic_id.clone())
            .or_default()
            .entry(connection)
            .or_default()
            .entry(received.object_id.clone())
            .or_default();

        // Add the remote metadata to the local node's view of the peer. `updated`
        // is `true` when the peer's advertised state changed.
        let updated = match (&mut view.peer_metadata, &received.metadata) {
            // No view yet. Adopt the peer's metadata as the raw view.
            (None, Some(remote)) => {
                view.peer_metadata = Some(PeerView::Remote(remote.to_vec()));
                true
            }
            // Raw view. Replace it only when the peer's metadata differs.
            (Some(PeerView::Remote(current)), Some(remote)) => {
                let changed = current != remote;
                if changed {
                    view.peer_metadata = Some(PeerView::Remote(remote.to_vec()));
                }
                changed
            }
            // Updatable view. Add the remote metadata. Report when it changed.
            (Some(PeerView::Local(meta)), Some(remote)) => meta.update(remote)?,
            // No metadata to learn from.
            (_, None) => false,
        };

        // Nothing changed and no payload. Nothing to do.
        if !updated && received.pieces.is_none() {
            return Ok(None);
        }

        // 2. Does the local node hold this object? If not, surface the piece as-is.
        let Some(shard) = self
            .local
            .get_mut(&received.topic_id)
            .and_then(|c| c.get(&received.object_id))
        else {
            return Ok(Some(ReceivedAction {
                emit: true,
                send: None,
            }));
        };

        // 3. Reconcile against the peer with the cached shard.
        let action = shard.action_from_metadata(
            peer,
            connection,
            view.peer_metadata.as_ref().map(PeerView::as_bytes),
        )?;

        let mut outcome = ReceivedAction::default();
        if action.need {
            outcome.emit = true;
        }
        if let Some((body, updated_peer_metadata)) = action.send {
            view.peer_metadata = Some(PeerView::Local(updated_peer_metadata));
            outcome.send = Some((body, shard.metadata().as_slice().to_vec()));
        }
        Ok(Some(outcome))
    }

    /// Removes all tracked state for a closed connection.
    pub(crate) fn connection_closed(&mut self, connection_id: ConnectionId) {
        for views in self.remote.values_mut() {
            views.remove(&connection_id);
        }
        self.remote.retain(|_, views| !views.is_empty());
    }

    /// Removes all tracked state for an unsubscribed topic.
    pub(crate) fn remove_topic(&mut self, topic_id: &[u8]) {
        self.local.remove(topic_id);
        self.remote.remove(topic_id);
    }
}

/// Data to send to a peer after reconciling an object.
///
/// This action describes an outbound message to a peer. `body` is the piece
/// payload. `metadata` is the local node's current metadata.
/// When both are `None`, send nothing.
#[derive(Debug, Default)]
pub(crate) struct ReconcileAction {
    /// The piece payload to send to the peer. `None` sends no piece.
    pub body: Option<Vec<u8>>,
    /// The current metadata to advertise. `None` sends no metadata.
    pub metadata: Option<Vec<u8>>,
}

/// The action the behaviour must take after it reconciles a received piece.
///
/// This action describes how to handle an inbound message. Use `emit` to give
/// the received piece to the application. Use `send` to reply to the peer.
#[derive(Debug, Default)]
pub(crate) struct ReceivedAction {
    /// When `true`, give the received piece to the application.
    pub emit: bool,
    /// The reply to send to the peer: `(body, our_metadata)`.
    pub send: Option<(Vec<u8>, Vec<u8>)>,
}

/// The local node's tracked state, per peer and object.
///
/// This state describes the pieces the peer has, the pieces the peer wants,
/// and the pieces that the local node believes the peer has.
#[derive(Debug, Default)]
struct RemoteView {
    /// The local node's view of the peer's current metadata.
    peer_metadata: Option<PeerView>,
    /// The peer's view of the local node's current metadata.
    our_metadata: Option<Box<dyn Metadata>>,
}
/// The side that last produced the tracked metadata.
#[derive(Debug)]
enum PeerView {
    /// The metadata last updated from a value sent by the remote peer.
    Remote(Vec<u8>),
    /// The metadata the local node last produced.
    Local(Box<dyn Metadata>),
}
impl PeerView {
    /// The tracked metadata as raw bytes, from either side.
    fn as_bytes(&self) -> &[u8] {
        match self {
            PeerView::Remote(bytes) => bytes,
            PeerView::Local(metadata) => metadata.as_slice(),
        }
    }
}

/// Errors that can occur during object piece processing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The received data is too short to contain the required headers or metadata.
    #[error("insufficient data: expected at least {expected} bytes, got {received}")]
    InsufficientData {
        /// The minimum number of bytes expected.
        expected: usize,
        /// The number of bytes received.
        received: usize,
    },
    /// The data format is invalid or corrupted.
    #[error("invalid data format")]
    InvalidFormat,
    /// The piece data does not belong to this object.
    #[error("wrong object id: got {object_id:?}")]
    WrongObject {
        /// The object id for this received piece.
        object_id: Vec<u8>,
    },
    /// The piece data is a duplicate of already received data.
    #[error("duplicate data for piece {0:?}")]
    DuplicatePiece(Vec<u8>),
    /// The piece data is out of the expected range or sequence.
    #[error("data out of range")]
    OutOfRange,
    /// The object is already complete and cannot accept more data.
    #[error("object is already complete")]
    AlreadyComplete,
    /// Application-specific validation failed.
    #[error("validation failed")]
    ValidationFailed,
}
