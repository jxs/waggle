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
// OR IMPLIED, BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, atomic::AtomicUsize},
    task::Poll,
};

use async_channel::Sender;
use libp2p::{
    PeerId,
    swarm::{ConnectionClosed, ConnectionId, FromSwarm, NetworkBehaviour, NotifyHandler, ToSwarm},
};
use rand::seq::SliceRandom;

use crate::{
    config::Config,
    handler::{self, HandlerIn, Received},
    protocol::proto::ObjectPieces,
    shard::{ReconcileAction, Shard, State},
    substream::{CloseSignal, DispatchHub, SendError},
};

/// Event emitted by the waggle behaviour.
#[derive(Debug)]
pub enum Event {
    /// A piece was received from a remote peer.
    Received {
        /// The topic this piece belongs to.
        topic_id: Vec<u8>,
        /// The peer that sent this piece.
        peer_id: PeerId,
        /// The received pieces.
        received: Received,
    },
    /// A remote peer subscribed to a topic.
    Subscribed {
        /// The remote peer that subscribed.
        peer_id: PeerId,
        /// The topic it subscribed to.
        topic_id: Vec<u8>,
    },
    /// A remote peer unsubscribed from a topic.
    Unsubscribed {
        /// The remote peer that unsubscribed.
        peer_id: PeerId,
        /// The topic it unsubscribed from.
        topic_id: Vec<u8>,
    },
    /// Data failed to reconcile against a peer's tracked metadata.
    ///
    /// The data may come from the peer, as in a received message, or from the
    /// local node, as in a newly published piece. It is not necessarily peer
    /// misbehavior. The application decides how to treat the peer.
    InvalidData {
        peer_id: PeerId,
        topic_id: Vec<u8>,
        object_id: Vec<u8>,
        reason: crate::shard::Error,
    },
    /// A remote peer has its send queue full.
    FullQueue(PeerId),
}

/// Network behaviour that handles the waggle protocol.
pub struct Behaviour {
    /// Behaviour configuration: queue sizes, limits, whitelist, and cache.
    config: Config,
    /// Piece-dissemination engine: the local object cache and per-connection views.
    state: State,
    /// Outbound events to the swarm, drained in `poll`.
    pending_events: VecDeque<ToSwarm<Event, HandlerIn>>,
    /// Established connections.
    connections: HashMap<ConnectionId, Connection>,
    /// The topics the local node is currently subscribed to.
    subscribed_topics: HashMap<Vec<u8>, CloseSignal>,
}

/// Per-connection outbound state for one established connection.
///
/// Holds the connection's outbound hub and the topics the remote peer
/// subscribed to on this connection.
#[derive(Debug)]
struct Connection {
    /// The remote peer behind this connection.
    peer_id: PeerId,
    /// The per-connection outbound hub, with its queue budget and senders.
    hub: DispatchHub,
}

impl Connection {
    /// Subscribes the remote connection to topic.
    fn subscribe(&mut self, topic_id: Vec<u8>, sender: Sender<ObjectPieces>) {
        self.hub.senders.insert(topic_id.clone(), sender);
    }

    fn unsubscribe(&mut self, topic_id: &[u8]) {
        self.hub.senders.remove(topic_id);
    }

    /// Checks if the connection is subscribed to the topic.
    fn is_subscribed(&self, topic_id: &[u8]) -> bool {
        self.hub.senders.contains_key(topic_id)
    }
}

impl Behaviour {
    /// Creates a new waggle [`Behaviour`] instance with the given [`Config`].
    pub fn new(config: Config) -> Self {
        Self {
            state: State::new(config.max_cached_objects()),
            config,
            pending_events: VecDeque::new(),
            connections: HashMap::new(),
            subscribed_topics: HashMap::new(),
        }
    }

    /// Subscribes to a topic.
    ///
    /// Returns `false` if the topic was already subscribed.
    pub fn subscribe(&mut self, topic_id: Vec<u8>) -> bool {
        if self.subscribed_topics.contains_key(&topic_id) {
            return false;
        }

        let close_signal = CloseSignal::new();
        // Hand every connection a receiver clone of the topic's close signal.
        for (connection_id, connection) in &self.connections {
            self.pending_events.push_back(ToSwarm::NotifyHandler {
                peer_id: connection.peer_id,
                handler: NotifyHandler::One(*connection_id),
                event: HandlerIn::Subscribe {
                    topic_id: topic_id.clone(),
                    close_signal: close_signal.receiver(),
                },
            });
        }

        self.subscribed_topics.insert(topic_id, close_signal);
        true
    }

    /// Unsubscribes from a topic.
    ///
    /// Returns `true` if the topic was subscribed.
    pub fn unsubscribe(&mut self, topic_id: Vec<u8>) -> bool {
        // Dropping the signal closes its channel; every connection's stream
        // wakes and closes.
        let Some(subscription) = self.subscribed_topics.remove(&topic_id) else {
            return false;
        };
        drop(subscription);

        self.state.remove_topic(&topic_id);
        true
    }

    /// Publishes an object piece to a topic.
    ///
    /// Disseminates the pieces to the topic's subscribers. Caches the pieces
    /// for reconciliation with peers that are missing them. `topic` selects
    /// the topic to which the piece belongs. `piece` carries the object
    /// identifier, the metadata, and the payload bits.
    ///
    /// Returns an [`Error`](crate::Error) if no peers are subscribed to the
    /// topic or the piece cannot be queued.
    pub fn publish<T: Into<Vec<u8>>, P: Shard + 'static>(
        &mut self,
        topic_id: T,
        piece: P,
    ) -> Result<(), Error> {
        let topic_id = topic_id.into();
        let object_id = piece.object_id();

        let mut eligible: Vec<(&ConnectionId, &Connection)> = self
            .connections
            .iter()
            .filter(|(_connection_id, connection)| connection.is_subscribed(&topic_id))
            .collect();

        if eligible.is_empty() {
            return Err(Error::NoPeersSubscribedToTopic);
        }

        eligible.shuffle(&mut rand::rng());

        // Cache and disseminate. When the metadata is unchanged, there is
        // nothing new to send.
        if !self.state.store(&topic_id, piece) {
            return Ok(());
        }

        // Select a random subset of the eligible connections to publish to.
        let publish_fanout = self.config.topic_config(&topic_id).publish_fanout();

        let mut full = vec![];
        let mut no_data = vec![];
        let mut published = 0;
        for (connection_id, connection) in eligible {
            if published >= publish_fanout.get() {
                break;
            }

            match self
                .state
                .reconcile(&topic_id, *connection_id, connection.peer_id, &object_id)
            {
                Ok(None) => {
                    no_data.push(*connection_id);
                }
                Ok(Some(ReconcileAction { body, metadata })) => {
                    let message = ObjectPieces {
                        object_id: object_id.clone(),
                        pieces_metadata: metadata,
                        pieces: body,
                    };

                    match connection.hub.try_send(&topic_id, message) {
                        Ok(()) => {
                            published += 1;
                        }
                        Err(SendError::Full) => {
                            tracing::debug!(peer_id=%connection.peer_id, "Outbound queue full for peer");
                            full.push(*connection_id);
                        }
                        Err(err @ (SendError::Closed | SendError::UnknownTopic)) => {
                            tracing::error!(peer_id=%connection.peer_id, error=%err, "Failed to send message");
                        }
                    }
                }
                Err(reason) => {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::InvalidData {
                            peer_id: connection.peer_id,
                            topic_id: topic_id.clone(),
                            object_id: object_id.clone(),
                            reason,
                        }))
                }
            }
        }

        if published == 0 {
            return Err(Error::MessageNotSent { full, no_data });
        }
        Ok(())
    }

    /// Reconciles a piece received on a connection and reports the outcome.
    ///
    /// Reconciles the received piece against any cached object. Emits the piece
    /// to the application when needed. Sends the reconciled reply back on the
    /// same connection.
    fn handle_received(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        received: Received,
    ) {
        // Ignore pieces for a topic the local node is not subscribed to.
        if !self.subscribed_topics.contains_key(&received.topic_id) {
            tracing::debug!(peer_id=%peer_id, "Received piece for an unsubscribed topic");
            return;
        }

        let outcome = match self.state.received(connection_id, peer_id, &received) {
            Ok(Some(o)) => o,
            Ok(None) => return,
            Err(reason) => {
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::InvalidData {
                        peer_id,
                        topic_id: received.topic_id,
                        object_id: received.object_id,
                        reason,
                    }));
                return;
            }
        };

        // Send the received piece to the application.
        if outcome.emit {
            self.pending_events
                .push_back(ToSwarm::GenerateEvent(Event::Received {
                    topic_id: received.topic_id.clone(),
                    peer_id,
                    received: received.clone(),
                }));
        }

        // Send the reconciled reply to the peer.
        if let Some((body, local_metadata)) = outcome.send {
            let message = ObjectPieces {
                object_id: received.object_id.clone(),
                pieces_metadata: Some(local_metadata),
                pieces: Some(body),
            };

            let Some(connection) = self.connections.get(&connection_id) else {
                tracing::error!(peer_id=%peer_id, "Piece received by unknown connection");
                return;
            };

            match connection.hub.try_send(&received.topic_id, message) {
                Ok(()) => {}
                Err(SendError::Full) => {
                    tracing::debug!(peer_id=%peer_id, "Outbound queue full for peer");
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::FullQueue(peer_id)));
                }
                Err(err @ (SendError::Closed | SendError::UnknownTopic)) => {
                    tracing::error!(peer_id=%peer_id, error=%err, "Failed to send message");
                }
            }
        }

        // Gossip the metadata for peers that don't have it yet
        let mut eligible: Vec<(&ConnectionId, &Connection)> = self
            .connections
            .iter()
            .filter(|(id, connection)| {
                **id != connection_id && connection.hub.senders.contains_key(&received.topic_id)
            })
            .collect();
        eligible.shuffle(&mut rand::rng());

        let gossip_fanout = self.config.topic_config(&received.topic_id).gossip_fanout();
        let mut sent = 0;

        for (connection_id, connection) in &eligible {
            if sent >= gossip_fanout.get() {
                break;
            }

            match self.state.reconcile(
                &received.topic_id,
                **connection_id,
                connection.peer_id,
                &received.object_id,
            ) {
                // Nothing new for this peer.
                Ok(None) => {}
                Ok(Some(ReconcileAction { metadata, .. })) => {
                    let message = ObjectPieces {
                        object_id: received.object_id.clone(),
                        pieces_metadata: metadata,
                        pieces: None,
                    };
                    match connection.hub.try_send(&received.topic_id, message) {
                        Ok(()) => sent += 1,
                        Err(SendError::Full) => {
                            tracing::debug!(peer_id=%connection.peer_id, "Outbound queue full for peer");
                        }
                        Err(err @ (SendError::Closed | SendError::UnknownTopic)) => {
                            tracing::error!(peer_id=%connection.peer_id, error=%err, "Failed to send message");
                        }
                    }
                }
                Err(reason) => {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::InvalidData {
                            peer_id: connection.peer_id,
                            topic_id: received.topic_id.clone(),
                            object_id: received.object_id.clone(),
                            reason,
                        }));
                }
            }
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = handler::Handler;

    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        _local_addr: &libp2p::Multiaddr,
        _remote_addr: &libp2p::Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        let used = Arc::new(AtomicUsize::new(0));
        let hub = DispatchHub::new(self.config.max_connection_queue_limit(), Arc::clone(&used));
        self.connections
            .insert(connection_id, Connection { peer_id, hub });

        // Subscribe this connection to all the topics we are subscribed to.
        for (topic_id, close_signal) in &self.subscribed_topics {
            self.pending_events.push_back(ToSwarm::NotifyHandler {
                peer_id,
                handler: NotifyHandler::One(connection_id),
                event: HandlerIn::Subscribe {
                    topic_id: topic_id.clone(),
                    close_signal: close_signal.receiver(),
                },
            });
        }

        Ok(handler::Handler::new(used, self.config.clone()))
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        _addr: &libp2p::Multiaddr,
        _role_override: libp2p::core::Endpoint,
        _port_use: libp2p::core::transport::PortUse,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        let used = Arc::new(AtomicUsize::new(0));
        let hub = DispatchHub::new(self.config.max_connection_queue_limit(), Arc::clone(&used));
        self.connections
            .insert(connection_id, Connection { peer_id, hub });

        // Subscribe this connection to all the topics we are subscribed to.
        for (topic_id, close_signal) in &self.subscribed_topics {
            self.pending_events.push_back(ToSwarm::NotifyHandler {
                peer_id,
                handler: NotifyHandler::One(connection_id),
                event: HandlerIn::Subscribe {
                    topic_id: topic_id.clone(),
                    close_signal: close_signal.receiver(),
                },
            });
        }

        Ok(handler::Handler::new(used, self.config.clone()))
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        if let FromSwarm::ConnectionClosed(ConnectionClosed { connection_id, .. }) = event {
            self.connections.remove(&connection_id);
            self.state.connection_closed(connection_id);
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection_id: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        match event {
            handler::Event::Subscribed { topic_id, sender } => {
                let Some(connection) = self.connections.get_mut(&connection_id) else {
                    tracing::error!("Subscription from an Unknown connection");
                    return;
                };

                connection.subscribe(topic_id.clone(), sender);
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Subscribed {
                        peer_id,
                        topic_id,
                    }))
            }
            handler::Event::Received(received) => {
                self.handle_received(peer_id, connection_id, received)
            }

            handler::Event::Unsubscribed { topic_id } => {
                let Some(connection) = self.connections.get_mut(&connection_id) else {
                    tracing::error!("Unsubscription from an Unknown connection");
                    return;
                };

                connection.unsubscribe(&topic_id);
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Unsubscribed {
                        peer_id,
                        topic_id,
                    }));
            }
        }
    }

    fn poll(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, libp2p::swarm::THandlerInEvent<Self>>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(event);
        }

        std::task::Poll::Pending
    }
}

/// Error associated with publishing an object piece on a topic.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No peers are currently subscribed to receive messages on this topic.
    /// Wait for peers to subscribe or check the network connectivity.
    #[error("no peers subscribed to the topic")]
    NoPeersSubscribedToTopic,
    /// A piece could not be delivered on any selected connection.
    #[error("message not sent on any of the connections, {0} full and {1} did not require new data",
         full.len(), no_data.len())]
    MessageNotSent {
        /// Connections whose send queue was full.
        full: Vec<ConnectionId>,
        /// Connections that had nothing new to send.
        no_data: Vec<ConnectionId>,
    },
}
