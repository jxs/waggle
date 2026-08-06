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

use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    sync::atomic::AtomicUsize,
    task::{Context, Poll},
};

use async_channel::{Receiver, Sender, unbounded};
use futures::{
    StreamExt,
    stream::{FuturesUnordered, SelectAll},
};
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent,
    handler::{
        ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
        StreamUpgradeError, SubstreamProtocol,
    },
};

use crate::{
    config::Config,
    protocol::{InboundProtocol, OutboundProtocol, proto},
    substream::{Inbound, Outbound},
};

/// Instructions from the behaviour to the handler for managing topic streams.
#[derive(Debug)]
pub enum HandlerIn {
    /// Subscribe to a topic on this connection.
    ///
    /// `close_signal` is this connection's receiver clone of the topic's
    /// close signal; it is handed to the subscription stream so the stream
    /// closes when the topic is unsubscribed.
    Subscribe {
        topic_id: Vec<u8>,
        close_signal: Receiver<()>,
    },
}

/// Events emitted by the handler to the behaviour.
#[derive(Debug)]
pub enum Event {
    /// The remote peer sent a valid subscription.
    /// `Sender` used by the behaviour to dispatch messages to the remote.
    Subscribed {
        topic_id: Vec<u8>,
        sender: Sender<proto::ObjectPieces>,
    },
    /// A message with object pieces was received.
    Received(Received),
    /// The remote peer closed the stream (or an error occurred).
    Unsubscribed { topic_id: Vec<u8> },
}

/// A set of object pieces received from a peer.
#[derive(Debug, Clone)]
pub struct Received {
    pub topic_id: Vec<u8>,
    pub object_id: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
    pub pieces: Option<Vec<u8>>,
}

/// Connection-level state for the waggle protocol.
///
/// Manages the topic subscription streams on one connection: the receive
/// streams of the topics we subscribed to, and the send streams of the
/// topics the remote peer subscribed to. Delivers events to the behaviour
/// and enforces the inbound stream limit.
pub struct Handler {
    /// Configuration for limits, queue sizes, and wire sizes.
    config: Config,
    /// Negotiates inbound substreams and enforces the inbound stream limit.
    inbound_protocol: InboundProtocol,
    /// The receive streams of the topics we subscribed to on this connection.
    outbound_substreams: SelectAll<Outbound>,
    /// The topics the remote peer subscribed to on this connection.
    inbound_subscriptions: HashSet<Vec<u8>>,
    /// The send streams for the topics the remote peer subscribed to.
    inbound_substreams: FuturesUnordered<Inbound>,
    /// The connection events to deliver to the swarm, drained in `poll`.
    pending_events: VecDeque<ConnectionHandlerEvent<OutboundProtocol, Receiver<()>, Event>>,
    /// The per-connection outbound counter, handed to each stream writer.
    used: Arc<AtomicUsize>,
}

impl Handler {
    /// Creates a new `Handler` for one connection.
    ///
    /// `used` is this connection's outbound queue counter, shared with the
    /// senders and the stream writers. `config` holds the limits and the
    /// wire sizes.
    pub(crate) fn new(used: Arc<AtomicUsize>, config: Config) -> Self {
        Self {
            inbound_protocol: InboundProtocol::new(
                config.max_protobuf_size(),
                config.max_topic_subscription(),
            ),
            config,
            outbound_substreams: SelectAll::new(),
            inbound_subscriptions: HashSet::new(),
            inbound_substreams: FuturesUnordered::new(),
            pending_events: VecDeque::new(),
            used,
        }
    }
}

impl ConnectionHandler for Handler {
    type FromBehaviour = HandlerIn;
    type ToBehaviour = Event;
    type InboundProtocol = InboundProtocol;
    type OutboundProtocol = OutboundProtocol;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = Receiver<()>;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol> {
        SubstreamProtocol::new(self.inbound_protocol.clone(), ())
    }

    fn on_behaviour_event(&mut self, hin: HandlerIn) {
        match hin {
            HandlerIn::Subscribe {
                topic_id,
                close_signal,
            } => {
                self.pending_events
                    .push_back(ConnectionHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(
                            OutboundProtocol {
                                max_protobuf_size: self.config.max_protobuf_size(),
                                topic_id,
                            },
                            close_signal,
                        ),
                    });
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ConnectionHandlerEvent<Self::OutboundProtocol, Receiver<()>, Self::ToBehaviour>> {
        // Process the pending events first.
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(event);
        }

        // Then process the receive streams of the topics we subscribed to.
        if let Poll::Ready(Some(event)) = self.outbound_substreams.poll_next_unpin(cx) {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        // Then drive the send streams of the topics the remote subscribed to,
        // dropping the ones that completed.
        while let Poll::Ready(Some(event)) = self.inbound_substreams.poll_next_unpin(cx) {
            if let Some(Event::Unsubscribed { topic_id }) = event {
                self.inbound_subscriptions.remove(&topic_id);
                self.inbound_protocol.stream_closed();
                self.pending_events
                    .push_back(ConnectionHandlerEvent::NotifyBehaviour(
                        Event::Unsubscribed { topic_id },
                    ));
            }
        }

        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: (subscription, framed),
                ..
            }) => {
                if self
                    .config
                    .topic_whitelist
                    .as_ref()
                    .is_some_and(|w| !w.contains(&subscription.topic_id))
                {
                    tracing::debug!("Subscription to non-whitelisted topic rejected");
                    self.inbound_protocol.stream_closed();
                    return;
                }

                if self.inbound_subscriptions.contains(&subscription.topic_id) {
                    tracing::debug!("Subscription to a currently subscribed topic rejected");
                    self.inbound_protocol.stream_closed();
                    return;
                }

                self.inbound_subscriptions
                    .insert(subscription.topic_id.clone());

                // The remote subscribed to this topic: set up the stream we
                // send the topic's pieces on.
                let (sender, receiver) = unbounded();
                let inbound = Inbound::new(
                    subscription.topic_id.clone(),
                    receiver,
                    Arc::clone(&self.used),
                    framed,
                );
                self.inbound_substreams.push(inbound);
                self.pending_events
                    .push_back(ConnectionHandlerEvent::NotifyBehaviour(Event::Subscribed {
                        topic_id: subscription.topic_id.clone(),
                        sender,
                    }));
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: (topic_id, substream),
                info: close_signal,
            }) => {
                // We subscribed to this topic: set up the stream we receive
                // the topic's pieces on, and signal the behaviour so it can
                // close the stream if the topic is unsubscribed.
                let outbound = Outbound::new(topic_id.clone(), substream, close_signal);
                self.outbound_substreams.push(outbound);
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError {
                error: StreamUpgradeError::NegotiationFailed,
                ..
            }) => {
                tracing::debug!("Remote peer does not support waggle on this connection");
            }
            _ => {}
        }
    }
}
