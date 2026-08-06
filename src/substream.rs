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
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, ARISING FROM, OUT OF
// OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use async_channel::{Receiver, Sender, TrySendError, bounded};
use asynchronous_codec::Framed;
use futures::{AsyncWriteExt, Sink, Stream, StreamExt};
use libp2p::Stream as Libp2pStream;

use crate::handler::{Event, Received};
use crate::protocol::{PieceCodec, proto};

/// A per-topic outbound subscription stream.
///
/// Receives the pieces of one topic from our subscription stream and
/// surfaces them as `Received` events. Ends when the local node unsubscribes
/// (the close-signal channel closes) or the peer closes the stream.
#[derive(Debug)]
pub struct Outbound {
    topic: Vec<u8>,
    /// Clone of the topic's close signal. The stream ends when its channel
    /// closes.
    close_signal: Pin<Box<Receiver<()>>>,
    state: OutboundState,
}

impl Outbound {
    /// Creates a new `Outbound` stream for a topic.
    ///
    /// `framed` is the negotiated subscription stream. `close_signal` is a
    /// clone of the topic's close signal; it wakes and ends the stream when
    /// the channel closes on unsubscribe.
    pub(crate) fn new(
        topic: Vec<u8>,
        framed: Framed<Libp2pStream, PieceCodec>,
        close_signal: Receiver<()>,
    ) -> Self {
        Self {
            topic,
            close_signal: Box::pin(close_signal),
            state: OutboundState::Active(framed),
        }
    }
}

/// State machine for a single outbound topic stream.
pub enum OutboundState {
    /// The stream is open and receiving pieces.
    Active(Framed<Libp2pStream, PieceCodec>),
    /// The stream is being closed.
    Closing(Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>),
    /// The stream is closed.
    Closed,
    /// Temporary state used during state transitions. It allows moving values
    /// out of the enum. The application never observes this variant during
    /// normal operation.
    Poisoned,
}

impl std::fmt::Debug for OutboundState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active(arg0) => f.debug_tuple("Active").field(arg0).finish(),
            Self::Closing(_) => f.debug_tuple("Closing").finish(),
            Self::Closed => f.debug_tuple("Closed").finish(),
            Self::Poisoned => write!(f, "Poisoned"),
        }
    }
}

impl Stream for Outbound {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let state = std::mem::replace(&mut this.state, OutboundState::Poisoned);
            match state {
                OutboundState::Active(mut framed) => {
                    // Local unsubscribe signal: close the stream quietly.
                    if this.close_signal.as_mut().poll_next(cx).is_ready() {
                        let mut stream = framed.into_inner();
                        this.state =
                            OutboundState::Closing(Box::pin(async move { stream.close().await }));
                        continue;
                    }

                    match framed.poll_next_unpin(cx) {
                        Poll::Ready(Some(result)) => match result {
                            Ok(piece) => {
                                this.state = OutboundState::Active(framed);
                                return Poll::Ready(Some(Event::Received(Received {
                                    topic_id: this.topic.clone(),
                                    object_id: piece.object_id,
                                    metadata: piece.pieces_metadata,
                                    pieces: piece.pieces,
                                })));
                            }
                            Err(err) => {
                                // TODO: Report a protocol violation when data is invalid?
                                tracing::debug!("Failed to decode piece: {err}");
                                let mut stream = framed.into_inner();
                                this.state = OutboundState::Closing(Box::pin(async move {
                                    stream.close().await
                                }));
                            }
                        },
                        // The peer closed the stream.
                        Poll::Ready(None) => {
                            let mut stream = framed.into_inner();
                            this.state =
                                OutboundState::Closing(Box::pin(
                                    async move { stream.close().await },
                                ));
                        }
                        Poll::Pending => {
                            this.state = OutboundState::Active(framed);
                            return Poll::Pending;
                        }
                    }
                }
                OutboundState::Closing(mut pin) => {
                    let Poll::Ready(result) = pin.as_mut().poll(cx) else {
                        this.state = OutboundState::Closing(pin);
                        return Poll::Pending;
                    };
                    if let Err(err) = result {
                        tracing::debug!("Error closing outbound stream: {err}");
                    }
                    this.state = OutboundState::Closed;
                    return Poll::Ready(None);
                }
                OutboundState::Closed => {
                    this.state = OutboundState::Closed;
                    return Poll::Ready(None);
                }
                OutboundState::Poisoned => {
                    panic!("entered poisoned outbound stream state")
                }
            }
        }
    }
}

/// Fan-out close signal for the topic subscription streams.
///
/// Holds one producer `Sender`. Each connection receives a clone of the
/// receiver, so all the topic's streams close together when the signal is
/// dropped on unsubscribe.
#[derive(Debug)]
pub(crate) struct CloseSignal {
    /// Keeps the channel open. The signal is dropped on unsubscribe; when the
    /// `Sender` drops, the channel closes and every receiver wakes.
    #[allow(unused)]
    sender: Sender<()>,
    /// Source for the per-connection receiver clones.
    receiver: Receiver<()>,
}

impl CloseSignal {
    /// Creates a new close signal for a topic.
    ///
    /// The signal never carries a message. The channel closes only when the
    /// `sender` is dropped.
    pub(crate) fn new() -> Self {
        let (sender, receiver) = bounded(1);
        Self { sender, receiver }
    }

    /// Returns a receiver clone for one connection.
    pub(crate) fn receiver(&self) -> Receiver<()> {
        self.receiver.clone()
    }
}
/// Errors from the outbound hub when dispatching a message.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SendError {
    /// The connection's outbound queue reached its limit.
    #[error("connection outbound queue full")]
    Full,
    /// The channel for the topic is closed.
    #[error("outbound channel closed")]
    Closed,
    /// There is no outbound stream for the topic.
    #[error("no outbound stream for the topic")]
    UnknownTopic,
}

/// Per-connection dispatcher for outbound piece messages.
///
/// Routes the published pieces to the topic's stream writers under one
/// connection-wide queue budget.
#[derive(Debug)]
pub(crate) struct DispatchHub {
    /// The connection's outbound queue limit.
    limit: usize,
    /// The number of messages currently queued on the connection, shared with
    /// the outbound receivers.
    used: Arc<AtomicUsize>,
    /// The per-topic senders, keyed by topic.
    pub(crate) senders: HashMap<Vec<u8>, Sender<proto::ObjectPieces>>,
}

impl DispatchHub {
    /// Creates an empty hub with the given queue `limit`.
    pub(crate) fn new(limit: usize, used: Arc<AtomicUsize>) -> Self {
        Self {
            limit,
            used,
            senders: HashMap::new(),
        }
    }

    /// Queues `message` on the outbound channel for `topic`.
    ///
    /// Returns `Full` when the connection's queue reached its limit, `Closed`
    /// when the channel is closed, and `UnknownTopic` when there is no
    /// channel for the topic.
    pub(crate) fn try_send(
        &self,
        topic: &[u8],
        message: proto::ObjectPieces,
    ) -> Result<(), SendError> {
        let Some(sender) = self.senders.get(topic) else {
            return Err(SendError::UnknownTopic);
        };
        let n = self.used.fetch_add(1, Ordering::SeqCst);
        if n >= self.limit {
            self.used.fetch_sub(1, Ordering::SeqCst);
            return Err(SendError::Full);
        }
        match sender.try_send(message) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.used.fetch_sub(1, Ordering::SeqCst);
                Err(SendError::Full)
            }
            Err(TrySendError::Closed(_)) => {
                self.used.fetch_sub(1, Ordering::SeqCst);
                Err(SendError::Closed)
            }
        }
    }
}

/// A per-topic inbound stream writer.
///
/// Reads the messages of one topic from its channel and writes them to the
/// subscription stream the remote peer opened for that topic. The future
/// completes when the channel closes or a write fails. It completes with
/// `Some(Unsubscribed)` when the remote closed the stream, or `None` when
/// the local side stopped it.
pub struct Inbound {
    topic: Vec<u8>,
    receiver: Pin<Box<Receiver<proto::ObjectPieces>>>,
    used: Arc<AtomicUsize>,
    framed: Pin<Box<Framed<Libp2pStream, PieceCodec>>>,
    state: InboundState,
}

impl Inbound {
    /// Creates a new `Inbound` stream writer for a topic.
    ///
    /// `receiver` is the channel with the messages to send on `topic`.
    /// `used` is this connection's outbound queue counter, shared with the
    /// senders. `framed` is the subscription stream the remote peer opened.
    pub(crate) fn new(
        topic: Vec<u8>,
        receiver: Receiver<proto::ObjectPieces>,
        used: Arc<AtomicUsize>,
        framed: Framed<Libp2pStream, PieceCodec>,
    ) -> Self {
        Self {
            topic,
            receiver: Box::pin(receiver),
            used,
            framed: Box::pin(framed),
            state: InboundState::WaitingMessage,
        }
    }
}

impl Drop for Inbound {
    fn drop(&mut self) {
        // Messages left buffered in the channel were counted on push but never
        // popped; release their budget so the connection's counter stays exact.
        self.used.fetch_sub(self.receiver.len(), Ordering::SeqCst);
    }
}

enum InboundState {
    /// Waiting for the next message from the channel.
    WaitingMessage,
    /// A message was received; watching for sink readiness.
    WaitingReady { message: proto::ObjectPieces },
    /// A message was accepted; the sink is flushing it.
    Flushing,
    /// The writer ended.
    Closed,
    /// Temporary state used during state transitions.
    Poisoned,
}

impl Future for Inbound {
    type Output = Option<Event>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let state = std::mem::replace(&mut this.state, InboundState::Poisoned);
            match state {
                InboundState::WaitingMessage => match this.receiver.as_mut().poll_next(cx) {
                    Poll::Ready(Some(message)) => {
                        this.used.fetch_sub(1, Ordering::SeqCst);
                        this.state = InboundState::WaitingReady { message };
                    }
                    // The channel is closed and drained: the local side stopped it.
                    Poll::Ready(None) => {
                        tracing::debug!(topic = ?this.topic, "Outbound channel closed");
                        this.state = InboundState::Closed;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => {
                        this.state = InboundState::WaitingMessage;
                        return Poll::Pending;
                    }
                },
                InboundState::WaitingReady { message } => {
                    match Sink::poll_ready(this.framed.as_mut(), cx) {
                        Poll::Ready(Ok(())) => {
                            match Sink::start_send(this.framed.as_mut(), message) {
                                Ok(()) => this.state = InboundState::Flushing,
                                Err(err) => {
                                    tracing::debug!(
                                        topic = ?this.topic,
                                        "Failed to send message on inbound stream: {err}"
                                    );
                                    this.state = InboundState::Closed;
                                    return Poll::Ready(Some(Event::Unsubscribed {
                                        topic_id: this.topic.clone(),
                                    }));
                                }
                            }
                        }
                        Poll::Ready(Err(err)) => {
                            tracing::debug!(topic = ?this.topic, "Inbound stream error: {err}");
                            this.state = InboundState::Closed;
                            return Poll::Ready(Some(Event::Unsubscribed {
                                topic_id: this.topic.clone(),
                            }));
                        }
                        Poll::Pending => {
                            this.state = InboundState::WaitingReady { message };
                            return Poll::Pending;
                        }
                    }
                }
                InboundState::Flushing => match Sink::poll_flush(this.framed.as_mut(), cx) {
                    Poll::Ready(Ok(())) => this.state = InboundState::WaitingMessage,
                    Poll::Ready(Err(err)) => {
                        tracing::debug!(
                            topic = ?this.topic,
                            "Failed to flush inbound stream: {err}"
                        );
                        this.state = InboundState::Closed;
                        return Poll::Ready(Some(Event::Unsubscribed {
                            topic_id: this.topic.clone(),
                        }));
                    }
                    Poll::Pending => {
                        this.state = InboundState::Flushing;
                        return Poll::Pending;
                    }
                },
                InboundState::Closed => return Poll::Ready(None),
                InboundState::Poisoned => {
                    panic!("entered poisoned inbound stream state")
                }
            }
        }
    }
}
