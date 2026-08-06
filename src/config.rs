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
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

/// Dissemination parameters for a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfig {
    /// Number of connected peers to randomly select when publishing a new piece.
    publish_fanout: NonZeroUsize,
    /// Number of connected peers to gossip metadata to upon receiving a new piece.
    gossip_fanout: NonZeroUsize,
}

impl TopicConfig {
    /// Creates new dissemination parameters with the given fanouts.
    pub fn new(publish_fanout: NonZeroUsize, gossip_fanout: NonZeroUsize) -> Self {
        Self {
            publish_fanout,
            gossip_fanout,
        }
    }

    /// The number of connected peers to randomly select when publishing a new piece.
    pub fn publish_fanout(&self) -> NonZeroUsize {
        self.publish_fanout
    }

    /// Sets the number of connected peers to randomly select when publishing
    /// a new piece.
    pub fn set_publish_fanout(mut self, value: NonZeroUsize) -> Self {
        self.publish_fanout = value;
        self
    }

    /// The number of connected peers to gossip metadata to upon receiving a
    /// new piece.
    pub fn gossip_fanout(&self) -> NonZeroUsize {
        self.gossip_fanout
    }

    /// Sets the number of connected peers to gossip metadata to upon
    /// receiving a new piece.
    pub fn set_gossip_fanout(mut self, value: NonZeroUsize) -> Self {
        self.gossip_fanout = value;
        self
    }
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            publish_fanout: NonZeroUsize::new(50).expect("50 is not zero"),
            gossip_fanout: NonZeroUsize::new(50).expect("50 is not zero"),
        }
    }
}

/// Configuration for a Waggle protocol instance.
#[derive(Debug, Clone)]
pub struct Config {
    max_connection_queue_limit: usize,
    max_topic_subscription: usize,
    max_protobuf_size: usize,
    max_cached_objects: NonZeroUsize,
    object_lifetime: Duration,
    pub(crate) topic_whitelist: Option<Arc<HashSet<Vec<u8>>>>,
    /// Dissemination parameters, keyed by topic.
    topic_configs: HashMap<Vec<u8>, TopicConfig>,
    /// Fallback dissemination parameters for topics without a specific configuration.
    default_topic_config: TopicConfig,
}

impl Config {
    /// The maximum number of messages queued outbound on one connection.
    ///
    /// When the connection queue is full, no further messages are sent and
    /// the peer is reported as slow.
    ///
    /// Defaults to 5000.
    pub fn max_connection_queue_limit(&self) -> usize {
        self.max_connection_queue_limit
    }

    /// Sets the maximum number of messages queued outbound on one connection.
    pub fn set_max_connection_queue_limit(mut self, len: usize) -> Self {
        self.max_connection_queue_limit = len;
        self
    }

    /// The maximum number of topics a single peer may subscribe to.
    ///
    /// Defaults to 50.
    pub fn max_topic_subscription(&self) -> usize {
        self.max_topic_subscription
    }

    /// Sets the maximum number of topics a single peer may subscribe to.
    pub fn set_max_topic_subscription(mut self, value: usize) -> Self {
        self.max_topic_subscription = value;
        self
    }

    /// The maximum size in bytes of a single protobuf message on the wire.
    ///
    /// Defaults to 65536 (64 KB).
    pub fn max_protobuf_size(&self) -> usize {
        self.max_protobuf_size
    }

    /// Sets the maximum size in bytes of a single protobuf message on the wire.
    pub fn set_max_protobuf_size(mut self, value: usize) -> Self {
        self.max_protobuf_size = value;
        self
    }

    /// The topic subscription whitelist.
    ///
    /// When it is `None`, all topic subscriptions are accepted.
    pub fn topic_whitelist(&self) -> Option<&HashSet<Vec<u8>>> {
        self.topic_whitelist.as_deref()
    }

    /// Sets the topic subscription whitelist.
    pub fn set_topic_whitelist(mut self, whitelist: Option<HashSet<Vec<u8>>>) -> Self {
        self.topic_whitelist = whitelist.map(Arc::new);
        self
    }

    /// The maximum number of published objects cached locally per topic for
    /// reconciliation with peers that are missing pieces.
    ///
    /// Defaults to 100.
    pub fn max_cached_objects(&self) -> NonZeroUsize {
        self.max_cached_objects
    }

    /// Sets the maximum number of published objects cached locally per topic.
    pub fn set_max_cached_objects(mut self, value: NonZeroUsize) -> Self {
        self.max_cached_objects = value;
        self
    }

    /// How long a cached object is kept locally for reconciliation.
    ///
    /// After this time, the object is eligible for eviction.
    ///
    /// Defaults to 60 seconds.
    pub fn object_lifetime(&self) -> Duration {
        self.object_lifetime
    }

    /// Sets how long a cached object is kept locally for reconciliation.
    pub fn set_object_lifetime(mut self, value: Duration) -> Self {
        self.object_lifetime = value;
        self
    }

    /// The dissemination parameters for `topic`.
    ///
    /// Returns the topic's specific parameters, or the default parameters
    /// when the topic has none.
    pub fn topic_config(&self, topic: &[u8]) -> &TopicConfig {
        self.topic_configs
            .get(topic)
            .unwrap_or(&self.default_topic_config)
    }

    /// Sets the dissemination parameters for `topic`.
    pub fn set_topic_config(mut self, topic: Vec<u8>, config: TopicConfig) -> Self {
        self.topic_configs.insert(topic, config);
        self
    }

    /// Sets the fallback dissemination parameters.
    pub fn set_default_topic_config(mut self, config: TopicConfig) -> Self {
        self.default_topic_config = config;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_connection_queue_limit: 5000,
            max_topic_subscription: 50,
            topic_whitelist: None,
            max_protobuf_size: 65536,
            max_cached_objects: NonZeroUsize::new(100).expect("100 is non-zero"),
            object_lifetime: Duration::from_secs(60),
            topic_configs: HashMap::new(),
            default_topic_config: TopicConfig::default(),
        }
    }
}
