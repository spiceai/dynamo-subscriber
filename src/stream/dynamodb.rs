use super::{
    DynamodbSDKClient, Error,
    channel::{self, ConsumerChannel, ProducerChannel},
    types::{Lineages, Shard},
};
use crate::error::StreamResult;
use crate::types::initial_interator_type::InitialIteratorType;
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use std::collections::HashSet;
use std::{
    cmp,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep},
};
use tokio_stream::Stream;
use tracing::error;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(3);
const DEFAULT_BUFFER_SIZE: usize = 100;

/// The polling half of DynamoDB Streams.
#[derive(Debug)]
pub struct DynamodbStreamProducer<Client>
where
    Client: DynamodbSDKClient + 'static,
{
    table_name: String,
    stream_arn: String,
    shards: Option<Vec<Shard>>,
    channel: ProducerChannel,
    client: Arc<Client>,
    interval: Option<Duration>,
    sender: mpsc::Sender<StreamResult>,
    // Need to evict old shards from the set.
    seen_shard_ids: HashSet<String>,
}

impl<Client> DynamodbStreamProducer<Client>
where
    Client: DynamodbSDKClient + 'static,
{
    fn client(&self) -> Arc<Client> {
        Arc::clone(&self.client)
    }

    /// Get shards and shard iterator ids for first attempt to get records.
    async fn init(&mut self, initial: InitialIteratorType) -> Result<(), Error> {
        let stream_arn = self.client.get_stream_arn(self.table_name.clone()).await?;
        self.stream_arn = stream_arn;

        let shards = match initial {
            InitialIteratorType::Latest => {
                self.initialize_all_shards(ShardIteratorType::Latest).await
            }
            InitialIteratorType::TrimHorizon => {
                self.initialize_all_shards(ShardIteratorType::TrimHorizon)
                    .await
            }
        }?;

        self.shards = Some(shards);
        self.channel.send_init();

        Ok(())
    }

    async fn initialize_all_shards(
        &self,
        iterator_type: ShardIteratorType,
    ) -> Result<Vec<Shard>, Error> {
        let shards = self.client.get_all_shards(&self.stream_arn).await?;
        let shards = self.get_shard_iterators(shards, iterator_type).await;

        Ok(shards)
    }

    /// Get records and renew shards for next iteration.
    async fn iterate(&mut self) -> Result<Vec<StreamResult>, Error> {
        let shards_to_look_into = self.shards.take().unwrap_or_default();
        for shard in &shards_to_look_into {
            self.seen_shard_ids.insert(shard.id().to_string());
        }

        // This buffer prevents mpsc::channel from panic when passed zero as its argument.
        let buf = cmp::max(1, shards_to_look_into.len());
        let (tx, mut rx) = mpsc::channel::<(Option<Shard>, StreamResult)>(buf);

        // lineages based on shards we want to look into
        let lineages: Lineages = shards_to_look_into.clone().into();

        lineages.get_records(&self.client(), &tx);
        drop(tx);

        let mut shards: Vec<Shard> = vec![];
        let mut results: Vec<StreamResult> = vec![];

        while let Some((opt, shard_result)) = rx.recv().await {
            // These shards represent shards with non-empty iterator
            if let Some(shard) = opt {
                shards.push(shard);
            }

            results.push(shard_result);
        }

        let new_shards = self
            .client
            .get_all_shards(&self.stream_arn)
            .await?
            .into_iter()
            .filter(|shard| !self.seen_shard_ids.contains(shard.id()))
            .collect::<Vec<Shard>>();

        let mut new_shards = self
            .get_shard_iterators(new_shards, ShardIteratorType::TrimHorizon)
            .await;

        shards.append(&mut new_shards);
        self.shards = Some(shards);

        Ok(results)
    }

    /// Poll the DynamoDB Streams.
    async fn streaming(&mut self, initial: InitialIteratorType) {
        match self.init(initial).await {
            Ok(_) => {}
            Err(err) => {
                let _ = self.sender.send(Err(err)).await;
                return;
            }
        }

        loop {
            let stream_results = match self.iterate().await {
                Ok(results) => results,
                Err(err) => {
                    let _ = self.sender.send(Err(err)).await;
                    return;
                }
            };

            if self.channel.should_close() {
                return;
            }

            for result in stream_results {
                match result {
                    Ok(records) if records.is_empty() => continue, // Skip empty records
                    _ => {
                        if self.sender.send(result).await.is_err() {
                            return;
                        }
                    }
                }
            }

            if let Some(duration) = self.interval {
                sleep(duration).await;
            }
        }
    }

    /// Get and set shard iterator.
    async fn get_shard_iterators(
        &self,
        shards: Vec<Shard>,
        shard_iterator_type: ShardIteratorType,
    ) -> Vec<Shard> {
        // The buffer size must be positive (not zero).
        let buf = cmp::max(1, shards.len());
        let (tx, mut rx) = mpsc::channel::<Shard>(buf);
        let mut output: Vec<Shard> = vec![];
        let client = self.client();

        for shard in shards {
            let tx = tx.clone();
            let client = Arc::clone(&client);
            let stream_arn = self.stream_arn.clone();
            let shard_iterator_type = shard_iterator_type.clone();

            tokio::spawn(async move {
                let result = client.get_shard_with_iterator(
                    stream_arn,
                    shard.id(),
                    shard.parent_shard_id(),
                    &shard_iterator_type,
                    None,
                );

                let shard = ok_or_return!(result.await, |err| {
                    error!("Unexpected error during getting shard iterator: {err}");
                });

                if let Err(err) = tx.send(shard).await {
                    error!("Unexpected error during sending shard: {err}");
                }
            });
        }

        drop(tx);

        while let Some(shard) = rx.recv().await {
            output.push(shard);
        }

        output
    }
}

/// Represent DynamoDB Stream.
///
/// This struct receives DynamoDB Stream records from polling half and emit them as Rust Stream.
#[derive(Debug)]
pub struct DynamodbStream {
    receiver: mpsc::Receiver<StreamResult>,
    channel: Option<ConsumerChannel>,
}

impl DynamodbStream {
    /// Get [`ConsumerChannel`] as communication channel to the stream.
    ///
    /// Once you take a channel from this method, you can't take it anymore from the same channel
    /// because this method also passes the ownership of the channel.
    ///
    /// ```rust,no_run
    /// use aws_config::BehaviorVersion;
    /// use dynamo_subscriber as subscriber;
    ///
    /// # async fn wrapper() {
    /// # let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    /// # let client = subscriber::SDKClient::new(&config);
    /// let mut stream = subscriber::stream::builder()
    ///     .client(client)
    ///     .table_name("People")
    ///     .build();
    /// let channel = stream.take_channel();
    /// assert!(channel.is_some());
    ///
    /// let channel = stream.take_channel();
    /// assert!(channel.is_none());
    /// # }
    /// ```
    pub fn take_channel(&mut self) -> Option<ConsumerChannel> {
        self.channel.take()
    }
}

impl Stream for DynamodbStream {
    type Item = StreamResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for DynamodbStream {
    fn drop(&mut self) {
        self.receiver.close();
        if let Some(mut channel) = self.take_channel() {
            channel.close(|| {});
        }
    }
}

// impl AsRef<mpsc::Receiver<Vec<Record>>> for DynamodbStream {
//     fn as_ref(&self) -> &mpsc::Receiver<Vec<Record>> {
//         &self.receiver
//     }
// }
//
// impl AsMut<mpsc::Receiver<Vec<Record>>> for DynamodbStream {
//     fn as_mut(&mut self) -> &mut mpsc::Receiver<Vec<Record>> {
//         &mut self.receiver
//     }
// }

/// A builder for [`DynamodbStream`].
#[derive(Debug)]
pub struct DynamodbStreamBuilder<Client>
where
    Client: DynamodbSDKClient + 'static,
{
    table_name: String,
    client: Client,
    interval: Option<Duration>,
    buffer: usize,
    initial_iterator_type: InitialIteratorType,
}

impl<Client> DynamodbStreamBuilder<Client>
where
    Client: DynamodbSDKClient + 'static,
{
    /// Create a new `DynamodbStreamBuilder`.
    #[must_use]
    pub fn new(
        client: Client,
        table_name: String,
        initial_iterator_type: InitialIteratorType,
    ) -> Self {
        Self {
            client,
            table_name,
            interval: Some(DEFAULT_INTERVAL),
            buffer: DEFAULT_BUFFER_SIZE,
            initial_iterator_type,
        }
    }

    /// Set interval between polling attempts. When None is provided there are no intervals between
    /// polling iterations.
    ///
    /// Setting any interval is optional. If you omit calling this method,
    /// `3 seconds` is used as default value.
    pub fn interval(self, interval: Option<Duration>) -> Self {
        Self { interval, ..self }
    }

    /// Set the buffer for [`tokio::sync::mpsc::channel`](tokio::sync::mpsc::channel).
    ///
    /// The stream records are stored up to the buffer size unless the records are consumed.
    /// Once the buffer is full, attempts to receive records from the DynamoDB Streams will
    /// wait until the records is consumed.
    ///
    /// This method will panic when given zero as buffer size.
    ///
    /// Setting buffer size is optional. If you omit calling this method,
    /// `100` is used as default value.
    pub fn buffer(self, buffer: usize) -> Self {
        if buffer == 0 {
            panic!("buffer must be positive.");
        }

        Self { buffer, ..self }
    }

    /// Consumes the builder and constructs a [`DynamodbStream`].
    ///
    /// This method will panic if no table name is set or no client is set.
    pub fn build(self) -> DynamodbStream {
        let (c_half, rx) = self.build_producer();

        DynamodbStream {
            receiver: rx,
            channel: Some(c_half),
        }
    }

    fn build_producer(self) -> (ConsumerChannel, mpsc::Receiver<StreamResult>) {
        let (p_half, c_half) = channel::new();
        let (tx_mpsc, rx_mpsc) = mpsc::channel::<StreamResult>(self.buffer);

        let mut producer = DynamodbStreamProducer {
            table_name: self.table_name,
            stream_arn: String::new(),
            shards: None,
            channel: p_half,
            client: Arc::new(self.client),
            interval: self.interval,
            sender: tx_mpsc,
            seen_shard_ids: HashSet::new(),
        };

        let initial_iterator_type = self.initial_iterator_type;

        tokio::spawn(async move {
            producer.streaming(initial_iterator_type).await;
        });

        (c_half, rx_mpsc)
    }
}
