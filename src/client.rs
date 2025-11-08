use crate::client_sdk::{DynamodbSDKClient, SDKClient};
use crate::stream::{DynamodbStream, DynamodbStreamBuilder};
use crate::types::initial_interator_type::InitialIteratorType;
use aws_config::SdkConfig;
use std::time::Duration;

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
pub struct Client<SDKClient>
where
    SDKClient: DynamodbSDKClient + 'static,
{
    sdk_client: SDKClient,
    table_name: String,
    interval: Option<Duration>,
    buffer: usize,
}

const DEFAULT_BUFFER_SIZE: usize = 100;
const DEFAULT_INTERVAL: Duration = Duration::from_secs(3);

impl Client<SDKClient>
where
    SDKClient: DynamodbSDKClient + 'static,
{
    #[must_use]
    pub fn builder(sdk_config: SdkConfig, table_name: String) -> ClientBuilder {
        ClientBuilder::new(sdk_config, table_name)
    }

    #[must_use]
    pub fn new(client: SDKClient, table_name: String) -> Self {
        Self {
            sdk_client: client,
            table_name,
            interval: Some(DEFAULT_INTERVAL),
            buffer: DEFAULT_BUFFER_SIZE,
        }
    }

    #[must_use]
    pub fn stream_from_latest(&self) -> DynamodbStream {
        DynamodbStreamBuilder::new(
            self.sdk_client.clone(),
            self.table_name.clone(),
            InitialIteratorType::Latest,
        )
        .interval(self.interval)
        .buffer(self.buffer)
        .build()
    }

    #[must_use]
    pub fn stream_from_trim_horizon(&self) -> DynamodbStream {
        DynamodbStreamBuilder::new(
            self.sdk_client.clone(),
            self.table_name.clone(),
            InitialIteratorType::TrimHorizon,
        )
        .interval(self.interval)
        .buffer(self.buffer)
        .build()
    }
}

#[derive(Debug)]
pub struct ClientBuilder {
    sdk_config: SdkConfig,
    table_name: String,
    interval: Option<Duration>,
    buffer: usize,
}

impl ClientBuilder {
    pub fn new(sdk_config: SdkConfig, table_name: String) -> Self {
        Self {
            sdk_config,
            table_name,
            interval: Some(DEFAULT_INTERVAL),
            buffer: DEFAULT_BUFFER_SIZE,
        }
    }

    #[must_use]
    pub fn interval(mut self, interval: Option<Duration>) -> Self {
        self.interval = interval;
        self
    }

    #[must_use]
    pub fn buffer(mut self, buffer: usize) -> Self {
        assert!((buffer != 0), "buffer must be positive");
        self.buffer = buffer;
        self
    }

    pub fn build(self) -> Client<SDKClient> {
        Client {
            sdk_client: SDKClient::new(&self.sdk_config),
            table_name: self.table_name,
            interval: self.interval,
            buffer: self.buffer,
        }
    }
}
