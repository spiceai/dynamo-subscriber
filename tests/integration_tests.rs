mod common;

use common::{pk, put_item, setup, teardown, wait_until_initialized};
use dynamo_subscriber::ClientBuilder;
use tokio_stream::StreamExt;

#[tokio::test]
async fn it_can_be_consumed_as_stream() {
    let config = setup().await;

    let table_name = config.table_name();
    let sdk_config = config.aws_sdk_config();

    let client = ClientBuilder::new(sdk_config.clone(), table_name.to_string())
        .interval(None)
        .build();

    let mut stream = client.stream_from_trim_horizon();

    let channel_opt = stream.take_channel();
    assert!(channel_opt.is_some());

    let mut channel = channel_opt.unwrap();
    wait_until_initialized(&mut channel).await;

    // Put item to table.
    // First attempt
    put_item("pk0", sdk_config).await;
    // Second attempt
    put_item("pk1", sdk_config).await;

    // Receive dynamodb stream
    // First iteration (from first attempt)
    let records_opt = stream.next().await;
    assert!(records_opt.is_some());

    let records = records_opt.unwrap();
    assert_eq!(records.len(), 1);

    let record = records.get(0).unwrap();
    assert_eq!(pk(record), "pk0");

    // Second iteration (from second attempt)
    let records_opt = stream.next().await;
    assert!(records_opt.is_some());

    let records = records_opt.unwrap();
    assert_eq!(records.len(), 1);

    let record = records.get(0).unwrap();
    assert_eq!(pk(record), "pk1");

    // Send `Stop polling` event to the polling half of the stream.
    channel.close(|| {});

    // Stream is now closed.
    let records_opt = stream.next().await;
    assert!(records_opt.is_none());

    teardown(sdk_config).await;
}
