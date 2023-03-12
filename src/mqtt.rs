// Copyright (c) 2020, Jason Fritcher <jkf@wolfnet.org>
// All rights reserved.

use std::process;

use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use url::Url;

use mqtt_async_client::{
    client::{Client, KeepAlive, Publish as PublishOpts, QoS},
    Result,
};

#[allow(unused_imports)]
use log::{trace, debug, info, warn, error};

use crate::common::{MqttArgs, WFSource};

fn client_from_args(args: MqttArgs) -> Result<Client> {
    let mut b = Client::builder();
    b.set_url(
        format!("mqtt://{0}:{1}", &args.hostname, args.port)
            .parse::<Url>()
            .map_err(|e| e.to_string())?
    )?
    .set_client_id(args.client_id)
    .set_keep_alive(KeepAlive::from_secs(30))
    .set_connect_retry_delay(Duration::from_secs(1))
    .set_packet_buffer_len(1024)
    .set_automatic_connect(true);

    // Set auth creds, if specified
    if args.username.is_some() && args.password.is_some() {
        b.set_username(args.username)
            .set_password(args.password.map(|s| s.as_bytes().to_vec()));
    }

    b.build()
}

pub fn mqtt_publish_raw_message(
    publisher_tx: &mpsc::UnboundedSender<(String, String)>,
    topic_base: &str,
    msg_source: &WFSource,
    msg_str: &str,
) {
    let topic_suffix = match msg_source {
        WFSource::UDP => "udp_raw",
        WFSource::WS => "ws_raw",
    };
    let msg_topic = format!("{}/{}", topic_base, topic_suffix);

    mqtt_publish_message(publisher_tx, &msg_topic, msg_str);
}

pub fn mqtt_publish_message(
    publisher_tx: &mpsc::UnboundedSender<(String, String)>,
    msg_topic: &str,
    msg_str: &str,
) {
    if let Err(err) = publisher_tx.send((msg_topic.to_string(), String::from(msg_str))) {
        error!("Failed to add message to publisher_tx: {}", err);
    }
}

pub async fn mqtt_publisher(
    mut publisher_rx: mpsc::UnboundedReceiver<(String, String)>,
    args: MqttArgs,
) {
    let mut client = match client_from_args(args) {
        Ok(client) => client,
        Err(err) => { error!("Failed to build mqtt client: {}", err); process::abort(); }
    };

    while let Err(err) = client.connect().await {
        error!("Failed to connect to mqtt server: {}", err);
        sleep(Duration::from_secs(1)).await;
    }

    while let Some((topic, payload)) = publisher_rx.recv().await {
        trace!("Received message: {} | {}", topic, payload);
        let mut p = PublishOpts::new(topic, payload.as_bytes().to_vec());
        p.set_qos(QoS::AtMostOnce);
        p.set_retain(false);
        if let Err(err) = client.publish(&p).await {
            error!("Failed to publish message: {}", err);
        }
    }
}
