use std::{
    error::Error,
    net::IpAddr,
    str,
};

use tokio::sync::mpsc;

use futures::join;

use serde_json::Value;

#[allow(unused_imports)]
use log::{trace, debug, info, warn, error};
use simple_logger;

mod common;
mod udp;
mod websocket;
use common::{WFMessage, WFAuthMethod};
use udp::udp_collector;
use websocket::websocket_collector;

// WeatherFlow constants
pub const WF_API_KEY: &str = "20c70eae-e62f-4d3b-b3a4-8586e90f3ac8";
pub const WF_AUTH_TOKEN: &str = "12345678-1234-1234-1234-123456789abc";
pub const WF_STATION_ID: u16 = 19992;
pub const WF_DEVICE_ID: u32 = 67708;


async fn message_consumer(mut collector_rx: mpsc::UnboundedReceiver<WFMessage>,
                          _publisher_tx: mpsc::UnboundedSender<()>,
                          ignored_msg_types: Vec<String>) {
    // Wait for messages from the consumers to process
    while let Some(msg) = collector_rx.recv().await {
        let msg_str = match str::from_utf8(&msg.message) {
            Err(err) => {
                error!("Error in str::from_utf8, ignoring message: {}", err);
                continue;
            },
            Ok(msg) => msg,
        };
        let msg_json: Value = match serde_json::from_str(&msg_str) {
            Err(err) => {
                error!("Error parsing json, skipping message: {}", err);
                continue;
            },
            Ok(msg) => msg,
        };
        if !msg_json.is_object() {
            warn!("{:?} message not valid json object, skipping", msg.source);
            continue;
        }
        let msg_obj = msg_json.as_object().unwrap();
        let msg_type = msg_obj
                       .get("type").unwrap()
                       .as_str().unwrap();

        // Ignore message types we're not interested in
        if ignored_msg_types.iter().any(|x| x == &msg_type ) {
            continue;
        }

        println!("{:?} - {}", msg.source, &msg_str);
    }
}

async fn mqtt_publisher(mut publisher_rx: mpsc::UnboundedReceiver<()>) {
    while let Some(msg) = publisher_rx.recv().await {
        debug!("Received message: {:?}", msg);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Debug).unwrap();

    // Channel for consumer to processor messaging
    let (collector_tx, consumer_rx) = mpsc::unbounded_channel::<WFMessage>();

    // Channel to mqtt publishing task
    let (publisher_tx, publisher_rx) = mpsc::unbounded_channel::<()>();

    // Vec of IpAddrs of senders we're interested in
    #[allow(unused_mut)]
    let mut senders: Vec<IpAddr> = Vec::new();
    //senders.push("10.1.3.3".parse()?);

    // Message types to ignore
    let mut ignored_msg_types: Vec<String> = Vec::new();
    ignored_msg_types.push(String::from("rapid_wind"));
    ignored_msg_types.push(String::from("light_debug"));

    let auth_method = WFAuthMethod::AUTHTOKEN(WF_AUTH_TOKEN.to_string());

    // Spawn tasks for the collectors / consumer
    let udp_task = tokio::spawn(udp_collector(collector_tx.clone(), senders));
    let ws_task = tokio::spawn(websocket_collector(collector_tx, auth_method));
    let msg_consumer_task = tokio::spawn(message_consumer(consumer_rx, publisher_tx, ignored_msg_types));
    let mqtt_publisher_task = tokio::spawn(mqtt_publisher(publisher_rx));

    // Wait for spawned tasks to complete, which should not occur, so effectively hang the task.
    join!(udp_task, ws_task, msg_consumer_task, mqtt_publisher_task);

    Ok(())
}