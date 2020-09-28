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
mod mqtt;
mod udp;
mod websocket;
use common::{WFMessage, WFAuthMethod, WFSource, MqttArgs, WsArgs};
use mqtt::{mqtt_publisher,mqtt_publish_raw_message};
use udp::udp_collector;
use websocket::websocket_collector;

// WeatherFlow constants
const WF_API_KEY: &str = "20c70eae-e62f-4d3b-b3a4-8586e90f3ac8";
const WF_AUTH_TOKEN: &str = "12345678-1234-1234-1234-123456789abc";
const WF_STATION_ID: u32 = 690;
const WF_DEVICE_ID: u32 = 1110;

// MQTT args - to be replaced by cli args
const MQTT_HOST: &str = "10.1.1.1";
const MQTT_PORT: u16 = 1883;
const MQTT_USER: &str = "username";
const MQTT_PASS: &str = "password";
const MQTT_CLIENT_ID: &str = "client-id";


fn get_hub_sn_from_msg(msg_obj: &serde_json::Value) -> Option<&str> {
    let msg_map = msg_obj.as_object()?;
    if msg_map.contains_key("type") && msg_map.get("type")? == "hub_status" {
        msg_map.get("serial_number")?.as_str()
    } else {
        msg_map.get("hub_sn")?.as_str()
    }
}

async fn message_consumer(mut collector_rx: mpsc::UnboundedReceiver<WFMessage>,
                          publisher_tx: mpsc::UnboundedSender<(String, String)>,
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
            warn!("{:?} message is not an expected json object, skipping", msg.source);
            continue;
        }
        let msg_obj = msg_json.as_object().unwrap();
        let msg_type = match msg_obj.get("type") {
            Some(msg_type) => msg_type.as_str().unwrap(),
            None => {
                error!("type key was not found in message, skipping.");
                debug!("msg_obj = {:?}", msg_obj);
                continue;
            }
        };

        // Ignore message types we're not interested in
        if ignored_msg_types.iter().any(|x| x == &msg_type ) {
            continue;
        }

        // Ignore cached WS summary messages
        if msg.source == WFSource::WS &&
           msg_obj.get("source").unwrap().as_str().unwrap() == "cache" {
            debug!("Ignoring WS cached summary message");
            continue;
        }

        // Get the Hub serial number and make topic base
        let hub_sn = get_hub_sn_from_msg(&msg_json).unwrap();
        let topic_base = format!("weatherflow/{}", hub_sn);

        // Publish raw message to mqtt
        mqtt_publish_raw_message(&publisher_tx, topic_base, msg.source, &msg_str);

        //println!("{:?} - {}", msg.source, &msg_str);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Debug).unwrap();

    // Channel for consumer to processor messaging
    let (collector_tx, consumer_rx) = mpsc::unbounded_channel::<WFMessage>();

    // Channel to mqtt publishing task
    let (publisher_tx, publisher_rx) = mpsc::unbounded_channel::<(String, String)>();

    // Vec of IpAddrs of senders we're interested in
    let mut senders: Vec<IpAddr> = Vec::new();
    senders.push("10.1.3.4".parse()?);

    // Collect args for MQTT
    let mqtt_args = MqttArgs {
        hostname: String::from(MQTT_HOST),
        port: MQTT_PORT,
        username: Some(String::from(MQTT_USER)),
        password: Some(String::from(MQTT_PASS)),
        client_id: Some(String::from(MQTT_CLIENT_ID)),
    };
    
    // Message types to ignore
    let mut ignored_msg_types: Vec<String> = Vec::new();
    ignored_msg_types.push(String::from("rapid_wind"));
    ignored_msg_types.push(String::from("light_debug"));

    let ws_args = WsArgs {
        auth_method: WFAuthMethod::AUTHTOKEN(String::from(WF_AUTH_TOKEN)),
        station_id: None,
        device_id: Some(WF_DEVICE_ID),
    };

    // Spawn tasks for the collectors / consumer
    let udp_task = tokio::spawn(udp_collector(collector_tx.clone(), senders));
    let ws_task = tokio::spawn(websocket_collector(collector_tx, ws_args));
    let msg_consumer_task = tokio::spawn(message_consumer(consumer_rx, publisher_tx, ignored_msg_types));
    let mqtt_publisher_task = tokio::spawn(mqtt_publisher(publisher_rx, mqtt_args));

    // Wait for spawned tasks to complete, which should not occur, so effectively hang the task.
    join!(udp_task, ws_task, msg_consumer_task, mqtt_publisher_task);

    Ok(())
}