// Copyright (c) 2020, Jason Fritcher <jkf@wolfnet.org>
// All rights reserved.

use std::{str, sync::atomic::Ordering};

use serde_json::Value;
use tokio::sync::mpsc;

#[allow(unused_imports)]
use log::{trace, debug, info, warn, error};

use crate::common::{WFMessage, WFSource};
use crate::mqtt::{mqtt_publish_message, mqtt_publish_raw_message};
use crate::websocket::get_ws_connected;

fn get_hub_sn_from_msg(msg_obj: &serde_json::Value) -> Option<&str> {
    let msg_map = msg_obj.as_object()?;
    if msg_map.contains_key("type") && msg_map.get("type")? == "hub_status" {
        msg_map.get("serial_number")?.as_str()
    } else {
        msg_map.get("hub_sn")?.as_str()
    }
}

pub async fn message_consumer(mut collector_rx: mpsc::UnboundedReceiver<WFMessage>,
                              publisher_tx: mpsc::UnboundedSender<(String, String)>,
                              ignored_msg_types: Vec<String>) {
    // Get WS connected reference
    let ws_connected = get_ws_connected();

    // Wait for messages from the consumers to process
    while let Some(msg) = collector_rx.recv().await {
        let msg_str = match str::from_utf8(&msg.message) {
            Err(err) => {
                error!("Error in str::from_utf8, ignoring message: {}", err);
                continue;
            }
            Ok(msg) => msg,
        };
        let msg_json: Value = match serde_json::from_str(&msg_str) {
            Err(err) => {
                error!("Error parsing json, skipping message: {}", err);
                continue;
            }
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
        if msg_type.ends_with("_debug") || msg_type == "ack" ||
           ignored_msg_types.iter().any(|x| x == msg_type ) {
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
        mqtt_publish_raw_message(&publisher_tx, &topic_base, &msg.source, &msg_str);

        // Handle specific message types
        match msg_type {
            "rapid_wind" => {
                let topic = format!("{}/rapid", &topic_base);
                mqtt_publish_message(&publisher_tx, &topic, &msg_str);
            }
            mt if mt.ends_with("_status") => {
                let topic = format!("{}/status", &topic_base);
                mqtt_publish_message(&publisher_tx, &topic, &msg_str);
            }
            mt if mt.starts_with("obs_") => {
                if ws_connected.load(Ordering::SeqCst) && msg.source == WFSource::UDP {
                    // Ignore the UDP events if we're connected via WS
                    continue;
                }
                let topic = format!("{}/obs", &topic_base);
                mqtt_publish_message(&publisher_tx, &topic, &msg_str);
            }
            mt if mt.starts_with("evt_") => {
                match mt {
                    "evt_strike" => {
                        if ws_connected.load(Ordering::SeqCst) && msg.source == WFSource::UDP {
                            // Ignore the UDP events if we're connected via WS
                            continue;
                        }
                    }
                    "evt_precip" => {
                        if msg.source == WFSource::WS {
                            // Ignore the WS event and always use UDP event
                            continue;
                        }
                    }
                    _ => { warn!("Unknown evt message type, ignoring. ({})", mt); continue; }
                }
                let topic = format!("{}/evt", &topic_base);
                mqtt_publish_message(&publisher_tx, &topic, &msg_str);
            }
            _ => (),
        }
    }
}
