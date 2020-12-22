// Copyright (c) 2020, Jason Fritcher <jkf@wolfnet.org>
// All rights reserved.

use std::{env, error::Error, process, str, sync::atomic::Ordering};
use getopts::{Matches, Options};

use simple_logger::SimpleLogger;

use tokio::sync::mpsc;

use futures::join;

use serde_json::Value;

#[allow(unused_imports)]
use log::{trace, debug, info, warn, error};
use log::LevelFilter;

mod common;
mod config;
mod mqtt;
mod udp;
mod websocket;
use common::{WFMessage, WFAuthMethod, WFSource, MqttArgs, WsArgs};
use config::new_config_from_yaml_file;
use mqtt::{mqtt_publisher,mqtt_publish_message,mqtt_publish_raw_message};
use udp::udp_collector;
use websocket::{websocket_collector,get_ws_connected};


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

fn print_usage_and_exit(program: &str, opts: Options) -> ! {
    let brief = format!("Usage: {} [options] <config file name>", program);
    print!("{}", opts.usage(&brief));
    process::exit(-1);
}

fn parse_arguments(input: Vec<String>) -> (Matches, String) {
    let program = &input[0];

    // Build options
    let mut opts = Options::new();
    opts.optflag("h", "help", "Print usage");
    opts.optflagmulti("d", "debug", "Enable debug logging, multiple times for trace level");

    // Parse arguments
    let matches = match opts.parse(&input[1..]) {
        Ok(m) => m,
        Err(err) => {
            println!("{}", err.to_string());
            print_usage_and_exit(program, opts);
        }
    };

    if matches.opt_present("h") {
        print_usage_and_exit(program, opts);
    }

    if matches.free.len() == 0 {
        println!("Must specify a config file name");
        print_usage_and_exit(program, opts);
    }
    let config_name = matches.free[0].clone();

    (matches, config_name)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let (args, config_name) = parse_arguments(env::args().collect());

    // Initialize logging
    let log_level = match args.opt_count("d") {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    SimpleLogger::new().with_level(log_level).init().unwrap();

    // Load config file
    let config = new_config_from_yaml_file(&config_name);

    // Message types to ignore
    #[allow(unused_mut)]
    let mut ignored_msg_types: Vec<String> = Vec::new();

    // Collect args for MQTT
    let mqtt_args = MqttArgs {
        hostname: config.mqtt_hostname,
        port: config.mqtt_port,
        username: config.mqtt_username,
        password: config.mqtt_password,
        client_id: config.mqtt_client_id,
    };

    let auth_method;
    if config.token.is_some() {
        auth_method = WFAuthMethod::AUTHTOKEN(config.token.unwrap());
    } else {
        auth_method = WFAuthMethod::APIKEY(config.api_key.unwrap());
    }
    let ws_args = WsArgs {
        auth_method: auth_method,
        station_id: Some(config.station_id),
        device_ids: None,
    };

    // Channel for consumer to processor messaging
    let (collector_tx, consumer_rx) = mpsc::unbounded_channel::<WFMessage>();

    // Channel to mqtt publishing task
    let (publisher_tx, publisher_rx) = mpsc::unbounded_channel::<(String, String)>();

    // Spawn tasks for the collectors / consumer
    let udp_task = tokio::spawn(udp_collector(collector_tx.clone(), config.senders));
    let ws_task = tokio::spawn(websocket_collector(collector_tx, ws_args));
    let msg_consumer_task = tokio::spawn(message_consumer(consumer_rx, publisher_tx, ignored_msg_types));
    let mqtt_publisher_task = tokio::spawn(mqtt_publisher(publisher_rx, mqtt_args));

    // Wait for spawned tasks to complete, which should not occur, so effectively hang the task.
    join!(udp_task, ws_task, msg_consumer_task, mqtt_publisher_task);

    Ok(())
}