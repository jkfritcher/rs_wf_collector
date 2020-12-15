// Copyright (c) 2020, Jason Fritcher <jkf@wolfnet.org>
// All rights reserved.

use std::{env, error::Error, net::IpAddr, str, sync::atomic::Ordering};
use getopts::{Matches, Options};

use simple_logger::SimpleLogger;

use tokio::sync::mpsc;

use futures::join;

use serde_json::Value;

#[allow(unused_imports)]
use log::{trace, debug, info, warn, error};
use log::LevelFilter;

mod common;
mod mqtt;
mod udp;
mod websocket;
use common::{WFMessage, WFAuthMethod, WFSource, MqttArgs, WsArgs};
use mqtt::{mqtt_publisher,mqtt_publish_message,mqtt_publish_raw_message};
use udp::udp_collector;
use websocket::{websocket_collector,get_ws_connected};

// WeatherFlow constants
const WF_DEFAULT_API_KEY: &str = "20c70eae-e62f-4d3b-b3a4-8586e90f3ac8";

// MQTT arg defaults
const MQTT_DEFAULT_HOST: &str = "localhost";
const MQTT_DEFAULT_PORT: u16 = 1883;


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
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
    std::process::exit(-1);
}

fn parse_arguments(input: Vec<String>) -> Matches {
    let program = &input[0];

    // Build options
    let mut opts = Options::new();
    opts.optflag("h", "help", "Print usage");
    opts.optflagmulti("d", "debug", "Enable debug logging, multiple times for trace level");
    opts.optopt("s", "senders", "Comma separated list of allowed ip addresses", "SENDER[,SENDER]");
    opts.optopt("t", "token", "Auth Token for WeatherFlow APIs", "TOKEN");
    opts.optopt("k", "key", "API Key for WeatherFlow APIs", "KEY");
    opts.reqopt("i", "station-id", "Station ID for WeatherFlow APIs", "STATION-ID");

    // MQTT options
    opts.optopt("", "mqtt-host", "MQTT Broker host name, default localhost", "HOST");
    opts.optopt("", "mqtt-port", "MQTT Broker port, default 1883", "PORT");
    opts.optopt("", "mqtt-user", "MQTT Broker auth user name", "USER");
    opts.optopt("", "mqtt-password", "MQTT Broker auth password", "PASSWORD");
    opts.optopt("", "mqtt-client-id", "MQTT Broker client id", "CLIENT-ID");

    // Parse arguments
    let matches = match opts.parse(&input[1..]) {
        Ok(m) => m,
        Err(err) => {
            println!("{}", err.to_string());
            print_usage_and_exit(program, opts);
        }
    };

    if matches.opt_present("h") {
        print_usage_and_exit(&input[0], opts);
    }

    if matches.opt_present("t") && matches.opt_present("k") {
        println!("Can not specify both a token and key for authentication, only one");
        print_usage_and_exit(program, opts);
    }

    matches
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let args = parse_arguments(env::args().collect());

    // Initialize logging
    let log_level = match args.opt_count("d") {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    SimpleLogger::new().with_level(log_level).init().unwrap();

    // Vec of IpAddrs of senders we're interested in
    let mut senders: Vec<IpAddr> = Vec::new();
    match args.opt_str("s") {
        Some(senders_str) => {
            for sender in senders_str.split(",") {
                senders.push(sender.parse()?);
            }
        },
        None => ()
    }

    // Message types to ignore
    #[allow(unused_mut)]
    let mut ignored_msg_types: Vec<String> = Vec::new();

    // Collect args for MQTT
    let mqtt_host = match args.opt_str("mqtt-host") {
        Some(host) => host,
        None => String::from(MQTT_DEFAULT_HOST),
    };
    let mqtt_port = match args.opt_str("mqtt-port") {
        Some(port) => port.parse::<u16>().expect("mqtt-port is not valid, only integers 0-65535 are valid."),
        None => MQTT_DEFAULT_PORT,
    };
    let mqtt_args = MqttArgs {
        hostname: mqtt_host,
        port: mqtt_port,
        username: args.opt_str("mqtt-user"),
        password: args.opt_str("mqtt-password"),
        client_id: args.opt_str("mqtt-client-id"),
    };

    let auth_method;
    if args.opt_present("t") {
        auth_method = WFAuthMethod::AUTHTOKEN(args.opt_str("t").unwrap());
    } else {
        let key = match args.opt_str("k") {
            Some(key) => key,
            None => {
                info!("No auth token or API Key was specified, using public development API Key.");
                String::from(WF_DEFAULT_API_KEY)
            },
        };
        auth_method = WFAuthMethod::APIKEY(key);
    }
    let station_id = match args.opt_str("i") {
        Some(station_id) => station_id.parse::<u32>().expect("station-id is not valid, only 32-bit integers are valid."),
        None => panic!("This should not be reached."),
    };
    let ws_args = WsArgs {
        auth_method: auth_method,
        station_id: Some(station_id),
        device_ids: None,
    };

    // Channel for consumer to processor messaging
    let (collector_tx, consumer_rx) = mpsc::unbounded_channel::<WFMessage>();

    // Channel to mqtt publishing task
    let (publisher_tx, publisher_rx) = mpsc::unbounded_channel::<(String, String)>();

    // Spawn tasks for the collectors / consumer
    let udp_task = tokio::spawn(udp_collector(collector_tx.clone(), senders));
    let ws_task = tokio::spawn(websocket_collector(collector_tx, ws_args));
    let msg_consumer_task = tokio::spawn(message_consumer(consumer_rx, publisher_tx, ignored_msg_types));
    let mqtt_publisher_task = tokio::spawn(mqtt_publisher(publisher_rx, mqtt_args));

    // Wait for spawned tasks to complete, which should not occur, so effectively hang the task.
    join!(udp_task, ws_task, msg_consumer_task, mqtt_publisher_task);

    Ok(())
}