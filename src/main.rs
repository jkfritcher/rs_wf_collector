// Copyright (c) 2020, Jason Fritcher <jkf@wolfnet.org>
// All rights reserved.

use std::{env, error::Error, process, str};

use getopts::{Matches, Options};
use simple_logger::SimpleLogger;

use futures::join;
use tokio::sync::mpsc;

use log::LevelFilter;
#[allow(unused_imports)]
use log::{trace, debug, info, warn, error};

mod common;
mod config;
mod messages;
mod mqtt;
mod udp;
mod websocket;
use common::{MqttArgs, WFAuthMethod, WFMessage, WsArgs};
use config::new_config_from_yaml_file;
use messages::message_consumer;
use mqtt::mqtt_publisher;
use udp::udp_collector;
use websocket::websocket_collector;

fn print_usage_and_exit(program: &str, opts: Options) -> ! {
    let brief = format!("Usage: {} [options] <config file name>", program);
    print!("{}", opts.usage(&brief));
    process::exit(-1);
}

fn parse_arguments(args: env::Args) -> (Matches, String) {
    let args = args.collect::<Vec<_>>();
    let program = &args[0];

    // Build options
    let mut opts = Options::new();
    opts.optflag("h", "help", "Print usage");
    opts.optflagmulti("d", "debug", "Enable debug logging, multiple times for trace level");

    // Parse arguments
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(err) => {
            println!("{}", err);
            print_usage_and_exit(program, opts);
        }
    };

    if matches.opt_present("h") {
        print_usage_and_exit(program, opts);
    }

    if matches.free.is_empty() {
        println!("Must specify a config file name");
        print_usage_and_exit(program, opts);
    }
    let config_name = matches.free[0].clone();

    (matches, config_name)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let (args, config_name) = parse_arguments(env::args());

    // Initialize logging
    let log_level = match args.opt_count("d") {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    SimpleLogger::new()
        .with_level(log_level)
        .with_module_level("mqtt_async_client", LevelFilter::Info)
        .with_utc_timestamps()
        .init()
        .expect("Failed to initialize logger");

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
        topic_base: config.mqtt_topic_base,
    };

    let auth_method = if config.token.is_some() {
        WFAuthMethod::AUTHTOKEN(config.token.unwrap())
    } else {
        WFAuthMethod::APIKEY(config.api_key.unwrap())
    };
    let ws_args = WsArgs {
        auth_method,
        station_id: Some(config.station_id),
        device_ids: None,
    };

    let mqtt_topic_base = mqtt_args.topic_base.clone();

    // Channel for consumer to processor messaging
    let (collector_tx, consumer_rx) = mpsc::unbounded_channel::<WFMessage>();

    // Channel to mqtt publishing task
    let (publisher_tx, publisher_rx) = mpsc::unbounded_channel::<(String, String)>();

    // Spawn tasks for the collectors / consumer
    let udp_task = tokio::spawn(udp_collector(collector_tx.clone(), config.senders));
    let ws_task = tokio::spawn(websocket_collector(collector_tx, ws_args));
    let msg_consumer_task = tokio::spawn(message_consumer(consumer_rx, publisher_tx, ignored_msg_types, mqtt_topic_base));
    let mqtt_publisher_task = tokio::spawn(mqtt_publisher(publisher_rx, mqtt_args));

    // Wait for spawned tasks to complete, which should not occur, so effectively hang the task.
    join!(udp_task, ws_task, msg_consumer_task, mqtt_publisher_task);

    Ok(())
}
