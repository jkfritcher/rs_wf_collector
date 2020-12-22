// Copyright (c) 2020, Jason Fritcher <jkf@wolfnet.org>
// All rights reserved.

use std::{fs::File, io::BufReader, net::IpAddr};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub station_id: u32,
    pub token: Option<String>,
    pub api_key: Option<String>,

    #[serde(default = "Config::default_senders")]
    pub senders: Vec<IpAddr>,

    #[serde(default = "Config::default_mqtt_hostname")]
    pub mqtt_hostname: String,
    #[serde(default = "Config::default_mqtt_port")]
    pub mqtt_port: u16,
    pub mqtt_username: Option<String>,
    pub mqtt_password: Option<String>,
    pub mqtt_client_id: Option<String>
}

pub fn new_config_from_yaml_file(config_file: &str) -> Config {
    // Open config and wrap in a buffered reader
    let file = File::open(config_file).expect("Unable to open config file");
    let buf_reader = BufReader::new(file);

    // Parse file into the struct
    let config: Config = serde_yaml::from_reader(buf_reader).expect("Unable to parse config file.");

    // Check that an auth credential has been provided
    if config.api_key.is_none() && config.token.is_none() {
        panic!("One of token or api_key needs to be specified.");
    }

    config
}

impl Config {
    fn default_mqtt_hostname() -> String {
        String::from("localhost")
    }

    fn default_mqtt_port() -> u16 {
        1883
    }

    fn default_senders() -> Vec<IpAddr> {
        Vec::new()
    }
}