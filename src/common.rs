// Copyright (c) 2020, Jason Fritcher <jkf@wolfnet.org>
// All rights reserved.

#[derive(Debug, Default)]
pub struct MqttArgs {
    pub hostname: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Debug)]
pub struct WsArgs {
    pub auth_method: WFAuthMethod,
    pub station_id: Option<u32>,
    pub device_ids: Option<Vec<u32>>,
}

#[derive(Debug, PartialEq)]
pub enum WFSource {
    UDP,
    WS,
}

#[derive(Debug)]
pub struct WFMessage {
    pub source: WFSource,
    pub message: Vec<u8>,
}

#[derive(Debug)]
pub enum WFAuthMethod {
    APIKEY(String),
    AUTHTOKEN(String),
}
