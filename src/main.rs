use std::{
    error::Error,
    net::IpAddr,
    time::SystemTime,
    str,
};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{
    delay_for,
    Duration
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::protocol::Message,
};

use futures_util::{SinkExt, StreamExt};
use futures::join;

use serde_json::json;
use serde_json::Value;

use log::{trace, debug, info, warn, error};
use simple_logger;

#[derive(Debug)]
enum WFSource {
    UDP,
    WS,
}

#[derive(Debug)]
struct WFMessage {
    source: WFSource,
    message: Vec<u8>,
}

// WeatherFlow constants
const WF_API_KEY: &str = "20c70eae-e62f-4d3b-b3a4-8586e90f3ac8";
const WF_REST_BASE_URL: &str = "https://swd.weatherflow.com/swd/rest/";
const WF_WS_URL: &str = "wss://ws.weatherflow.com/swd/data?api_key=";
const WF_STATION_ID: u16 = 19992;
const WF_DEVICE_ID: u32 = 67708;


async fn udp_collector(sender: mpsc::UnboundedSender<WFMessage>,
                      sources: Vec<IpAddr>) {
    let listen_addr = "0.0.0.0:50222".to_string();
    let mut socket = UdpSocket::bind(&listen_addr).await.expect("Failed to create UDP listener socket!");

    // Buffer for received packets
    let mut buf = vec![0; 1024];
    loop {
        let (size, from) = socket.recv_from(&mut buf).await.expect("Error from recv_from.");

        // Check recevied packet against approved sources
        if !sources.is_empty() && !sources.iter().any(|&source| source == from.ip()) {
            warn!("Ignoring packet from {}, sender is not approved!", from.ip());
            continue;
        }

        // Build message to send via the channel
        let msg = WFMessage {
            source: WFSource::UDP,
            message: buf[..size].to_vec(),
        };
        match sender.send(msg) {
            Err(err) => { error!("Failed to add message to sender: {}", err); },
            Ok(()) => ()
        }
    }
}

async fn websocket_collector(sender: mpsc::UnboundedSender<WFMessage>) {
    let url_str = format!("{}{}", WF_WS_URL, WF_API_KEY);
    let device_id = WF_DEVICE_ID; // TODO Lookup device id via station id
    let mut reconnect_delay: u64 = 0;

    loop {
        // Delay before reconnecting if there were previous errors
        if reconnect_delay > 0 {
            delay_for(Duration::from_secs(reconnect_delay)).await;
            reconnect_delay = if reconnect_delay < 30 { reconnect_delay * 2 } else { 30 };
        }

        // Use current epoch time as request id
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Failed to get current epoch time")
            .as_secs();
        // json to send as request
        let ws_request = json!({"type":"listen_start","device_id":device_id,"id":now.to_string()}).to_string();

        // Connect to WS endpoint
        let mut ws_stream = match connect_async(&url_str).await {
            Ok((ws_stream, ws_response)) => {
                let code = ws_response.status().as_u16();
                if code != 101 {
                    error!("Unexpected response code received: {}", code);
                    reconnect_delay = if reconnect_delay == 0 { 1 } else { reconnect_delay };
                    continue;
                }
                ws_stream
            },
            Err(err) => {
                error!("Error connecting to WebSocket server: {}", err);
                reconnect_delay = if reconnect_delay == 0 { 1 } else { reconnect_delay };
                continue;
            }
        };

        // Get initial message from server
        let msg = match ws_stream.next().await {
            Some(Ok(msg)) => msg,
            Some(Err(err)) => {
                error!("Error occurred reading next message: {}", err);
                match ws_stream.close(None).await {
                    Err(err) => { error! ("Error closing ws_stream: {}", err); },
                    Ok(_) => (),
                };
                reconnect_delay = if reconnect_delay == 0 { 1 } else { reconnect_delay };
                continue;
            },
            None => {
                error!("End of stream found on WS stream. Shutting down stream.");
                match ws_stream.close(None).await {
                    Err(err) => { error! ("Error closing ws_stream: {}", err); },
                    Ok(_) => (),
                };
                reconnect_delay = if reconnect_delay == 0 { 1 } else { reconnect_delay };
                continue;
            }
        };
        trace!("WS Message received: {}", msg);
        match msg.into_text() {
            Ok(txt) => {
                if !txt.contains("connection_opened") {
                    error!("WebSocket connection not successful: {}", txt);
                    match ws_stream.close(None).await {
                        Err(err) => { error! ("Error closing ws_stream: {}", err); },
                        Ok(_) => (),
                    };
                    reconnect_delay = if reconnect_delay == 0 { 1 } else { reconnect_delay };
                    continue;
                }
            },
            Err(err) => {
                error!("Error converting message into string: {}", err);
                continue;
            }
        };
        // Reset reconnect delay
        reconnect_delay = 0;

        // Connection opened, request station observations
        match ws_stream.send(Message::text(ws_request)).await {
            Err(err) => {
                error!("Error received sending station obs request: {}", err);
                match ws_stream.close(None).await {
                    Err(err) => { error! ("Error closing ws_stream: {}", err); },
                    Ok(_) => (),
                };
                reconnect_delay = 1;
                continue;
            },
            Ok(()) => (),
        };

        while let Some(msg) = ws_stream.next().await {
            let msg = match msg {
                Ok(msg) => msg,
                Err(err) => {
                    error!("WebSocket receive error: {:?}", err);
                    continue;
                }
            };
            trace!("WS Message received: {}", msg);
            if msg.is_close() {
                info!("WebSocket connection closed: {}", msg);
                break;
            }
            if !(msg.is_text()) {
                warn!("WebSocket non-text message received: {}", msg);
                continue;
            }
            let msg = WFMessage {
                source: WFSource::WS,
                message: msg.into_data(),
            };
            match sender.send(msg) {
                Err(err) => { error!("Failed to add message to sender: {}", err); },
                Ok(()) => ()
            }
        }
    }
}

async fn message_consumer(mut receiver: mpsc::UnboundedReceiver<WFMessage>,
                           ignored_msg_types: Vec<String>) {
    // Wait for messages from the consumers to process
    while let Some(msg) = receiver.recv().await {
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
            warn!("{:?} message not valid, skipping", msg.source);
            continue;
        }

        let msg_type = msg_json.as_object().unwrap()
                               .get("type").unwrap()
                               .as_str().unwrap();

        // Ignore message types we're not interested in
        if ignored_msg_types.iter().any(|x| x == &msg_type ) {
            continue;
        }

        println!("{:?} - {}", msg.source, &msg_str);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Debug).unwrap();

    // Channel for consumer to processor messaging
    let (tx, rx) = mpsc::unbounded_channel::<WFMessage>();

    // Vec of IpAddrs of senders we're interested in
    let mut senders: Vec<IpAddr> = Vec::new();
    //senders.push("10.1.3.3".parse()?);

    // Message types to ignore
    let mut ignored_msg_types: Vec<String> = Vec::new();
    ignored_msg_types.push("rapid_wind".to_string());
    ignored_msg_types.push("light_debug".to_string());

    // Spawn tasks for the consumers
    let udp_task = tokio::spawn(udp_collector(tx.clone(), senders));
    let ws_task = tokio::spawn(websocket_collector(tx.clone()));
    // rx ownership transfer to the spawned task
    let msg_consumer_task = tokio::spawn(message_consumer(rx, ignored_msg_types));

    // Let go of our tx reference to the channel.
    drop(tx);

    // Wait for spawned tasks to complete, which should not occur, so effectively hang the task.
    join!(udp_task, ws_task, msg_consumer_task);

    Ok(())
}