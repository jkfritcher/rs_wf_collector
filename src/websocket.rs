use std::{
    time::SystemTime,
    str,
};

use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{
    delay_for,
    Duration
};
use tokio_tungstenite::{
    connect_async,
    MaybeTlsStream,
    tungstenite::protocol::Message,
    WebSocketStream,
};

use futures_util::{SinkExt, StreamExt};

use serde_json::json;

use crate::WF_DEVICE_ID;
use crate::common::{WFMessage, WFSource, WFAuthMethod};
use WFAuthMethod::{APIKEY, AUTHTOKEN};

#[allow(unused_imports)]
use log::{trace, debug, info, warn, error};

const WF_REST_BASE_URL: &str = "https://swd.weatherflow.com/swd/rest/";
const WF_WS_URL: &str = "wss://ws.weatherflow.com/swd/data";


async fn websocket_connect(url_str: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, String> {
    // Connect to WS endpoint
    let mut ws_stream = match connect_async(url_str).await {
        Ok((ws_stream, ws_response)) => {
            let code = ws_response.status().as_u16();
            if code != 101 {
                error!("Unexpected response code received: {}", code);
                return Err(String::from("Unexpected response code received."));
            }
            ws_stream
        },
        Err(err) => {
            error!("Error connecting to WebSocket server: {}", err);
            return Err(String::from("Error connecting to WebSocket server."));
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
            return Err(String::from("Error while reading next message."));
        },
        None => {
            error!("End of stream found on WS stream. Shutting down stream.");
            match ws_stream.close(None).await {
                Err(err) => { error! ("Error closing ws_stream: {}", err); },
                Ok(_) => (),
            };
            return Err(String::from("End of stream encounter, closing stream."));
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
                return Err(String::from("WebSocket connection not successful."));
            }
        },
        Err(err) => {
            error!("Error converting message into string: {}", err);
            return Err(String::from("Error converting message into string."));
        }
    };

    Ok(ws_stream)
}

async fn websocket_send_listen_start(ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>, device_id: u32) -> Result<(), String> {
    // Use current epoch time as request id
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get current epoch time")
        .as_secs();
    let now_str = now.to_string();
    // json to send as request
    let ws_request = json!({"type":"listen_start","device_id":device_id,"id":now_str}).to_string();

    // Connection opened, request station observations
    match ws_stream.send(Message::text(ws_request)).await {
        Err(err) => {
            error!("Error received sending station obs request: {}", err);
            match ws_stream.close(None).await {
                Err(err) => { error! ("Error closing ws_stream: {}", err); },
                Ok(_) => (),
            };
            return Err(String::from("Error sending listen_start request."));
        },
        Ok(_) => (),
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
            return Err(String::from("Error while reading next message."));
        },
        None => {
            error!("End of stream found on WS stream. Shutting down stream.");
            match ws_stream.close(None).await {
                Err(err) => { error! ("Error closing ws_stream: {}", err); },
                Ok(_) => (),
            };
            return Err(String::from("End of stream encounter, closed stream."));
        }
    };
    trace!("WS Message received: {}", msg);
    match msg.into_text() {
        Ok(txt) => {
            if !txt.contains("ack") || !txt.contains(&now_str) {
                error!("Failed to receive ack for listen_start command: {}", txt);
                match ws_stream.close(None).await {
                    Err(err) => { error! ("Error closing ws_stream: {}", err); },
                    Ok(_) => (),
                };
                return Err(String::from("WebSocket connection not successful."));
            }
        },
        Err(err) => {
            error!("Error converting message into string: {}", err);
            return Err(String::from("Error converting message into string."));
        }
    };

    Ok(())
}

pub async fn websocket_collector(collector_tx: mpsc::UnboundedSender<WFMessage>,
                                 auth_method: WFAuthMethod
) {
    let url_str = match auth_method {
        APIKEY(key) => { format!("{}?api_key={}", WF_WS_URL, key) },
        AUTHTOKEN(token) => { format!("{}?token={}", WF_WS_URL, token) },
    };
    let device_id = WF_DEVICE_ID; // TODO Lookup device id via REST and station id

    let mut reconnect_delay: u32 = 0;
    loop {
        // Delay before reconnecting if there were previous errors
        if reconnect_delay > 0 {
            delay_for(Duration::from_secs(reconnect_delay.into())).await;
            reconnect_delay = if reconnect_delay < 32 { reconnect_delay * 2 } else { 32 };
        }

        info!("Connecting to WebSocket server");
        debug!("Connection URL: {}", url_str);
        let mut ws_stream = match websocket_connect(&url_str).await {
            Err(_) => {
                reconnect_delay = if reconnect_delay == 0 { 1 } else { reconnect_delay };
                continue;
            },
            Ok(ws_stream) => ws_stream,
        };
        // Reset reconnect delay
        reconnect_delay = 0;

        info!("WebSocket connected successfully.");

        match websocket_send_listen_start(&mut ws_stream, device_id).await {
            Err(_) => {
                reconnect_delay = 1;
                continue;
            },
            Ok(_) => (),
        }

        info!("WS listen_start sent successfully.");

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
            match collector_tx.send(msg) {
                Err(err) => { error!("Failed to add message to sender: {}", err); },
                Ok(()) => ()
            }
        }
    }
}
