// Copyright (c) 2020, Jason Fritcher <jkf@wolfnet.org>
// All rights reserved.

use std::net::IpAddr;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

#[allow(unused_imports)]
use log::{trace, debug, info, warn, error};

use crate::common::{WFMessage, WFSource};

pub async fn udp_collector(
    collector_tx: mpsc::UnboundedSender<WFMessage>,
    sources: Vec<IpAddr>,
) -> ! {
    let listen_addr = "0.0.0.0:50222".to_string();
    let socket = UdpSocket::bind(&listen_addr)
        .await
        .expect("Failed to create UDP listener socket!");

    info!("UDP listener successfully opened.");

    // Buffer for received packets
    let mut buf = vec![0u8; 1024];
    loop {
        let (size, from) = socket
            .recv_from(&mut buf)
            .await
            .expect("Error from recv_from.");
        trace!("Received packet from {}", from);

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
        if let Err(err) = collector_tx.send(msg) {
            error!("Failed to add message to collector_tx: {}", err);
        }
    }
}
