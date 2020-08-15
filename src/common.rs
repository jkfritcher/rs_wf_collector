#[derive(Debug)]
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
