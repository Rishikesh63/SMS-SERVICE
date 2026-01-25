#[derive(Clone)]
pub struct BrokerConfig {
    pub stream: &'static str,
    pub topic: &'static str,
    pub partitions: u32,
}
