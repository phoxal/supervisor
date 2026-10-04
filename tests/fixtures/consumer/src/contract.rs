use phoxal::contracts::component::encoder::EncoderSample;
use phoxal::contracts::{Empty, Latest, Queue, RequestReply};

// Payload and endpoint declarations owned by the consumer fixture.

#[phoxal::message(package = "example.contract_evaluation.v1")]
pub struct ConsumerStatus {
    /// Lifecycle phase reported by the consumer.
    #[phoxal(tag = 1)]
    pub phase: String,
    /// Count of accepted encoder observations since initialization.
    #[phoxal(tag = 2)]
    pub observed: u64,
    /// Count of completed read_encoder calls since initialization.
    #[phoxal(tag = 3)]
    pub readings: u64,
    /// Last encoder position reported by the current provider, if any.
    #[phoxal(tag = 4)]
    pub position_rad: Option<f64>,
    /// Count of accepted queued tick batches since initialization.
    #[phoxal(tag = 5)]
    pub ticks: u64,
    /// Count of completed read_backup calls since initialization.
    #[phoxal(tag = 6)]
    pub backups: u64,
    /// Last encoder position reported by the backup provider, if any.
    #[phoxal(tag = 7)]
    pub backup_position_rad: Option<f64>,
}
/// The consumer's endpoint contract.
#[phoxal::endpoints]
pub struct ConsumerApi {
    #[phoxal::input(max_age_ms = 100, max_bytes = 1024)]
    encoder: Latest<EncoderSample>,

    #[phoxal::input(max_items = 4, max_bytes = 4096)]
    ticks: Queue<EncoderSample>,

    #[phoxal::output(max_bytes = 4096)]
    status: Latest<ConsumerStatus>,

    #[phoxal::operation(
        contract = "example.contract_evaluation.v1.InspectConsumer",
        max_items = 8,
        max_bytes = 4096
    )]
    inspect: RequestReply<Empty, ConsumerStatus>,

    #[phoxal::call(
        contract = "example.contract_evaluation.v1.ReadEncoder",
        max_items = 8,
        max_bytes = 1024
    )]
    read_encoder: RequestReply<Empty, EncoderSample>,

    #[phoxal::call(
        contract = "example.contract_evaluation.v1.ReadEncoder",
        max_items = 8,
        max_bytes = 1024
    )]
    read_backup: RequestReply<Empty, EncoderSample>,
}
