#[derive(clap::Parser)]
pub struct Cli {
    /// Subsquid archive endpoint URL
    #[clap(long)]
    pub archive: String,

    /// Rpc api URL of an ethereum node
    #[clap(long)]
    pub rpc: String,

    /// Number of blocks after which data is considered final
    pub finality_confirmation: u64,
}
