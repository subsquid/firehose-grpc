#[derive(clap::Parser)]
pub struct Cli {
    /// Subsquid archive endpoint URL
    #[clap(long)]
    pub archive: String,
}
