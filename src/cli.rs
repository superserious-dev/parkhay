use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct ParkhayCli {
    /// Path to the parquet file
    pub path: String,
}
