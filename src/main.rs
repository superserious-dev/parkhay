use anyhow::{Result, anyhow};
use clap::Parser;
use parkhay::ParkhayCli;

fn main() -> Result<()> {
    env_logger::init();

    let cli = ParkhayCli::parse();

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([600.0, 400.0]),
        ..Default::default()
    };
    eframe::run_native(
        "parkhay",
        native_options,
        Box::new(|cc| Ok(Box::new(parkhay::ParkhayApp::new(cc, cli.path)))),
    )
    .map_err(|e| anyhow!("Error launching Parkhay: {e}"))
}
