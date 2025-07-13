fn main() -> eframe::Result {
    env_logger::init();

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([600.0, 400.0]),
        ..Default::default()
    };
    eframe::run_native(
        "parkhay",
        native_options,
        Box::new(|cc| Ok(Box::new(parkhay::ParkhayApp::new(cc)))),
    )
}
