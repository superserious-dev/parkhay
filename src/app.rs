use eframe::{CreationContext, Frame};
use egui::{CentralPanel, Context, ViewportCommand};

#[derive(Default)]
pub struct ParkhayApp {}

impl ParkhayApp {
    pub fn new(cc: &CreationContext<'_>, parquet_path: String) -> Self {
        // Update window title
        cc.egui_ctx
            .send_viewport_cmd(ViewportCommand::Title(parquet_path));

        Default::default()
    }
}

impl eframe::App for ParkhayApp {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        CentralPanel::default().show(ctx, |ui| {
            ui.heading("Hello World!");
        });
    }
}
