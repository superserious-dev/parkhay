use egui::CentralPanel;

use super::View;
use crate::ParkhayFile;

pub struct LayoutView {
    parkhay_file: ParkhayFile,
}

impl LayoutView {
    pub fn new(parkhay_file: ParkhayFile) -> Self {
        Self { parkhay_file }
    }
}

impl View for LayoutView {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        CentralPanel::default().show(ctx, |ui| {
            ui.label("Layout View");
        });
    }
}
