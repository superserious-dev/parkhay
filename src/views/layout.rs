use egui::{CentralPanel, Frame, Label, RichText, Ui, Widget, epaint::MarginF32};

use super::View;
use crate::ParkhayFile;

pub struct LayoutView {
    parkhay_file: ParkhayFile,
}

impl LayoutView {
    const CORNER_RADIUS: f32 = 2.5;
    const HEADER_TEXT_SIZE: f32 = 18.;
    const INNER_SECTION_MARGIN: f32 = 8.;
    const WINDOW_PADDING_HORIZONTAL: f32 = 20.;

    pub fn new(parkhay_file: ParkhayFile) -> Self {
        Self { parkhay_file }
    }

    fn render_layout_header(ui: &mut Ui, text: &str) {
        Label::new(
            RichText::new(text)
                .monospace()
                .size(Self::HEADER_TEXT_SIZE)
                .strong(),
        )
        .selectable(false)
        .ui(ui);
    }
}

impl View for LayoutView {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        CentralPanel::default().show(ctx, |ui| {
            ui.set_width(ui.available_width());
            let window_padding_vertical = 0.2 * ui.available_width() / 2.;

            Frame::default()
                .corner_radius(Self::CORNER_RADIUS)
                .fill(ui.visuals().window_fill)
                .stroke(ui.visuals().window_stroke)
                .outer_margin(MarginF32::symmetric(
                    window_padding_vertical,
                    Self::WINDOW_PADDING_HORIZONTAL,
                ))
                .show(ui, |ui| {
                    Frame::default()
                        .fill(ui.style().visuals.widgets.inactive.bg_fill)
                        .stroke(ui.style().visuals.widgets.inactive.bg_stroke)
                        .corner_radius(Self::CORNER_RADIUS)
                        .inner_margin(Self::INNER_SECTION_MARGIN)
                        .outer_margin(MarginF32::ZERO)
                        .show(ui, |ui| {
                            ui.set_width(ui.available_width());
                            Self::render_layout_header(
                                ui,
                                &String::from_utf8_lossy(&self.parkhay_file.start_magic),
                            );
                        });

                    // TODO add file content

                    Frame::default()
                        .fill(ui.style().visuals.widgets.inactive.bg_fill)
                        .stroke(ui.style().visuals.widgets.inactive.bg_stroke)
                        .corner_radius(Self::CORNER_RADIUS)
                        .inner_margin(Self::INNER_SECTION_MARGIN)
                        .outer_margin(MarginF32::ZERO)
                        .show(ui, |ui| {
                            ui.set_width(ui.available_width());
                            Self::render_layout_header(
                                ui,
                                &format!("Footer Length: {}", self.parkhay_file.footer_length),
                            );
                        });

                    Frame::default()
                        .fill(ui.style().visuals.widgets.inactive.bg_fill)
                        .stroke(ui.style().visuals.widgets.inactive.bg_stroke)
                        .corner_radius(Self::CORNER_RADIUS)
                        .inner_margin(Self::INNER_SECTION_MARGIN)
                        .outer_margin(MarginF32::ZERO)
                        .show(ui, |ui| {
                            ui.set_width(ui.available_width());
                            Self::render_layout_header(
                                ui,
                                &String::from_utf8_lossy(&self.parkhay_file.end_magic),
                            );
                        });
                });
        });
    }
}
