use std::sync::mpsc::Sender;

use data_renderer::DataRenderer;
use egui::{CentralPanel, Frame, Label, RichText, Ui, Widget, epaint::MarginF32};
use footer_renderer::FooterRenderer;

use super::View;
use crate::{ParkhayFile, file::PageReadRequest};

mod components;
mod data_renderer;
mod footer_renderer;

const CORNER_RADIUS: f32 = 2.5;
const LAYOUT_LABEL_SIZE: f32 = 18.;
const INNER_SECTION_MARGIN: f32 = 8.;
const WINDOW_PADDING_HORIZONTAL: f32 = 20.;

pub struct LayoutView {
    parkhay_file: ParkhayFile,
    page_reader_tx: Sender<PageReadRequest>,
}

impl LayoutView {
    pub fn new(parkhay_file: ParkhayFile, page_reader_tx: Sender<PageReadRequest>) -> Self {
        Self {
            parkhay_file,
            page_reader_tx,
        }
    }

    fn render_layout_header(ui: &mut Ui, text: &str) {
        Label::new(
            RichText::new(text)
                .monospace()
                .size(LAYOUT_LABEL_SIZE)
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
                .corner_radius(CORNER_RADIUS)
                .fill(ui.visuals().window_fill)
                .stroke(ui.visuals().window_stroke)
                .outer_margin(MarginF32::symmetric(
                    window_padding_vertical,
                    WINDOW_PADDING_HORIZONTAL,
                ))
                .show(ui, |ui| {
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        Frame::default()
                            .fill(ui.style().visuals.widgets.inactive.bg_fill)
                            .stroke(ui.style().visuals.widgets.inactive.bg_stroke)
                            .corner_radius(CORNER_RADIUS)
                            .inner_margin(INNER_SECTION_MARGIN)
                            .outer_margin(MarginF32::ZERO)
                            .show(ui, |ui| {
                                ui.set_width(ui.available_width());
                                Self::render_layout_header(
                                    ui,
                                    &String::from_utf8_lossy(&self.parkhay_file.start_magic),
                                );
                            });

                        // Data
                        DataRenderer::render(ui, &self.parkhay_file.data, &mut self.page_reader_tx);

                        // Footer
                        FooterRenderer::render(ui, &self.parkhay_file.footer);

                        Frame::default()
                            .fill(ui.style().visuals.widgets.inactive.bg_fill)
                            .stroke(ui.style().visuals.widgets.inactive.bg_stroke)
                            .corner_radius(CORNER_RADIUS)
                            .inner_margin(INNER_SECTION_MARGIN)
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
                            .corner_radius(CORNER_RADIUS)
                            .inner_margin(INNER_SECTION_MARGIN)
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
        });
    }
}
