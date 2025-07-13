use egui::{Color32, Frame, Grid, Label, Margin, RichText, ScrollArea, Ui, Widget};
use row_groups_renderer::RowGroupsRenderer;
use schema_renderer::SchemaRenderer;

use crate::file::ParkhayFooter;

use super::components::{CollapsibleSection, CollapsibleSectionIcon, LabeledValue};

mod row_groups_renderer;
mod schema_renderer;
mod ui_helpers;

const CORNER_RADIUS: f32 = 2.5;
const LAYOUT_LABEL_SIZE: f32 = 19.;
const HEADER_LABEL_SIZE: f32 = 15.;
const HEADER_VALUE_SIZE: f32 = 14.;
const SUBHEADER_LABEL_SIZE: f32 = 13.;
const SUBHEADER_VALUE_SIZE: f32 = 12.;
const SUBHEADER_TABLE_TEXT_SIZE: f32 = 12.;
const INNER_MARGIN: (i8, i8) = (10, 10);

pub struct FooterRenderer;
impl FooterRenderer {
    pub fn render(ui: &mut Ui, footer: &ParkhayFooter) {
        Frame::canvas(ui.style())
            .fill(Color32::from_rgb(246, 246, 246))
            .corner_radius(CORNER_RADIUS)
            .inner_margin(Margin::symmetric(INNER_MARGIN.0, INNER_MARGIN.1))
            .outer_margin(Margin::ZERO)
            .show(ui, |ui| {
                Self::render_layout_label(ui, "Footer");
                ui.add_space(5.);
                Self::render_header_labeled_value(ui, "Version", footer.version.to_string());
                ui.separator();
                Self::render_header_labeled_value(ui, "Num Rows", footer.num_rows.to_string());
                ui.separator();
                Self::render_header_labeled_value(
                    ui,
                    "Created By",
                    footer.created_by.as_deref().unwrap_or("N/A"),
                );
                ui.separator();
                Self::render_header_collapsible(ui, "Schema", |ui| {
                    SchemaRenderer::render(ui, &footer.schema_root)
                });
                ui.separator();
                Self::render_header_collapsible(ui, "Key Value Metadata", |ui| {
                    if let Some(kv_metadata) = &footer.key_value_metadata {
                        Self::render_key_value_metadata(ui, kv_metadata);
                    } else {
                        Self::render_header_value(ui, "N/A");
                    }
                });
                ui.separator();
                Self::render_header_collapsible(ui, "Row Group Metadata", |ui| {
                    RowGroupsRenderer::render(ui, &footer.row_groups);
                });

                // TODO Column Orders
            });
    }

    fn render_layout_label(ui: &mut Ui, label: impl AsRef<str>) {
        Label::new(
            RichText::new(label.as_ref())
                .monospace()
                .size(LAYOUT_LABEL_SIZE)
                .strong(),
        )
        .selectable(false)
        .ui(ui);
    }

    fn render_header_value(ui: &mut Ui, label: impl AsRef<str>) {
        Label::new(
            RichText::new(label.as_ref())
                .monospace()
                .size(HEADER_VALUE_SIZE),
        )
        .selectable(false)
        .ui(ui);
    }

    fn render_header_labeled_value(ui: &mut Ui, label: impl AsRef<str>, value: impl AsRef<str>) {
        LabeledValue::show(
            ui,
            RichText::new(label.as_ref())
                .monospace()
                .size(HEADER_LABEL_SIZE),
            RichText::new(value.as_ref())
                .monospace()
                .size(HEADER_VALUE_SIZE),
        );
    }
    fn render_header_collapsible(
        ui: &mut Ui,
        header: impl AsRef<str>,
        content: impl FnOnce(&mut Ui),
    ) {
        CollapsibleSection::new(
            RichText::new(header.as_ref())
                .monospace()
                .size(HEADER_LABEL_SIZE),
            CollapsibleSectionIcon::Default,
            true,
        )
        .show(ui, content);
    }

    fn render_key_value_metadata(ui: &mut Ui, kv_metadata: &Vec<parquet::format::KeyValue>) {
        ScrollArea::horizontal().show(ui, |ui| {
            Grid::new("Key Value Metadata")
                .num_columns(2)
                .spacing([10., 5.])
                .striped(true)
                .show(ui, |ui| {
                    Label::new(
                        RichText::new("Key")
                            .monospace()
                            .size(SUBHEADER_VALUE_SIZE)
                            .strong(),
                    )
                    .ui(ui);
                    Label::new(
                        RichText::new("Value")
                            .monospace()
                            .size(SUBHEADER_VALUE_SIZE)
                            .strong(),
                    )
                    .ui(ui);
                    ui.end_row();
                    for kv in kv_metadata {
                        Label::new(
                            RichText::new(&kv.key)
                                .monospace()
                                .size(SUBHEADER_VALUE_SIZE),
                        )
                        .ui(ui);
                        if let Some(value) = &kv.value {
                            Label::new(RichText::new(value).monospace().size(SUBHEADER_VALUE_SIZE))
                                .ui(ui);
                        } else {
                            Label::new(RichText::new("N/A").monospace().size(SUBHEADER_VALUE_SIZE))
                                .ui(ui);
                        }
                        ui.end_row();
                    }
                });
            ui.add_space(5.);
        });
    }
}
