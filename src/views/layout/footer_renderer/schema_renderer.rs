use egui::{Grid, RichText, Ui};

use crate::{
    file::Field,
    views::layout::components::{CollapsibleSection, CollapsibleSectionIcon},
};

use super::{SUBHEADER_LABEL_SIZE, SUBHEADER_TABLE_TEXT_SIZE};

#[derive(Clone, Copy, PartialEq)]
enum SchemaMode {
    Message,
    Tree,
}

pub struct SchemaRenderer;

impl SchemaRenderer {
    const SCHEMA_MODE_DATA_KEY: &str = "schema_mode";

    pub fn render(ui: &mut Ui, schema_root: &Field) {
        ui.add_space(5.);

        let id = ui.make_persistent_id(Self::SCHEMA_MODE_DATA_KEY);
        let mut selected =
            ui.data_mut(|d| *d.get_temp_mut_or_insert_with::<SchemaMode>(id, || SchemaMode::Tree));
        ui.horizontal(|ui| {
            ui.selectable_value(
                &mut selected,
                SchemaMode::Tree,
                RichText::new("Tree").monospace().size(SUBHEADER_LABEL_SIZE),
            );
            ui.selectable_value(
                &mut selected,
                SchemaMode::Message,
                RichText::new("Message")
                    .monospace()
                    .size(SUBHEADER_LABEL_SIZE),
            );
        });
        ui.add_space(10.);
        match selected {
            SchemaMode::Tree => Self::render_schema_field(ui, schema_root),
            SchemaMode::Message => {
                ui.label(
                    RichText::new(Self::schema_message_string(schema_root))
                        .monospace()
                        .size(12.),
                );
            }
        }

        ui.data_mut(|d| d.insert_temp::<SchemaMode>(id, selected));
    }

    fn render_schema_table_row(ui: &mut Ui, label: impl AsRef<str>, value: impl AsRef<str>) {
        ui.label(
            RichText::new(label.as_ref())
                .monospace()
                .strong()
                .size(SUBHEADER_TABLE_TEXT_SIZE),
        );
        ui.label(
            RichText::new(value.as_ref())
                .monospace()
                .size(SUBHEADER_TABLE_TEXT_SIZE),
        );
        ui.end_row();
    }

    fn render_schema_field(ui: &mut Ui, field: &Field) {
        ui.style_mut().visuals.indent_has_left_vline = false;
        if field.is_primitive() {
            Self::render_primitive_field(ui, field);
        } else if field.is_group() {
            Self::render_group_field(ui, field);
        }
        ui.style_mut().visuals.indent_has_left_vline = false;
    }

    fn render_primitive_field(ui: &mut Ui, field: &Field) {
        Self::render_schema_primitive_collapsible(ui, field.name(), |ui| {
            ui.add_space(15.);
            Grid::new(format!("field_metadata_{}", field.name()))
                .num_columns(2)
                .spacing([10., 5.])
                .striped(true)
                .show(ui, |ui| {
                    Self::render_schema_table_row(ui, "Name", field.name());
                    if field.get_basic_info().has_repetition() {
                        Self::render_schema_table_row(
                            ui,
                            "Repetition",
                            field.get_basic_info().repetition().to_string(),
                        );
                    } else {
                        Self::render_schema_table_row(ui, "Repetition", "N/A");
                    }

                    Self::render_schema_table_row(
                        ui,
                        "Physical Type",
                        field.get_physical_type().to_string(),
                    );

                    Self::render_schema_table_row(
                        ui,
                        "Logical Type",
                        field
                            .get_basic_info()
                            .logical_type()
                            .map_or(String::from("N/A"), |v| format!("{v:?}")),
                    );

                    Self::render_schema_table_row(
                        ui,
                        "Converted Type",
                        field.get_basic_info().converted_type().to_string(),
                    );

                    Self::render_schema_table_row(ui, "Scale", field.get_scale().to_string());

                    Self::render_schema_table_row(
                        ui,
                        "Precision",
                        field.get_precision().to_string(),
                    );
                });
            ui.add_space(15.);
        });
    }

    fn render_group_field(ui: &mut Ui, field: &Field) {
        Self::render_schema_group_collapsible(ui, field.name(), |ui| {
            Self::render_schema_metadata_collapsible(ui, "[metadata]", |ui| {
                ui.add_space(15.);
                ui.indent("indent", |ui| {
                    Grid::new(field.name())
                        .num_columns(2)
                        .spacing([10., 5.])
                        .striped(true)
                        .show(ui, |ui| {
                            Self::render_schema_table_row(ui, "Name", field.name());
                            if field.get_basic_info().has_repetition() {
                                Self::render_schema_table_row(
                                    ui,
                                    "Repetition",
                                    field.get_basic_info().repetition().to_string(),
                                );
                            } else {
                                Self::render_schema_table_row(ui, "Repetition", "N/A");
                            }

                            Self::render_schema_table_row(
                                ui,
                                "Logical Type",
                                field
                                    .get_basic_info()
                                    .logical_type()
                                    .map_or(String::from("N/A"), |v| format!("{v:?}")),
                            );

                            Self::render_schema_table_row(
                                ui,
                                "Converted Type",
                                field.get_basic_info().converted_type().to_string(),
                            );
                        });
                });
                ui.add_space(15.);
            });
            for child_field in field.get_fields() {
                Self::render_schema_field(ui, child_field);
            }
        });
    }

    fn render_schema_primitive_collapsible(
        ui: &mut Ui,
        header: impl AsRef<str>,
        content: impl FnOnce(&mut Ui),
    ) {
        CollapsibleSection::new(
            RichText::new(header.as_ref())
                .monospace()
                .size(SUBHEADER_LABEL_SIZE),
            CollapsibleSectionIcon::Circle,
            true,
        )
        .show(ui, content);
    }

    fn render_schema_group_collapsible(
        ui: &mut Ui,
        header: impl AsRef<str>,
        content: impl FnOnce(&mut Ui),
    ) {
        CollapsibleSection::new(
            RichText::new(header.as_ref())
                .monospace()
                .size(SUBHEADER_LABEL_SIZE),
            CollapsibleSectionIcon::Default,
            true,
        )
        .show(ui, content);
    }

    fn render_schema_metadata_collapsible(
        ui: &mut Ui,
        header: impl AsRef<str>,
        content: impl FnOnce(&mut Ui),
    ) {
        CollapsibleSection::new(
            RichText::new(header.as_ref()).monospace().small(),
            CollapsibleSectionIcon::Blank,
            false,
        )
        .show(ui, content);
    }

    fn schema_message_string(schema_root: &Field) -> String {
        let mut out = vec![];
        parquet::schema::printer::print_schema(&mut out, schema_root);
        String::from_utf8_lossy(&out).to_string()
    }
}
