use egui::{Frame, Label, Margin, RichText, Sense, Stroke, Ui, UiBuilder, Widget};

use crate::file::ParkhayDataSection;

use super::{
    CORNER_RADIUS,
    components::{CollapsibleSection, CollapsibleSectionIcon, LabeledValue},
};

const LAYOUT_LABEL_SIZE: f32 = 16.;
const HEADER_LABEL_SIZE: f32 = 15.;
const HEADER_VALUE_SIZE: f32 = 14.;

pub struct DataRenderer;
impl DataRenderer {
    pub fn render(ui: &mut Ui, data: &ParkhayDataSection) {
        match data {
            ParkhayDataSection::Root(sections) => {
                for section in sections.values() {
                    Self::render_section(ui, section);
                }
            }
            _ => unreachable!(),
        }
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

    fn render_section(ui: &mut Ui, section: &ParkhayDataSection) {
        match section {
            ParkhayDataSection::Root(_) => unreachable!(),
            ParkhayDataSection::RowGroup(idx, sections) => {
                Self::render_collapsible_section(ui, &format!("Row Group: {idx}"), |ui| {
                    for subsection in sections.values() {
                        Self::render_section(ui, subsection);
                    }
                });
            }
            ParkhayDataSection::ColumnChunk(idx, sections, _schema) => {
                Self::render_collapsible_section(ui, &format!("Column Chunk: {idx}"), |ui| {
                    for subsection in sections.values() {
                        Self::render_section(ui, subsection);
                    }
                });
            }
            ParkhayDataSection::Page(idx, page_header) => {
                Self::render_page(ui, *idx as usize, page_header);
            }
        }
    }

    fn render_collapsible_section(
        ui: &mut Ui,
        identifier: &str,
        section_content: impl FnOnce(&mut Ui),
    ) {
        ui.style_mut().visuals.collapsing_header_frame = true;
        let id = ui.make_persistent_id(identifier);
        ui.scope_builder(UiBuilder::new().id_salt(id).sense(Sense::click()), |ui| {
            let response = ui.response();
            let mut is_visible =
                ui.data_mut(|d| *d.get_temp_mut_or_insert_with::<bool>(id, || false));

            if response.clicked() {
                ui.data_mut(|d| {
                    d.insert_temp(id, !is_visible);
                    is_visible = !is_visible;
                })
            }

            let visuals = *ui.style().interact(&response);

            let bg_stroke = Stroke::new(1., visuals.bg_stroke.color);
            let bg_fill = visuals.bg_fill.gamma_multiply(0.2);

            Frame::canvas(ui.style())
                .corner_radius(CORNER_RADIUS)
                .fill(bg_fill)
                .stroke(bg_stroke)
                .inner_margin(ui.spacing().menu_margin)
                .outer_margin(Margin::ZERO)
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    Self::render_layout_label(ui, identifier);
                    if is_visible {
                        ui.scope(section_content);
                    }
                });
        });
        ui.style_mut().visuals.collapsing_header_frame = false;
    }

    fn render_page(ui: &mut Ui, page_idx: usize, page_header: &parquet::format::PageHeader) {
        let identifier = format!("Page: {page_idx}");

        let id = ui.make_persistent_id(&identifier);
        ui.scope_builder(UiBuilder::new().id_salt(id).sense(Sense::click()), |ui| {
            let response = ui.response();
            let visuals = *ui.style().interact(&response);

            let bg_stroke = Stroke::new(1., visuals.bg_stroke.color);
            let bg_fill = visuals.bg_fill.gamma_multiply(0.2);

            Frame::canvas(ui.style())
                .corner_radius(CORNER_RADIUS)
                .fill(bg_fill)
                .stroke(bg_stroke)
                .inner_margin(ui.spacing().menu_margin)
                .outer_margin(Margin::ZERO)
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    Self::render_layout_label(ui, identifier);

                    Self::render_page_header(ui, page_header);
                    ui.label("Page Data"); // TODO fetch page data on demand
                });
        });
    }

    fn render_page_header(ui: &mut Ui, page_header: &parquet::format::PageHeader) {
        Self::render_header_collapsible(ui, "Page Header", |ui| {
            Self::render_header_labeled_value(
                ui,
                "Page Type",
                format!(
                    "{}",
                    parquet::basic::PageType::try_from(page_header.type_).unwrap()
                ),
            );
            ui.separator();
            Self::render_header_labeled_value(
                ui,
                "Uncompressed Page Size",
                page_header.uncompressed_page_size.to_string(),
            );
            ui.separator();
            Self::render_header_labeled_value(
                ui,
                "Compressed Page Size",
                page_header.compressed_page_size.to_string(),
            );
            ui.separator();
            Self::render_header_labeled_value(
                ui,
                "CRC",
                page_header
                    .crc
                    .map(|v| v.to_string())
                    .unwrap_or(String::from("N/A")),
            );
            ui.separator();
            Self::render_header_collapsible(ui, "Data Page Header", |ui| {
                if let Some(data_page_header) = &page_header.data_page_header {
                    Self::render_data_page_header(ui, data_page_header);
                } else {
                    ui.label("N/A");
                }
            });
            ui.separator();
            Self::render_header_collapsible(ui, "Dictionary Page Header", |ui| {
                if let Some(dictionary_page_header) = &page_header.dictionary_page_header {
                    Self::render_dictionary_page_header(ui, dictionary_page_header);
                } else {
                    ui.label("N/A");
                }
            });
            // TODO index page header
            ui.separator();
            Self::render_header_collapsible(ui, "Data Page Header V2", |ui| {
                if let Some(data_page_header_v2) = &page_header.data_page_header_v2 {
                    Self::render_data_page_header_v2(ui, data_page_header_v2);
                } else {
                    ui.label("N/A");
                }
            });
        });
    }

    fn render_data_page_header(ui: &mut Ui, data_page_header: &parquet::format::DataPageHeader) {
        Self::render_header_labeled_value(
            ui,
            "Num Values",
            data_page_header.num_values.to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Encoding",
            parquet::basic::Encoding::try_from(data_page_header.encoding)
                .unwrap()
                .to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Definition Level Encoding",
            parquet::basic::Encoding::try_from(data_page_header.definition_level_encoding)
                .unwrap()
                .to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Repetition Level Encoding",
            parquet::basic::Encoding::try_from(data_page_header.repetition_level_encoding)
                .unwrap()
                .to_string(),
        );
        ui.separator();
        Self::render_header_collapsible(ui, "Statistics", |ui| {
            if let Some(statistics) = &data_page_header.statistics {
                Self::render_statistics(ui, statistics);
            } else {
                ui.label("N/A");
            }
        });
    }

    fn render_data_page_header_v2(
        ui: &mut Ui,
        data_page_header_v2: &parquet::format::DataPageHeaderV2,
    ) {
        Self::render_header_labeled_value(
            ui,
            "Num Values",
            data_page_header_v2.num_values.to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Num Nulls",
            data_page_header_v2.num_nulls.to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(ui, "Num Rows", data_page_header_v2.num_rows.to_string());
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Encoding",
            parquet::basic::Encoding::try_from(data_page_header_v2.encoding)
                .unwrap()
                .to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Definition Levels Byte Length",
            data_page_header_v2
                .definition_levels_byte_length
                .to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Repetition Levels Byte Length",
            data_page_header_v2
                .repetition_levels_byte_length
                .to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Is Compressed",
            data_page_header_v2
                .is_compressed
                .map(|v| v.to_string())
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_collapsible(ui, "Statistics", |ui| {
            if let Some(statistics) = &data_page_header_v2.statistics {
                Self::render_statistics(ui, statistics);
            } else {
                ui.label("N/A");
            }
        });
    }

    fn render_dictionary_page_header(
        ui: &mut Ui,
        dictionary_page_header: &parquet::format::DictionaryPageHeader,
    ) {
        Self::render_header_labeled_value(
            ui,
            "Num Values",
            dictionary_page_header.num_values.to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Encoding",
            parquet::basic::Encoding::try_from(dictionary_page_header.encoding)
                .unwrap()
                .to_string(),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Is Sorted",
            dictionary_page_header
                .is_sorted
                .map(|v| v.to_string())
                .unwrap_or(String::from("N/A")),
        );
    }

    fn render_statistics(ui: &mut Ui, statistics: &parquet::format::Statistics) {
        Self::render_header_labeled_value(
            ui,
            "Max (deprecated)",
            statistics
                .max
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Min (deprecated)",
            statistics
                .min
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Null Count",
            statistics
                .null_count
                .map(|v| v.to_string())
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Distinct Count",
            statistics
                .distinct_count
                .map(|v| v.to_string())
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Max Value",
            statistics
                .max_value
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Min Value",
            statistics
                .min_value
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Is Max Value Exact",
            statistics
                .is_max_value_exact
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Is Min Value Exact",
            statistics
                .is_min_value_exact
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
    }
}
