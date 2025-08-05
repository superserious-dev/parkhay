use std::{
    f32,
    sync::{Arc, Mutex, mpsc::Sender},
};

use anyhow::Context;
use egui::{Color32, Frame, Label, Margin, RichText, Sense, Stroke, Ui, UiBuilder, Widget};

use crate::file::{ByteInterval, ParkhayDataSection, ReadRequest};

use super::{
    CORNER_RADIUS,
    components::{CollapsibleSection, CollapsibleSectionIcon, LabeledValue},
};

const LAYOUT_LABEL_SIZE: f32 = 16.;
const HEADER_LABEL_SIZE: f32 = 15.;
const HEADER_VALUE_SIZE: f32 = 14.;
const DATA_BUTTON_SIZE: f32 = 11.;
const DATA_PREVIEW_SIZE: f32 = 13.;
const DATA_PREVIEW_APPROX_ROW_COUNT: usize = 15;

#[derive(Clone, Default, PartialEq)]
enum PreviewState {
    #[default]
    Hidden,
    Visible,
    Pending,
}

pub struct DataRenderer;
impl DataRenderer {
    pub fn render(ui: &mut Ui, data: &ParkhayDataSection, reader_tx: &mut Sender<ReadRequest>) {
        match data {
            ParkhayDataSection::Root(sections) => {
                for (byte_interval, section) in sections {
                    Self::render_section(ui, byte_interval, section, reader_tx.clone());
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

    fn render_header_value(ui: &mut Ui, value: impl AsRef<str>) {
        ui.label(
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

    fn render_section(
        ui: &mut Ui,
        byte_interval: &ByteInterval,
        section: &ParkhayDataSection,
        data_reader_tx: Sender<ReadRequest>,
    ) {
        ui.style_mut().visuals.collapsing_header_frame = true;
        match section {
            ParkhayDataSection::Root(_) => unreachable!(),
            ParkhayDataSection::RowGroup(idx, sections) => {
                Self::render_collapsible_section(ui, &format!("Row Group: {idx}"), |ui| {
                    for (byte_interval, subsection) in sections {
                        Self::render_section(ui, byte_interval, subsection, data_reader_tx.clone());
                    }
                });
            }
            ParkhayDataSection::ColumnChunk(idx, sections, _schema) => {
                Self::render_collapsible_section(ui, &format!("Column Chunk: {idx}"), |ui| {
                    for (byte_interval, subsection) in sections {
                        Self::render_section(ui, byte_interval, subsection, data_reader_tx.clone());
                    }
                });
            }
            ParkhayDataSection::Page(idx, header, data) => {
                Self::render_page(
                    ui,
                    byte_interval,
                    *idx as usize,
                    header,
                    data.clone(),
                    data_reader_tx,
                );
            }
            ParkhayDataSection::OffsetIndex(idx, offset_index) => {
                Self::render_collapsible_section(ui, &format!("Offset Index: {idx}"), |ui| {
                    Self::render_offset_index(ui, offset_index);
                });
            }
            ParkhayDataSection::ColumnIndex(idx, column_index) => {
                Self::render_collapsible_section(ui, &format!("Column Index: {idx}"), |ui| {
                    Self::render_column_index(ui, column_index);
                });
            }
            ParkhayDataSection::BloomFilter(idx, header, bitset) => {
                Self::render_bloom_filter(
                    ui,
                    byte_interval,
                    *idx as usize,
                    header,
                    bitset.clone(),
                    data_reader_tx,
                );
            }
        }
        ui.style_mut().visuals.collapsing_header_frame = false;
    }

    fn render_collapsible_section(
        ui: &mut Ui,
        identifier: &str,
        section_content: impl FnOnce(&mut Ui),
    ) {
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
    }

    fn render_page(
        ui: &mut Ui,
        byte_interval: &ByteInterval,
        page_idx: usize,
        page_header: &parquet::format::PageHeader,
        page_data: Arc<Mutex<Option<Vec<u8>>>>,
        data_reader_tx: Sender<ReadRequest>,
    ) {
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

                    ui.separator();

                    // Get current preview state, setting it to default if it's not set
                    let current_state =
                        ui.data_mut(|d| d.get_temp_mut_or_default::<PreviewState>(id).clone());

                    // Compute next state based on current state and UI interactions
                    let next_state = match current_state {
                        PreviewState::Hidden => {
                            let button_clicked = ui
                                .vertical_centered_justified(|ui| {
                                    ui.button(
                                        RichText::new("Show Preview")
                                            .monospace()
                                            .size(DATA_BUTTON_SIZE)
                                            .strong(),
                                    )
                                    .clicked()
                                })
                                .inner;

                            if button_clicked {
                                if let Ok(pd) = page_data.lock() {
                                    if pd.is_some() {
                                        PreviewState::Visible
                                    } else {
                                        PreviewState::Pending
                                    }
                                } else {
                                    panic!("Can't get lock on page data");
                                }
                            } else {
                                current_state.clone()
                            }
                        }
                        PreviewState::Visible => {
                            let button_clicked = ui
                                .vertical_centered_justified(|ui| {
                                    ui.button(
                                        RichText::new("Hide Preview")
                                            .monospace()
                                            .size(DATA_BUTTON_SIZE)
                                            .strong(),
                                    )
                                    .clicked()
                                })
                                .inner;

                            if button_clicked {
                                PreviewState::Hidden
                            } else {
                                current_state.clone()
                            }
                        }
                        PreviewState::Pending => {
                            if let Ok(pd) = page_data.lock() {
                                if pd.is_some() {
                                    PreviewState::Visible
                                } else {
                                    ui.vertical_centered_justified(|ui| {
                                        ui.add_enabled(
                                            false,
                                            egui::Button::new(
                                                RichText::new("Show Preview")
                                                    .monospace()
                                                    .size(DATA_BUTTON_SIZE)
                                                    .strong(),
                                            ),
                                        );
                                    });
                                    current_state.clone()
                                }
                            } else {
                                panic!("Can't get lock on page data");
                            }
                        }
                    };

                    // Store next state
                    ui.data_mut(|d| {
                        d.insert_temp(id, next_state.clone());
                    });

                    match (current_state, next_state) {
                        // Show cached data
                        // Show newly fetched data
                        // Keep showing cached data
                        (PreviewState::Hidden, PreviewState::Visible)
                        | (PreviewState::Pending, PreviewState::Visible)
                        | (PreviewState::Visible, PreviewState::Visible) => {
                            if let Ok(pd) = page_data.lock() {
                                if let Some(ref pd_bytes) = *pd {
                                    Self::render_data_preview(ui, pd_bytes);
                                }
                            } else {
                                panic!("Can't get lock on page data");
                            }
                        }
                        // Fetch data
                        (PreviewState::Hidden, PreviewState::Pending) => {
                            data_reader_tx
                                .send(ReadRequest(*byte_interval, page_data.clone()))
                                .context("Couldn't send message to reader thread")
                                .unwrap();
                        }
                        // Invalid states
                        (PreviewState::Visible, PreviewState::Pending)
                        | (PreviewState::Pending, PreviewState::Hidden) => {
                            unreachable!()
                        }
                        // Don't show any data
                        (PreviewState::Visible, PreviewState::Hidden)
                        | (PreviewState::Pending, PreviewState::Pending)
                        | (PreviewState::Hidden, PreviewState::Hidden) => {}
                    }
                });
        });
    }

    fn render_data_preview(ui: &mut Ui, pd_bytes: &[u8]) {
        let font_id = egui::FontId::monospace(DATA_PREVIEW_SIZE);
        let char_width = ui.fonts(|fonts| {
            fonts.glyph_width(&font_id, 'a') // Pick an arbitrary char since the font is monospace
        });

        let byte_count_per_row = (ui.available_width() / (2. * char_width)).floor() as usize; // 2 hex chars for each byte
        let bytes_to_take = byte_count_per_row * DATA_PREVIEW_APPROX_ROW_COUNT;

        let hex_string = pd_bytes
            .iter()
            .take(bytes_to_take)
            .map(|b| format!("{b:02x}"))
            .collect::<String>();

        ui.label(
            RichText::new(hex_string)
                .monospace()
                .size(DATA_PREVIEW_SIZE),
        );

        let pd_bytes_len = pd_bytes.len();
        if bytes_to_take < pd_bytes_len {
            ui.add(egui::Label::new(
                RichText::new(format!(
                    "Truncated {} bytes for preview.",
                    pd_bytes_len - bytes_to_take
                ))
                .size(DATA_BUTTON_SIZE)
                .background_color(Color32::from_rgb(250, 230, 170))
                .monospace()
                .strong(),
            ));
        }
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
                    Self::render_header_value(ui, "N/A");
                }
            });
            ui.separator();
            Self::render_header_collapsible(ui, "Dictionary Page Header", |ui| {
                if let Some(dictionary_page_header) = &page_header.dictionary_page_header {
                    Self::render_dictionary_page_header(ui, dictionary_page_header);
                } else {
                    Self::render_header_value(ui, "N/A");
                }
            });
            ui.separator();
            Self::render_header_collapsible(ui, "Data Page Header V2", |ui| {
                if let Some(data_page_header_v2) = &page_header.data_page_header_v2 {
                    Self::render_data_page_header_v2(ui, data_page_header_v2);
                } else {
                    Self::render_header_value(ui, "N/A");
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
                Self::render_header_value(ui, "N/A");
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
                Self::render_header_value(ui, "N/A");
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

    fn render_column_index(ui: &mut Ui, column_index: &parquet::format::ColumnIndex) {
        Self::render_header_labeled_value(
            ui,
            "Null Pages",
            format!("{:?}", column_index.null_pages),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Min Values",
            format!("{:?}", column_index.min_values),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Max Values",
            format!("{:?}", column_index.max_values),
        );
        ui.separator();
        // TODO find a better way to find human-readable boundary order
        let boundary_order = match column_index.boundary_order.0 {
            0 => "UNORDERED",
            1 => "ASCENDING",
            2 => "DESCENDING",
            n => panic!("Unexpected boundary Order {n} found"),
        };
        Self::render_header_labeled_value(ui, "Boundary Order", boundary_order);
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Null Counts",
            column_index
                .null_counts
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Repetition Level Histograms",
            column_index
                .repetition_level_histograms
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
        ui.separator();
        Self::render_header_labeled_value(
            ui,
            "Definition Level Histograms",
            column_index
                .definition_level_histograms
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
    }

    fn render_offset_index(ui: &mut Ui, offset_index: &parquet::format::OffsetIndex) {
        for (page_offset_idx, page_location) in offset_index.page_locations.iter().enumerate() {
            let identifier = format!("Page Location: {page_offset_idx}");
            let id = ui.make_persistent_id(&identifier);
            ui.push_id(id, |ui| {
                Self::render_header_collapsible(ui, identifier, |ui| {
                    Self::render_header_labeled_value(
                        ui,
                        "Offset",
                        page_location.offset.to_string(),
                    );
                    ui.separator();
                    Self::render_header_labeled_value(
                        ui,
                        "Compressed Page Size",
                        page_location.compressed_page_size.to_string(),
                    );
                    ui.separator();
                    Self::render_header_labeled_value(
                        ui,
                        "First Row Index",
                        page_location.first_row_index.to_string(),
                    );
                });
                ui.separator();
            });
        }

        Self::render_header_labeled_value(
            ui,
            "Offset",
            offset_index
                .unencoded_byte_array_data_bytes
                .as_ref()
                .map(|v| format!("{v:?}"))
                .unwrap_or(String::from("N/A")),
        );
    }

    fn render_bloom_filter(
        ui: &mut Ui,
        byte_interval: &ByteInterval,
        bf_idx: usize,
        header: &parquet::format::BloomFilterHeader,
        bitset: Arc<Mutex<Option<Vec<u8>>>>,
        reader_tx: Sender<ReadRequest>,
    ) {
        let identifier = format!("Bloom Filter: {bf_idx}");

        let id = ui.make_persistent_id(&identifier);
        Self::render_collapsible_section(ui, &identifier, |ui| {
            ui.set_width(ui.available_width());

            Self::render_bloom_filter_header(ui, header);

            ui.separator();

            // Get current preview state, setting it to default if it's not set
            let current_state =
                ui.data_mut(|d| d.get_temp_mut_or_default::<PreviewState>(id).clone());

            // Compute next state based on current state and UI interactions
            let next_state = match current_state {
                PreviewState::Hidden => {
                    let button_clicked = ui
                        .vertical_centered_justified(|ui| {
                            ui.button(
                                RichText::new("Show Preview")
                                    .monospace()
                                    .size(DATA_BUTTON_SIZE)
                                    .strong(),
                            )
                            .clicked()
                        })
                        .inner;

                    if button_clicked {
                        if let Ok(pd) = bitset.lock() {
                            if pd.is_some() {
                                PreviewState::Visible
                            } else {
                                PreviewState::Pending
                            }
                        } else {
                            panic!("Can't get lock on bitset data");
                        }
                    } else {
                        current_state.clone()
                    }
                }
                PreviewState::Visible => {
                    let button_clicked = ui
                        .vertical_centered_justified(|ui| {
                            ui.button(
                                RichText::new("Hide Preview")
                                    .monospace()
                                    .size(DATA_BUTTON_SIZE)
                                    .strong(),
                            )
                            .clicked()
                        })
                        .inner;

                    if button_clicked {
                        PreviewState::Hidden
                    } else {
                        current_state.clone()
                    }
                }
                PreviewState::Pending => {
                    if let Ok(pd) = bitset.lock() {
                        if pd.is_some() {
                            PreviewState::Visible
                        } else {
                            ui.vertical_centered_justified(|ui| {
                                ui.add_enabled(
                                    false,
                                    egui::Button::new(
                                        RichText::new("Show Preview")
                                            .monospace()
                                            .size(DATA_BUTTON_SIZE)
                                            .strong(),
                                    ),
                                );
                            });
                            current_state.clone()
                        }
                    } else {
                        panic!("Can't get lock on bitset data");
                    }
                }
            };

            // Store next state
            ui.data_mut(|d| {
                d.insert_temp(id, next_state.clone());
            });

            match (current_state, next_state) {
                // Show cached data
                // Show newly fetched data
                // Keep showing cached data
                (PreviewState::Hidden, PreviewState::Visible)
                | (PreviewState::Pending, PreviewState::Visible)
                | (PreviewState::Visible, PreviewState::Visible) => {
                    if let Ok(pd) = bitset.lock() {
                        if let Some(ref pd_bytes) = *pd {
                            Self::render_data_preview(ui, pd_bytes);
                        }
                    } else {
                        panic!("Can't get lock on bitset data");
                    }
                }
                // Fetch data
                (PreviewState::Hidden, PreviewState::Pending) => {
                    reader_tx
                        .send(ReadRequest(*byte_interval, bitset.clone()))
                        .context("Couldn't send message to reader thread")
                        .unwrap();
                }
                // Invalid states
                (PreviewState::Visible, PreviewState::Pending)
                | (PreviewState::Pending, PreviewState::Hidden) => {
                    unreachable!()
                }
                // Don't show any data
                (PreviewState::Visible, PreviewState::Hidden)
                | (PreviewState::Pending, PreviewState::Pending)
                | (PreviewState::Hidden, PreviewState::Hidden) => {}
            }
        });
    }

    fn render_bloom_filter_header(ui: &mut Ui, header: &parquet::format::BloomFilterHeader) {
        Self::render_header_collapsible(ui, "Header", |ui| {
            Self::render_header_labeled_value(ui, "Num Bytes", header.num_bytes.to_string());
            ui.separator();
            Self::render_header_labeled_value(
                ui,
                "Bloom Filter Algorithm",
                format!("{:?}", header.algorithm),
            );
            ui.separator();
            Self::render_header_labeled_value(
                ui,
                "Bloom Filter Hash",
                format!("{:?}", header.hash),
            );
            ui.separator();
            Self::render_header_labeled_value(
                ui,
                "Bloom Filter Compression",
                format!("{:?}", header.compression),
            );
        });
    }
}
