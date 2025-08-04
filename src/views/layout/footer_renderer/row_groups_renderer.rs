use egui::{Grid, Label, RichText, ScrollArea, Ui, Widget};

use super::{SUBHEADER_VALUE_SIZE, ui_helpers::UiHelpers};

pub struct RowGroupsRenderer;

impl RowGroupsRenderer {
    pub fn render(ui: &mut Ui, row_groups: &[parquet::format::RowGroup]) {
        for (idx, row_group) in row_groups.iter().enumerate() {
            if idx > 0 {
                ui.separator();
            }
            Self::render_row_group(ui, row_group, idx);
        }
    }

    fn render_row_group(ui: &mut Ui, row_group: &parquet::format::RowGroup, rg_idx: usize) {
        UiHelpers::render_subheader_collapsible(ui, format!("Row Group: {rg_idx}"), |ui| {
            Self::render_column_chunks(ui, row_group, rg_idx);
            ui.separator();
            UiHelpers::render_subheader_labeled_value(
                ui,
                "Total Byte Size",
                row_group.total_byte_size.to_string(),
            );
            ui.separator();
            UiHelpers::render_subheader_labeled_value(
                ui,
                "Num Rows",
                row_group.num_rows.to_string(),
            );
            ui.separator();
            UiHelpers::render_subheader_collapsible(ui, "Sorting Columns", |ui| {
                if let Some(ref sorting_columns) = row_group.sorting_columns {
                    for sorting_column in sorting_columns {
                        UiHelpers::render_subheader_labeled_value(
                            ui,
                            "Column Index",
                            sorting_column.column_idx.to_string(),
                        );
                        ui.separator();
                        UiHelpers::render_subheader_labeled_value(
                            ui,
                            "Descending",
                            sorting_column.descending.to_string(),
                        );
                        ui.separator();
                        UiHelpers::render_subheader_labeled_value(
                            ui,
                            "Nulls First",
                            sorting_column.nulls_first.to_string(),
                        );
                    }
                } else {
                    UiHelpers::render_subheader_value(ui, "N/A");
                }
            });

            ui.separator();

            UiHelpers::render_subheader_labeled_value(
                ui,
                "File Offset",
                row_group
                    .file_offset
                    .map(|v| v.to_string())
                    .unwrap_or("N/A".to_string()),
            );

            ui.separator();

            UiHelpers::render_subheader_labeled_value(
                ui,
                "Total Compressed Size",
                row_group
                    .total_compressed_size
                    .map(|v| v.to_string())
                    .unwrap_or("N/A".to_string()),
            );

            ui.separator();

            UiHelpers::render_subheader_labeled_value(
                ui,
                "Ordinal",
                row_group
                    .ordinal
                    .map(|v| v.to_string())
                    .unwrap_or("N/A".to_string()),
            );
        });
    }

    fn render_column_chunks(ui: &mut Ui, row_group: &parquet::format::RowGroup, rg_idx: usize) {
        UiHelpers::render_subheader_collapsible(ui, "Column Chunks", |ui| {
            for (cc_idx, column_chunk) in row_group.columns.iter().enumerate() {
                if cc_idx != 0 {
                    ui.separator();
                }
                Self::render_column_chunk(ui, rg_idx, column_chunk, cc_idx);
            }
        });
    }

    fn render_column_chunk(
        ui: &mut Ui,
        rg_idx: usize,
        column_chunk: &parquet::format::ColumnChunk,
        cc_idx: usize,
    ) {
        ui.push_id(
            format!("Row Group: {rg_idx}, Column Chunk: {cc_idx}"),
            |ui| {
                UiHelpers::render_subheader_collapsible(
                    ui,
                    format!("Column Chunk: {cc_idx}"),
                    |ui| {
                        UiHelpers::render_subheader_labeled_value(
                            ui,
                            "File Path",
                            column_chunk.file_path.clone().unwrap_or("N/A".to_string()),
                        );

                        ui.separator();

                        UiHelpers::render_subheader_labeled_value(
                            ui,
                            "File Offset (deprecated)",
                            column_chunk.file_offset.to_string(),
                        );

                        ui.separator();

                        UiHelpers::render_subheader_collapsible(ui, "Column Metadata", |ui| {
                            if let Some(ref metadata) = column_chunk.meta_data {
                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Type",
                                    format!(
                                        "{}",
                                        parquet::basic::Type::try_from(metadata.type_).unwrap()
                                    ),
                                );

                                ui.separator();

                                UiHelpers::render_subheader_collapsible(ui, "Encodings", |ui| {
                                    for (encoding_idx, encoding) in
                                        metadata.encodings.iter().enumerate()
                                    {
                                        ui.push_id(
                                            format!("{rg_idx},{cc_idx},{encoding_idx}"),
                                            |ui| {
                                                UiHelpers::render_subheader_labeled_value(
                                                    ui,
                                                    "Encoding",
                                                    parquet::basic::Encoding::try_from(*encoding)
                                                        .unwrap()
                                                        .to_string(),
                                                );
                                            },
                                        );
                                    }
                                });

                                ui.separator();

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Path in Schema",
                                    metadata.path_in_schema.join("/"),
                                );

                                ui.separator();

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Codec",
                                    parquet::basic::Compression::try_from(metadata.codec)
                                        .unwrap()
                                        .to_string(),
                                );

                                ui.separator();

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Num Values",
                                    metadata.num_values.to_string(),
                                );

                                ui.separator();

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Total Uncompressed Size",
                                    metadata.total_uncompressed_size.to_string(),
                                );

                                ui.separator();

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Total Compressed Size",
                                    metadata.total_compressed_size.to_string(),
                                );

                                ui.separator();

                                UiHelpers::render_subheader_collapsible(
                                    ui,
                                    "Key Value Metadata",
                                    |ui| {
                                        if let Some(ref kv_metadata) = metadata.key_value_metadata {
                                            Self::render_key_value_metadata(ui, kv_metadata);
                                        } else {
                                            UiHelpers::render_subheader_value(ui, "N/A");
                                        }
                                    },
                                );

                                ui.separator();

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Data Page Offset",
                                    metadata.data_page_offset.to_string(),
                                );

                                ui.separator();

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Index Page Offset",
                                    metadata
                                        .index_page_offset
                                        .map(|v| v.to_string())
                                        .unwrap_or(String::from("N/A")),
                                );

                                ui.separator();

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Dictionary Page Offset",
                                    metadata
                                        .dictionary_page_offset
                                        .map(|v| v.to_string())
                                        .unwrap_or(String::from("N/A")),
                                );

                                ui.separator();

                                // TODO statistics
                                // TODO encoding stats

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Bloom Filter Offset",
                                    metadata
                                        .bloom_filter_offset
                                        .map(|v| v.to_string())
                                        .unwrap_or(String::from("N/A")),
                                );

                                ui.separator();

                                UiHelpers::render_subheader_labeled_value(
                                    ui,
                                    "Bloom Filter Length",
                                    metadata
                                        .bloom_filter_length
                                        .map(|v| v.to_string())
                                        .unwrap_or(String::from("N/A")),
                                );

                                // TODO size statistics
                                // TODO geospatial statistics
                            } else {
                                UiHelpers::render_subheader_value(ui, "N/A");
                            }
                        });

                        ui.separator();

                        UiHelpers::render_subheader_labeled_value(
                            ui,
                            "Offset Index Offset",
                            column_chunk
                                .offset_index_offset
                                .map(|v| v.to_string())
                                .unwrap_or("N/A".to_string()),
                        );

                        ui.separator();

                        UiHelpers::render_subheader_labeled_value(
                            ui,
                            "Offset Index Length",
                            column_chunk
                                .offset_index_length
                                .map(|v| v.to_string())
                                .unwrap_or("N/A".to_string()),
                        );

                        ui.separator();

                        UiHelpers::render_subheader_labeled_value(
                            ui,
                            "Column Index Offset",
                            column_chunk
                                .column_index_offset
                                .map(|v| v.to_string())
                                .unwrap_or("N/A".to_string()),
                        );

                        ui.separator();

                        UiHelpers::render_subheader_labeled_value(
                            ui,
                            "Column Index Length",
                            column_chunk
                                .column_index_length
                                .map(|v| v.to_string())
                                .unwrap_or("N/A".to_string()),
                        );
                        // TODO crypto metadata
                        // TODO encrypted column metadata
                    },
                );
            },
        );
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
