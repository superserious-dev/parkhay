use egui::{Label, RichText, Ui, Widget};

use crate::views::layout::components::{CollapsibleSection, CollapsibleSectionIcon, LabeledValue};

use super::{SUBHEADER_LABEL_SIZE, SUBHEADER_VALUE_SIZE};

pub struct UiHelpers;
impl UiHelpers {
    pub fn render_subheader_value(ui: &mut Ui, label: impl AsRef<str>) {
        Label::new(
            RichText::new(label.as_ref())
                .monospace()
                .size(SUBHEADER_VALUE_SIZE),
        )
        .selectable(false)
        .ui(ui);
    }

    pub fn render_subheader_labeled_value(
        ui: &mut Ui,
        label: impl AsRef<str>,
        value: impl AsRef<str>,
    ) {
        LabeledValue::show(
            ui,
            RichText::new(label.as_ref())
                .monospace()
                .size(SUBHEADER_LABEL_SIZE),
            RichText::new(value.as_ref())
                .monospace()
                .size(SUBHEADER_VALUE_SIZE),
        );
    }

    pub fn render_subheader_collapsible(
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
}
