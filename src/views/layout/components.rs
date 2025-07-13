use egui::{CollapsingHeader, Response, RichText, Ui, lerp};

// *******************
// COLLAPSIBLE SECTION
// *******************

pub enum CollapsibleSectionIcon {
    Default,
    Blank,
    Circle,
}

pub struct CollapsibleSection {
    header: RichText,
    icon: CollapsibleSectionIcon,
    should_indent: bool,
}

impl CollapsibleSection {
    pub fn new(header: RichText, icon: CollapsibleSectionIcon, should_indent: bool) -> Self {
        Self {
            header,
            icon,
            should_indent,
        }
    }

    pub fn empty_icon(_ui: &mut Ui, _openness: f32, _response: &Response) {}
    pub fn circle_icon(ui: &mut Ui, openness: f32, response: &Response) {
        let stroke = ui.style().interact(response).fg_stroke;
        let radius = lerp(2.0..=3.0, openness);
        ui.painter()
            .circle_filled(response.rect.center(), radius, stroke.color);
    }

    pub fn show(self, ui: &mut Ui, content: impl FnOnce(&mut Ui)) {
        let ch = CollapsingHeader::new(self.header);
        let ch = match self.icon {
            CollapsibleSectionIcon::Default => ch,
            CollapsibleSectionIcon::Blank => ch.icon(Self::empty_icon),
            CollapsibleSectionIcon::Circle => ch.icon(Self::circle_icon),
        };

        if self.should_indent {
            ch.show(ui, content);
        } else {
            ch.show_unindented(ui, content);
        }
    }
}

// *************
// LABELED VALUE
// *************

pub struct LabeledValue;

impl LabeledValue {
    pub fn show(ui: &mut Ui, label: RichText, value: RichText) {
        CollapsibleSection::new(label, CollapsibleSectionIcon::Default, true).show(ui, |ui| {
            ui.label(value);
        });
    }
}
