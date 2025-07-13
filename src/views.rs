pub trait View {
    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame);
}

mod layout;

pub use layout::LayoutView;
