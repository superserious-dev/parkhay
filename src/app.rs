use eframe::{CreationContext, Frame};
use egui::{Context, Theme, ViewportCommand};
use log::debug;

use crate::{
    file::ParkhayFile,
    views::{LayoutView, View},
};
use anyhow::Result;

pub struct ParkhayApp {
    active_view: Box<dyn View>,
}

impl ParkhayApp {
    pub fn new(cc: &CreationContext<'_>, parquet_path: String) -> Result<Self> {
        // Read parquet file metadata
        debug!("Reading metadata for file: {parquet_path}");

        let cloned_ctx = cc.egui_ctx.clone();
        let parkhay_file = ParkhayFile::new(&parquet_path)?;

        let page_reader_tx = parkhay_file.spawn_page_reader(move || {
            debug!("Requesting repaint from page reader...");
            cloned_ctx.request_repaint();
        })?;

        let active_view = Box::new(LayoutView::new(parkhay_file, page_reader_tx));

        // Update window title
        cc.egui_ctx
            .send_viewport_cmd(ViewportCommand::Title(parquet_path));

        Ok(Self { active_view })
    }
}

impl eframe::App for ParkhayApp {
    fn update(&mut self, ctx: &Context, frame: &mut Frame) {
        ctx.set_theme(Theme::Light); // Force light theme for now
        self.active_view.as_mut().update(ctx, frame);
    }
}
