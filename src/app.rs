use eframe::{CreationContext, Frame};
use egui::{Context, ViewportCommand};
use log::info;

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
        info!("Reading metadata for file: {parquet_path}");
        let parkhay_file = ParkhayFile::new(&parquet_path)?;
        let active_view = Box::new(LayoutView::new(parkhay_file));

        // Update window title
        cc.egui_ctx
            .send_viewport_cmd(ViewportCommand::Title(parquet_path));

        Ok(Self { active_view })
    }
}

impl eframe::App for ParkhayApp {
    fn update(&mut self, ctx: &Context, frame: &mut Frame) {
        self.active_view.as_mut().update(ctx, frame);
    }
}
