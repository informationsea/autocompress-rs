#[cfg(feature = "rayon")]
mod rayon;
#[cfg(feature = "rayon")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "rayon")))]
pub use rayon::*;

mod reader;
mod writer;

pub use reader::*;
pub use writer::*;
