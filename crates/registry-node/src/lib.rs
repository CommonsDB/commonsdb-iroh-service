//! The `iroh` integration layer: node identity, the `iroh-blobs`-backed
//! `BlobStore`, and the `iroh-docs`-backed root pointer document. See
//! docs/data-model.md and docs/operator-guide.md. This is the shared
//! plumbing between the operator-side daemon (`registryd`) and the
//! distributable reader client (`storectl`).

pub mod blob_store;
pub mod identity;
pub mod node;
pub mod pointer_doc;

pub use blob_store::IrohBlobStore;
pub use node::RegistryNode;
pub use pointer_doc::{PointerDoc, ISCC_INDEX_ROOT_KEY};

pub use iroh;
pub use iroh_blobs;
pub use iroh_docs;
pub use iroh_gossip;
