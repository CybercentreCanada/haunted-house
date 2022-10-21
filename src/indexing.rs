// use std::path::Path;
// use anyhow::Result;
// use uuid::Uuid;

// struct FileUpdate {
//     sha256: Vec<u8>,
//     expiry: chrono::DateTime<chrono::Utc>,
    
// }

// struct IndexID(Uuid);

// struct IndexDirectory {}

// impl IndexDirectory {
//     async fn open(path: &Path) -> Result<Self> {
//         todo!();
//     }

//     async fn get_indices(&self) -> Result<Vec<IndexID>> {
//         todo!()
//     }

//     async fn stage_index(&self, id: &IndexID) -> Result<()> {
//         todo!();
//     }

//     async fn swap_index(&self, new: &IndexID, old: &IndexID) -> Result<()> {
//         todo!();
//     }
// }


// struct IndexHandle {

// }

// struct IndexCache {}

// impl IndexCache {
//     pub fn open(storage: BlobStore, temp_path: &Path, capacity: usize) -> Result<Self> {
//         todo!();
//     }

//     pub async fn load_index(&self, id: &IndexID) -> Result<IndexHandle> {
//         todo!();
//     }

//     pub async fn current_indices(&self) -> Result<Vec<IndexID>> {
//         todo!();
//     }
// }


// struct IndexBuilder {}

// impl IndexBuilder {
//     pub fn open(storage: IndexCache, directory: IndexDirectory) -> Result<Self> {
//         todo!();
//     }

//     pub async fn batch_update(&self, files: Vec<FileUpdate>) -> Result<()> {
//         todo!()
//     }
// }