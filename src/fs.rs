use std::{ops::Range, path::Path};

use bytes::Bytes;

use crate::IoResult;

#[async_trait::async_trait]
pub trait FileSystem: Send + Sync {    
    async fn new_random_access_file(&self, path: &Path) -> IoResult<Box<dyn AsyncFileRead>>;
    
    async fn new_writable_file(&self, path: &Path) -> IoResult<Box<dyn AsyncFileWrite>>;
    
    async fn create_dir(&self, path: &Path) -> IoResult<()>;
    async fn delete_dir(&self, path: &Path) -> IoResult<()>;
    
    async fn delete_file(&self, path: &Path) -> IoResult<()>;
    async fn rename_file(&self, src: &Path, dst: &Path) -> IoResult<()>;
    
    async fn file_exists(&self, path: &Path) -> IoResult<bool>;
    async fn get_file_size(&self, path: &Path) -> IoResult<u64>;
    
    async fn lock_file(&self, path: &Path) -> IoResult<Box<dyn FileLock>>;
    async fn unlock_file(&self, lock: Box<dyn FileLock>) -> IoResult<()>;
}

pub trait FileLock: Send + Sync {}

#[async_trait::async_trait]
pub trait AsyncFileRead: Send + Sync {
    async fn read(&mut self, range: Range<u64>) -> IoResult<Bytes>;
}

#[async_trait::async_trait]
pub trait AsyncFileWrite: Send + Sync {
    async fn write(&mut self, data: Bytes) -> IoResult<()>;
    async fn close(&mut self) -> IoResult<()>;
}