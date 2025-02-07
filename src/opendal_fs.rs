use std::ops::Range;
use std::path::Path;

use bytes::Bytes;
use opendal::Operator;
use opendal::services;

use crate::{
    AsyncFileRead, AsyncFileWrite, FileLock, FileSystem, FileSystemError,
    IoResult,
};

#[derive(Clone)]
pub struct OpendalFileSystem {
    operator: Operator,
}

impl OpendalFileSystem {
    pub fn new_local_fs(path: &str) -> Result<Self, FileSystemError> {
        let builder = services::Fs::default().root(path);
        let op: Operator = Operator::new(builder)?.finish();

        Ok(Self { operator: op })
    }

    pub fn new_s3(
        path: &str,
        bucket: &str,
        endpoint: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> IoResult<Self> {
        let builder = services::S3::default()
            .root(path)
            .bucket(bucket)
            .endpoint(endpoint)
            .region("auto")
            .access_key_id(access_key_id)
            .secret_access_key(secret_access_key)
            .enable_virtual_host_style();

        let operator = Operator::new(builder)?.finish();

        Ok(Self { operator })
    }

    pub fn from_operator(operator: Operator) -> Self {
        Self { operator }
    }

    fn path_to_str(path: &Path) -> IoResult<&str> {
        path.to_str().ok_or_else(|| {
            FileSystemError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", path))
        })
    }
}

#[async_trait::async_trait]
impl FileSystem for OpendalFileSystem {

    async fn new_random_access_file(
        &self,
        path: &Path,
    ) -> IoResult<Box<dyn AsyncFileRead>> {
        let path_str = Self::path_to_str(path)?;
        let reader = self.operator.reader(path_str).await?;
        Ok(Box::new(OpendalRandomAccessFile { reader }))
    }

    async fn new_writable_file(&self, path: &Path) -> IoResult<Box<dyn AsyncFileWrite>> {
        let path_str = Self::path_to_str(path)?;
        let writer = self.operator.writer(path_str).await?;
        Ok(Box::new(OpendalWritableFile { writer }))
    }

    async fn create_dir(&self, path: &Path) -> IoResult<()> {
        let path_str = Self::path_to_str(path)?;
        self.operator.create_dir(path_str).await?;
        Ok(())
    }

    async fn delete_dir(&self, path: &Path) -> IoResult<()> {
        let path_str = Self::path_to_str(path)?;
        self.operator.delete(path_str).await?;
        Ok(())
    }

    async fn delete_file(&self, path: &Path) -> IoResult<()> {
        let path_str = Self::path_to_str(path)?;
        self.operator.delete(path_str).await?;
        Ok(())
    }

    async fn rename_file(&self, src: &Path, dst: &Path) -> IoResult<()> {
        let src_str = Self::path_to_str(src)?;
        let dst_str = Self::path_to_str(dst)?;
        self.operator.rename(src_str, dst_str).await?;
        Ok(())
    }

    async fn file_exists(&self, path: &Path) -> IoResult<bool> {
        let path_str = Self::path_to_str(path)?;
        
        match self.operator.exists(path_str).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(FileSystemError::OpendalError(e)),
        }
    }

    async fn get_file_size(&self, path: &Path) -> IoResult<u64> {
        let path_str = Self::path_to_str(path)?;
        let meta = self.operator.stat(path_str).await?;
        Ok(meta.content_length())
    }

    async fn lock_file(&self, _path: &Path) -> IoResult<Box<dyn FileLock>> {
        Err(FileSystemError::NotSupported("not supported".to_string()))
    }

    async fn unlock_file(&self, _lock: Box<dyn FileLock>) -> IoResult<()> {
        Ok(())
    }
}

pub struct OpendalRandomAccessFile {
    reader: opendal::Reader,
}

#[async_trait::async_trait]
impl AsyncFileRead for OpendalRandomAccessFile {
    async fn read(&mut self, range: Range<u64>) -> IoResult<Bytes> {
        Ok(self.reader.read(range).await?.to_bytes())
    }
}

struct OpendalWritableFile {
    writer: opendal::Writer,
}

#[async_trait::async_trait]
impl AsyncFileWrite for OpendalWritableFile {
    async fn write(&mut self, data: Bytes) -> IoResult<()> {
        self.writer.write(data).await?;
        Ok(())
    }

    async fn close(&mut self) -> IoResult<()> {
        self.writer.close().await?;
        Ok(())
    }
}
