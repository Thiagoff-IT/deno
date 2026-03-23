// Copyright 2018-2026 the Deno authors. MIT license.

use std::collections::HashMap;
use std::io::Error;
use std::path::Path;
use std::path::PathBuf;

use deno_path_util::fs::atomic_write_file_with_retries;
use sha2::{Digest, Sha512};
use sys_traits::FsCreateDirAll;
use sys_traits::FsHardLink;
use sys_traits::FsMetadata;
use sys_traits::FsRead;
use sys_traits::PathsInErrorsExt;

use crate::index::StoreIndex;
use crate::FileInfo;

#[derive(Debug)]
pub struct ContentAddressableStore<TSys> {
    sys: TSys,
    store_dir: PathBuf,
    index: StoreIndex,
}

impl<TSys> ContentAddressableStore<TSys>
where
    TSys: FsCreateDirAll + FsHardLink + FsMetadata + FsRead + PathsInErrorsExt,
{
    pub fn new(sys: TSys, store_dir: PathBuf) -> Result<Self, Error> {
        let index = StoreIndex::new(store_dir.join("index.db"))?;
        Ok(Self {
            sys,
            store_dir,
            index,
        })
    }

    pub fn add_files_from_dir(
        &self,
        source_dir: &Path,
        pkg_id: &str,
        integrity: &str,
    ) -> Result<(), Error> {
        let mut files_index = HashMap::new();

        self.walk_and_add_dir(source_dir, &mut files_index, Path::new(""))?;

        let key = format!("{}\t{}", integrity, pkg_id);
        self.index.set(&key, files_index)?;

        Ok(())
    }

    fn walk_and_add_dir(
        &self,
        dir: &Path,
        files_index: &mut HashMap<String, FileInfo>,
        rel_path: &Path,
    ) -> Result<(), Error> {
        // This is a simplified version; in practice, need to read directory
        // For now, assume we have a list of files
        // TODO: Implement directory walking
        Ok(())
    }

    pub fn import_package(
        &self,
        pkg_id: &str,
        integrity: &str,
        dest_dir: &Path,
    ) -> Result<(), Error> {
        let key = format!("{}\t{}", integrity, pkg_id);
        let files_index = self.index.get(&key)?;

        for (rel_path, info) in files_index {
            let src = self.get_file_path(&info.digest);
            let dest = dest_dir.join(&rel_path);
            if let Some(parent) = dest.parent() {
                self.sys.fs_create_dir_all(parent)?;
            }
            self.sys.fs_hard_link(&src, &dest)?;
        }

        Ok(())
    }

    fn get_file_path(&self, digest: &str) -> PathBuf {
        // Assuming digest is hex
        let first_two = &digest[0..2];
        let rest = &digest[2..];
        self.store_dir.join("files").join(first_two).join(rest)
    }

    fn calculate_digest(&self, content: &[u8]) -> String {
        let mut hasher = Sha512::new();
        hasher.update(content);
        format!("{:x}", hasher.finalize())
    }
}