// Copyright 2018-2026 the Deno authors. MIT license.

use std::collections::HashMap;
use std::io::Error;

use rusqlite::{params, Connection, Result as SqlResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    pub digest: String,
    pub size: u64,
    pub mode: u32,
}

pub type FilesIndex = HashMap<String, FileInfo>;

pub struct StoreIndex {
    conn: Connection,
}

impl StoreIndex {
    pub fn new(path: PathBuf) -> Result<Self, Error> {
        let conn = Connection::open(&path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS packages (
                key TEXT PRIMARY KEY,
                files TEXT NOT NULL
            )",
            [],
        )?;
        Ok(Self { conn })
    }

    pub fn set(&self, key: &str, files: FilesIndex) -> Result<(), Error> {
        let files_json = serde_json::to_string(&files)?;
        self.conn.execute(
            "INSERT OR REPLACE INTO packages (key, files) VALUES (?1, ?2)",
            params![key, files_json],
        )?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<FilesIndex, Error> {
        let files_json: String = self.conn.query_row(
            "SELECT files FROM packages WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )?;
        let files: FilesIndex = serde_json::from_str(&files_json)?;
        Ok(files)
    }
}