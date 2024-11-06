use std::str::FromStr;
use std::{collections::HashMap, path::Path};
use std::fmt::Debug;

use crate::CompressionType;

#[derive(Clone)]
pub struct S4ArchiveEntryDetails {
  pub name: String,
  pub _type: String,
  pub offset: u64,
  pub size: u64,
  pub compression: CompressionType,
}

impl S4ArchiveEntryDetails {
  pub fn parse_header<P: AsRef<Path> + Debug + ?Sized>(
    header_file: &P,
  ) -> Result<HashMap<String, Self>, String> {
    let conn = rusqlite::Connection::open(header_file)
      .map_err(|e| format!("error opening db file {header_file:?}: {e}"))?;
    let mut entry_query = conn
      .prepare("SELECT name, type, offset, size, compression FROM entry_list")
      .map_err(|e| format!("sql query mistake: {e}"))?;
    let entry_map = entry_query
      .query_map([], |row| {
        Ok(S4ArchiveEntryDetails {
          name: row.get(0)?,
          _type: row.get(1)?,
          offset: row.get(2)?,
          size: row.get(3)?,
          compression: CompressionType::from_str(&row.get::<usize, String>(4)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        })
      })
      .map_err(|e| format!("iterating through header failed: {e}"))?
      .filter_map(|x| x.inspect_err(|e| eprintln!("error parsing db entry: {e}, skipping")).ok())
      .map(|s4a_entry| (s4a_entry.name.clone(), s4a_entry))
      .collect::<HashMap<_, _>>();
    Ok(entry_map)
  }
}

pub struct HeaderDBWriter<'a> {
  insert_stmt: rusqlite::Statement<'a>,
}

impl<'a> HeaderDBWriter<'a> {
  pub fn new(conn: &'a rusqlite::Connection) -> Result<Self, String> {
    // Create table
    conn
      .execute(
        "CREATE TABLE entry_list (
          name VARCHAR(2048),
          type VARCHAR(8),
          offset BIGINT,
          size BIGINT,
          compression VARCHAR(8)
        )",
        [],
      )
      .map_err(|e| format!("at creating mysql table: {e}"))?;
    // Prepare SQL insert statement
  let insert_stmt = conn
    .prepare("INSERT INTO entry_list 
      (name, type, offset, size, compression) VALUES (?1, ?2, ?3, ?4, ?5)")
    .map_err(|e| format!("error preparing insert statement: {e}"))?;
    Ok(Self { insert_stmt })
  }

  pub fn insert_entry(&mut self, entry: S4ArchiveEntryDetails) -> Result<(), String> {
    self
      .insert_stmt
      .execute((&entry.name, entry._type, entry.offset, entry.size, entry.compression.to_string()))
      .map_err(|e| format!("error adding {} to index: {e}", &entry.name))?;
    Ok(())
  }

  pub fn flush(self) -> Result<(), String> {
    self.insert_stmt.finalize().map_err(|e| format!("error flushing data to index: {e}"))
  }
}
