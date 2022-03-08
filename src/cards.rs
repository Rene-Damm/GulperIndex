use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::params;

#[derive(std::fmt::Debug)]
pub enum Error {
    CantAccessCard,
    CantReadFormatOfCard,
    CantReadProperty(String),
    DatabaseError(String),
}

fn get_path_to_cards() -> PathBuf {
    ////TODO: Make this configurable.
    PathBuf::from("C:/Dropbox/Data/Cards")
}

fn get_file_path_for_card(typ: &str, id: u64) -> PathBuf {
    let mut path = get_path_to_cards();
    path.push(typ);
    path.push(id.to_string() + ".json");
    path
}

fn get_property<T: FromStr>(json: &serde_json::Value, name: &str) -> Result<T, Error> {
    let str = json[name].as_str().ok_or(Error::CantReadProperty(String::from(name)))?;
    let val = str.parse::<T>().map_err(|_| Error::CantReadProperty(String::from(name)))?;
    Ok(val)
}

fn get_optional_property<T: FromStr>(json: &serde_json::Value, name: &str) -> Result<Option<T>, Error> {
    if let Some(ref val) = json.get(name) {
        match val {
            serde_json::Value::Null => Ok(None),
            _ => match val.as_str() {
                Some(str) => str.parse::<T>().map_err(|_| Error::CantReadProperty(String::from(name))).map(|v| Some(v)),
                _ => Err(Error::CantReadProperty(String::from(name)))
            }
        }
    }
    else
    {
        Ok(None)
    }
}

fn get_bool_property(json: &serde_json::Value, name: &str) -> Result<bool, Error> {
    match &json[name] {
        serde_json::Value::Null => Ok(false),
        serde_json::Value::Bool(b) => Ok(false),
        serde_json::Value::String(s) => s.parse::<bool>().map_err(|err| Error::CantReadProperty(String::from(name))),
        _ => Err(Error::CantReadProperty(String::from(name)))
    }
}

pub trait Cardlike
    where Self: Sized {

    fn id(&self) -> u64;
    fn title(&self) -> &String;
    fn created(&self) -> DateTime<Utc>;
    fn modified(&self) -> DateTime<Utc>;

    fn typ() -> &'static str;
    fn load(id: u64) -> Result<Self, Error>;

    fn sql_schema() ->&'static str;
    fn sql_count_stmt() -> &'static str;
    fn sql_write_stmt() -> &'static str;
    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error>;

    fn qualified_id(&self) -> String {
        format!("{}/{}", Self::typ(), self.id())
    }

    fn path() -> PathBuf {
        let mut path = PathBuf::new();
        path.push(get_path_to_cards());
        path.push(Self::typ());
        path
    }

    fn list() -> Vec<u64> {

        let path = Project::path();
        let mut result = Vec::new();

        for entry in path.read_dir().expect("Can read files in project/ directory") {
            if let Ok(entry) = entry {
                let path = entry.path();
                if let Some(extension) = path.extension() {
                    if extension != "json" {
                        continue
                    }
                    if let Some(stem) = path.file_stem() {
                        if let Ok(id) = stem.to_str().unwrap().parse::<u64>() {
                            if let Ok(file_type) = entry.file_type() {
                                result.push(id)
                            }
                        }
                    }
                }
            }
        }

        result
    }
}

pub struct Project {
    id: u64,
    title: String,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    active: bool,
    started: Option<NaiveDate>,
    finished: Option<NaiveDate>,
}

impl Cardlike for Project {

    fn id(&self) -> u64 {
        self.id
    }

    fn title(&self) -> &String {
        &self.title
    }

    fn created(&self) -> DateTime<Utc> {
        self.created
    }

    fn modified(&self) -> DateTime<Utc> {
        self.modified
    }

    fn typ() -> &'static str {
        "project"
    }

    fn load(id: u64) -> Result<Project, Error> {
        let path = get_file_path_for_card(Project::typ(), id);
        let contents = fs::read_to_string(path).map_err(|_| Error::CantAccessCard)?;
        let json: serde_json::Value = serde_json::from_str(&contents).map_err(|_| Error::CantReadFormatOfCard)?;

        let project = Project {
            id,
            title: get_property(&json, "Title")?,
            created: get_property(&json, "Created")?,
            modified: get_property(&json, "Modified")?,
            started: get_optional_property(&json, "Started")?,
            finished: get_optional_property(&json, "Finished")?,
            active: get_bool_property(&json, "Active")?,
        };

        Ok(project)
    }

    fn sql_schema() -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS Projects (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created VARCHAR NOT NULL,
            modified VARCHAR NOT NULL,
            started VARCHAR,
            finished VARCHAR,
            active BOOLEAN DEFAULT 1
        );"#
    }

    fn sql_count_stmt() -> &'static str {
        "SELECT COUNT(id) FROM Projects"
    }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Projects (id, title, created, modified, started, finished, active) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {

        let started = self.started.map_or(String::from("NULL"), |d| d.to_string());
        let finished = self.finished.map_or(String::from("NULL"), |d| d.to_string());
        let created = self.created.to_rfc3339();
        let modified = self.modified.to_rfc3339();

        stmt.execute(params![
            self.id,
            self.title,
            created,
            modified,
            started,
            finished,
            self.active,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Task {
    id: u64,
    title: String,
}

pub struct Timelog {
    id: u64,
}

pub struct Transaction {
    id: u64,
}

pub struct Status {
    id: u64,
}

pub struct Book {
    id: u64,
    title: String,
}

pub struct Account {
    id: u64,
    username: String,
}

//is this even needed?
pub enum Card<T: Cardlike> {
    Card(T)
}

impl<T: Cardlike> Card<T> {
}

/*
#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    #[test]
    fn can_parse_dates() {
        assert_matches!(String::from("2022-03-07T23:30:00Z").parse::<DateTime>
        Some(str) => str.parse::<T>().map_err(|_| Error::CantReadProperty(String::from(name))).map(|v| Some(v)),
    }
}
*/
