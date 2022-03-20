use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::params;

////TODO: simply make the table name match typ_str()

#[derive(Copy, Clone, PartialEq)]
pub enum CardType {
    Invalid,
    Project,
    Task,
    Status,
    Timelog,
}

impl ToString for CardType {
    fn to_string(&self) -> String {
        match self {
            CardType::Invalid => String::from("invalid"),
            CardType::Project => String::from(Project::typ_str()),
            CardType::Task => String::from(Task::typ_str()),
            CardType::Status => String::from(Status::typ_str()),
            CardType::Timelog => String::from(Timelog::typ_str()),
        }
    }
}

impl FromStr for CardType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "project" => CardType::Project,
            "task" => CardType::Task,
            "status" => CardType::Status,
            "timelog" => CardType::Timelog,
            _ => CardType::Invalid,
        })
    }
}

#[derive(std::fmt::Debug)]
pub enum Error {
    CantFindCard(String),
    CantAccessCard,
    CantReadFormatOfCard,
    CantReadProperty(String),
    DatabaseError(String),
}

pub fn get_path_to_cards() -> PathBuf {
    ////TODO: Make this configurable.
    PathBuf::from("C:/Dropbox/Data/Cards")
}

pub fn parse_qualified_id(qualified_id: &str) -> Result<(CardType, u64), Error> {
    let slash = qualified_id.find('/').ok_or(Error::DatabaseError(String::from("card link is missing /")))?;
    let typ = CardType::from_str(&qualified_id[..slash]).map_err(|err| Error::DatabaseError(String::from("invalid card type")))?;
    let id = qualified_id[(slash + 1)..].parse::<u64>().map_err(|err| Error::DatabaseError(String::from("invalid card ID")))?;

    Ok((typ, id))
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

fn get_string_list_property(json: &serde_json::Value, name: &str) -> Result<Vec<String>, Error> {
    match &json[name] {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::Array(vec) => {
            let mut r = Vec::new();
            for v in vec.iter() {
                r.push(String::from(v.as_str().ok_or(Error::CantReadProperty(String::from(name)))?))
            }
            Ok(r)
        }
        _ => Err(Error::CantReadProperty(String::from(name)))
    }
}

fn load_card_from_json<T: Card, F: FnOnce(CardData) -> Result<T, Error>>(id: u64, f: F) -> Result<T, Error> {
    let path = get_file_path_for_card(T::typ_str(), id);
    let contents = fs::read_to_string(path).map_err(|_| Error::CantAccessCard)?;
    let json: serde_json::Value = serde_json::from_str(&contents).map_err(|_| Error::CantReadFormatOfCard)?;

    let data = CardData {
        id,
        title: get_property(&json, "Title")?,
        created: get_property(&json, "Created")?,
        modified: get_property(&json, "Modified")?,
        tags: get_string_list_property(&json, "Tags")?,
        links: get_string_list_property(&json, "Links")?,
        contents: json,
    };

    f(data)
}

struct CardData {
    id: u64,
    title: String,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    tags: Vec<String>,
    links: Vec<String>,
    contents: serde_json::Value,
}

pub trait Card
    where Self: Sized {

    fn id(&self) -> u64;
    fn title(&self) -> &String;
    fn created(&self) -> DateTime<Utc>;
    fn modified(&self) -> DateTime<Utc>;
    fn tags(&self) -> std::slice::Iter<'_, String>;
    fn links(&self) -> std::slice::Iter<'_, String>;

    fn typ() -> CardType;
    fn typ_str() -> &'static str;
    fn load(id: u64) -> Result<Self, Error>;

    fn sql_schema() ->&'static str;
    fn sql_table() -> &'static str;
    fn sql_write_stmt() -> &'static str;
    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error>;

    fn qualified_id(&self) -> String {
        format!("{}/{}", Self::typ_str(), self.id())
    }

    fn path() -> PathBuf {
        let mut path = PathBuf::new();
        path.push(get_path_to_cards());
        path.push(Self::typ_str());
        path
    }

    fn list() -> Vec<u64> {

        let path = Self::path();
        let mut result = Vec::new();

        for entry in path.read_dir().expect(format!("Can read files in {}/ directory", Self::typ_str()).as_str()) {
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

    fn json(id: u64) -> Result<String, Error> {
        let path = get_file_path_for_card(Self::typ_str(), id);
        if path.exists() {
            fs::read_to_string(path).map_err(|_| Error::CantAccessCard)
        }
        else {
            Err(Error::CantAccessCard)
        }
    }

    fn sql_find_id(db: &rusqlite::Connection, name_or_id: &str) -> Result<u64, Error> {
        fn get_next_id(rows: &mut rusqlite::Rows, name_or_id: &str) -> Result<u64, Error> {
            match rows.next() {
                Err(err) => Err(Error::DatabaseError(err.to_string())),
                Ok(None) => Err(Error::CantFindCard(String::from(name_or_id))),
                Ok(Some(row)) => row.get::<usize, u64>(0).map_err(|err| Error::DatabaseError(err.to_string())),
            }
        }
        if let Ok(id) = name_or_id.parse::<u64>() {
            Ok(id)
        }
        else {
            let mut stmt = db.prepare(&format!("SELECT id FROM Projects WHERE title LIKE '%{}%'", name_or_id))
                .map_err(|err| Error::DatabaseError(err.to_string()))?;
            let result = match stmt.query([]) {
                Err(e) => Err(Error::DatabaseError(e.to_string())),
                Ok(mut rows) => {
                    let first = get_next_id(&mut rows, name_or_id);
                    let second = get_next_id(&mut rows, name_or_id);

                    match (first, second) {
                        (Ok(_), Ok(_)) => Err(Error::CantFindCard(format!("Name '{}/{}' is ambiguous", Self::typ_str(), name_or_id))),
                        (f, _) => f
                    }
                }
            };
            result
        }
    }

    fn sql_list_ids(db: &rusqlite::Connection) -> Result<Vec<u64>, Error> {
        let mut stmt = db.prepare(format!("SELECT id FROM {}", Self::sql_table()).as_str())
            .map_err(|err| Error::DatabaseError(err.to_string()))?;
        let mut rows = stmt.query([])
            .map_err(|err| Error::DatabaseError(err.to_string()))?;

        let mut ids = Vec::new();
        while let Some(row) = rows.next().map_err(|err| Error::DatabaseError(err.to_string()))? {
            ids.push(row.get::<usize, u64>(0).map_err(|err| Error::DatabaseError(err.to_string()))?)
        }

        Ok(ids)
    }

    fn sql_write_links(&self, db: &mut rusqlite::Statement) -> Result<(), Error> {
        for v in self.links() {
            let colon = v.find(':');
            let role = match colon {
                Some(index) => &v[..index],
                None => "",
            };
            let qualified_id = match colon {
                Some(index) => &v[(index + 1)..],
                None => &v[..],
            };
            let slash = qualified_id.find('/').ok_or(Error::DatabaseError(String::from("card link is missing /")))?;
            let (to_type, to_id) = parse_qualified_id(qualified_id)?;

            db.insert(params![
                role,
                Self::typ() as u32,
                self.id(),
                to_type as u32,
                to_id,
            ]).map_err(|err| Error::DatabaseError(String::from(format!("cannot insert link: {}", err.to_string()))))?;
        }
        Ok(())
    }
}

pub struct Project {
    id: u64,
    title: String,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    tags: Vec<String>,
    links: Vec<String>,
    active: bool,
    started: Option<NaiveDate>,
    finished: Option<NaiveDate>,
}

impl Card for Project {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.title }
    fn created(&self) -> DateTime<Utc> { self.created }
    fn modified(&self) -> DateTime<Utc> { self.modified }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Project }
    fn typ_str() -> &'static str { "project" }

    fn load(id: u64) -> Result<Project, Error> {
        load_card_from_json(id,
            |data| Ok(Project {
                id,
                title: data.title,
                created: data.created,
                modified: data.modified,
                tags: data.tags,
                links: data.links,
                started: get_optional_property(&data.contents, "Started")?,
                finished: get_optional_property(&data.contents, "Finished")?,
                active: get_bool_property(&data.contents, "Active")?,
            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Projects;
        DROP INDEX IF EXISTS ProjectsByName;
        CREATE TABLE Projects (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created VARCHAR NOT NULL,
            modified VARCHAR NOT NULL,
            started VARCHAR,
            finished VARCHAR,
            active BOOLEAN DEFAULT 1
        );
        CREATE INDEX ProjectsByName ON Projects(title);"#
    }

    fn sql_table() -> &'static str { "Projects" }

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
    description: String,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    tags: Vec<String>,
    links: Vec<String>,
    obsolete: bool,
    completed: Option<NaiveDate>,
}

impl Card for Task {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.description }
    fn created(&self) -> DateTime<Utc> { self.created }
    fn modified(&self) -> DateTime<Utc> { self.modified }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Task }
    fn typ_str() -> &'static str { "task" }

    fn load(id: u64) -> Result<Task, Error> {
        load_card_from_json(id,
                            |data| Ok(Task {
                                id,
                                description: data.title,
                                created: data.created,
                                modified: data.modified,
                                tags: data.tags,
                                links: data.links,
                                completed: get_optional_property(&data.contents, "Completed")?,
                                obsolete: get_bool_property(&data.contents, "Obsolete")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Tasks;
        DROP INDEX IF EXISTS TasksByDescription;
        CREATE TABLE Tasks (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created VARCHAR NOT NULL,
            modified VARCHAR NOT NULL,
            completed VARCHAR,
            obsolete BOOLEAN
        );
        CREATE INDEX TasksByDescription ON Tasks(title);"#
    }

    fn sql_table() -> &'static str { "Tasks" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Tasks (id, title, created, modified, completed, obsolete) VALUES(?1, ?2, ?3, ?4, ?5, ?6)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {

        let completed = self.completed.map_or(String::from("NULL"), |d| d.to_string());
        let created = self.created.to_rfc3339();
        let modified = self.modified.to_rfc3339();

        stmt.execute(params![
            self.id,
            self.description,
            created,
            modified,
            completed,
            self.obsolete,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Timelog {
    id: u64,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    tags: Vec<String>,
    links: Vec<String>,
    description: String,
    started: DateTime<Utc>,
    ended: Option<DateTime<Utc>>,
    category: Option<String>,
}

impl Card for Timelog {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.description }
    fn created(&self) -> DateTime<Utc> { self.created }
    fn modified(&self) -> DateTime<Utc> { self.modified }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Timelog }
    fn typ_str() -> &'static str { "timelog" }

    fn load(id: u64) -> Result<Timelog, Error> {
        load_card_from_json(id,
                            |data| Ok(Timelog {
                                id,
                                description: data.title,
                                created: data.created,
                                modified: data.modified,
                                tags: data.tags,
                                links: data.links,
                                started: get_property(&data.contents, "Started")?,
                                ended: get_optional_property(&data.contents, "Ended")?,
                                category: get_optional_property(&data.contents, "Category")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Timelogs;
        CREATE TABLE Timelogs (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created VARCHAR NOT NULL,
            modified VARCHAR NOT NULL,
            started VARCHAR NOT NULL,
            ended VARCHAR,
            category VARCHAR
        );"#
    }

    fn sql_table() -> &'static str { "Timelogs" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Timelogs (id, title, created, modified, started, ended, category) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {

        let started = self.started.to_string();
        let ended = self.ended.map_or(String::from("NULL"), |d| d.to_string());
        let created = self.created.to_rfc3339();
        let modified = self.modified.to_rfc3339();

        stmt.execute(params![
            self.id,
            self.description,
            created,
            modified,
            started,
            ended,
            self.category,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Transaction {
    id: u64,
}

pub struct Status {
    id: u64,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    tags: Vec<String>,
    links: Vec<String>,
    message: String,
    began: Option<DateTime<Utc>>,
    ended: Option<DateTime<Utc>>,
}

impl Card for Status {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.message }
    fn created(&self) -> DateTime<Utc> { self.created }
    fn modified(&self) -> DateTime<Utc> { self.modified }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Status }
    fn typ_str() -> &'static str { "status" }

    fn load(id: u64) -> Result<Status, Error> {
        load_card_from_json(id,
                            |data| Ok(Status {
                                id,
                                message: data.title,
                                created: data.created,
                                modified: data.modified,
                                tags: data.tags,
                                links: data.links,
                                began: get_optional_property(&data.contents, "Began")?,
                                ended: get_optional_property(&data.contents, "Ended")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Statuses;
        CREATE TABLE Statuses (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created VARCHAR NOT NULL,
            modified VARCHAR NOT NULL,
            began VARCHAR,
            ended VARCHAR
        );"#
    }

    fn sql_table() -> &'static str { "Statuses" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Statuses (id, title, created, modified, began, ended) VALUES(?1, ?2, ?3, ?4, ?5, ?6)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {

        let began = self.began.map_or(String::from("NULL"), |d| d.to_string());
        let ended = self.ended.map_or(String::from("NULL"), |d| d.to_string());
        let created = self.created.to_rfc3339();
        let modified = self.modified.to_rfc3339();

        stmt.execute(params![
            self.id,
            self.message,
            created,
            modified,
            began,
            ended,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Book {
    id: u64,
    title: String,
}

pub struct Account {
    id: u64,
    username: String,
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
