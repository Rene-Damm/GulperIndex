use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use rusqlite::{OptionalExtension, params};
use urlencoding::decode;

////TODO: simply make the table name match typ_str()
////TODO: introduce CardId type (pub struct CardId(u64))
////TODO: lowercase all table names

pub enum CardType {
    Invalid,
    Project,
    Task,
    Status,
    Timelog,
    Book,
    Purchase,
    Metric,
    Word,
    Note,
    Thought,
    Achievement,
    Notebook,
}

impl ToString for CardType {
    fn to_string(&self) -> String {
        match self {
            CardType::Invalid => String::from("invalid"),
            CardType::Project => String::from(Project::typ_str()),
            CardType::Task => String::from(Task::typ_str()),
            CardType::Status => String::from(Status::typ_str()),
            CardType::Timelog => String::from(Timelog::typ_str()),
            CardType::Book => String::from(Book::typ_str()),
            CardType::Purchase => String::from(Purchase::typ_str()),
            CardType::Metric => String::from(Metric::typ_str()),
            CardType::Word => String::from(Word::typ_str()),
            CardType::Note => String::from(Note::typ_str()),
            CardType::Thought => String::from(Thought::typ_str()),
            CardType::Achievement => String::from(Achievement::typ_str()),
            CardType::Notebook => String::from(Notebook::typ_str()),
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
            "book" => CardType::Book,
            "purchase" => CardType::Purchase,
            "metric" => CardType::Metric,
            "word" => CardType::Word,
            "note" => CardType::Note,
            "thought" => CardType::Thought,
            "achievement" => CardType::Achievement,
            "notebook" => CardType::Notebook,
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
    let typ = CardType::from_str(&qualified_id[..slash]).map_err(|_| Error::DatabaseError(String::from("invalid card type")))?;
    let id = qualified_id[(slash + 1)..].parse::<u64>().map_err(|_| Error::DatabaseError(String::from("invalid card ID")))?;

    Ok((typ, id))
}

fn get_file_path_for_card(typ: &str, id: u64) -> PathBuf {
    let mut path = get_path_to_cards();
    path.push(typ);
    path.push(id.to_string() + ".json");
    path
}

fn get_property<T: FromStr + Default>(json: &serde_json::Value, name: &str) -> Result<T, Error> {
    ////FIXME: this is horrible code...
    let str = match json.get(name) {
        Some(serde_json::Value::Null) => String::from("null"),
        Some(serde_json::Value::Bool(b)) => if *b { String::from("true") } else { String::from("false") },
        Some(serde_json::Value::String(s)) => s.to_string(),
        Some(serde_json::Value::Number(n)) =>
            if n.is_f64() { n.as_f64().unwrap().to_string() }
            else if n.is_i64() { n.as_i64().unwrap().to_string() }
            else { n.as_u64().unwrap().to_string() }
        ////TODO: should raise Error::CantReadProperty
        _ => String::new()
    };
    if str.is_empty() {
        Ok(T::default())
    }
    else {
        let val = str.parse::<T>().map_err(|_| Error::CantReadProperty(String::from(name)))?;
        Ok(val)
    }
}

fn get_optional_property<T: FromStr>(json: &serde_json::Value, name: &str) -> Result<Option<T>, Error> {
    if let Some(ref val) = json.get(name) {
        match val {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::String(str) => str.parse::<T>().map_err(|_| Error::CantReadProperty(String::from(name))).map(|v| Some(v)),
            ////REVIEW: There's probably better ways to turn a serde::Value into a string but haven't found one so far (as_str only works for actual strings).
            _ => format!("{}", val).parse::<T>().map_err(|_| Error::CantReadProperty(String::from(name))).map(|v| Some(v)),
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
        serde_json::Value::Bool(b) => Ok(*b),
        serde_json::Value::String(s) => s.parse::<bool>().map_err(|_| Error::CantReadProperty(String::from(name))),
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
        source: get_optional_property(&json, "Source")?,
        tags: get_string_list_property(&json, "Tags")?,
        links: get_string_list_property(&json, "Links")?,
        contents: json,
    };

    f(data)
}

struct CardData {
    id: u64,
    title: String,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    contents: serde_json::Value,
}

pub trait Card
    where Self: Sized {

    fn id(&self) -> u64;
    fn title(&self) -> &String;
    fn created(&self) -> &String;
    fn modified(&self) -> &String;
    fn source(&self) -> &Option<String>;
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
                            if let Ok(_) = entry.file_type() {
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
            let mut stmt = db.prepare(&format!("SELECT id FROM {} WHERE title LIKE '%{}%'", Self::sql_table(), name_or_id))
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

    fn sql_list_ids(db: &rusqlite::Connection, query: &HashMap<String, String>) -> Result<Vec<u64>, Error> {

        let mut tags = Vec::new();
        let mut ids = Vec::new();

        let mut stmt_str = format!("SELECT id FROM {}", Self::sql_table());
        let mut have_where_clause = false;
        if !query.is_empty() {
            for (key, value) in query.iter() {
                if key == "tag" {
                    tags.push(value)
                }
                else if key == "_where" {
                    stmt_str = format!("{} {} {}", stmt_str, if have_where_clause { "AND" } else { "WHERE" }, decode(value).unwrap());
                    have_where_clause = true;
                }
                else {
                    ////REVIEW: stringify automatically?
                    stmt_str = format!("{} {} {} IS {}", stmt_str, if have_where_clause { "AND" } else { "WHERE" }, key, decode(value).unwrap());
                    have_where_clause = true;
                }
            }
        }

        // Look up all tag IDs.
        let mut tag_ids = Vec::new();
        if !tags.is_empty() {
            let mut tag_lookup = db.prepare("SELECT rowid FROM Tags WHERE name LIKE ?1")
                .map_err(|err| Error::DatabaseError(err.to_string()))?;
            for tag in tags.iter() {
                let id = tag_lookup.query_row(params![*tag],
                    |row| row.get::<usize, u64>(0))
                    .optional()
                    .map_err(|err| Error::DatabaseError(err.to_string()))?;

                match id {
                    Some(i) => tag_ids.push(i),
                    _ => ()
                }
            }

            // If we have tag constraints but none of the tags resulted in any hit,
            // our result set is empty so early out.
            if tag_ids.is_empty() {
                return Ok(ids)
            }
        }

        // If we are searching by tags, append lookup for Taggings table.
        if !tag_ids.is_empty() {
            for tag_id in tag_ids.iter() {
                stmt_str = format!("{} {} id IN (SELECT card_id FROM Taggings WHERE tag_id IS {} AND card_type IS {})",
                                   stmt_str, if have_where_clause { "AND" } else { "WHERE" },
                    tag_id, Self::typ() as u32
                );
                have_where_clause = true;
            }
        }

        let mut stmt = db.prepare(stmt_str.as_str())
            .map_err(|err| Error::DatabaseError(err.to_string()))?;
        let mut rows = stmt.query([])
            .map_err(|err| Error::DatabaseError(err.to_string()))?;

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

    fn sql_write_tags(&self, tag_insert: &mut rusqlite::Statement, tag_lookup: &mut rusqlite::Statement, tagging_insert: &mut rusqlite::Statement) -> Result<(), Error> {
        for tag in self.tags() {
            tag_insert.execute(params![tag])
                .map_err(|err| Error::DatabaseError(String::from(format!("cannot insert tag: {}", err.to_string()))))?;
            let tag_id = tag_lookup.query_row(params![tag],
                |row| row.get::<usize, usize>(0))
                .map_err(|err| Error::DatabaseError(String::from(format!("cannot query tag: {}", err.to_string()))))?;

            tagging_insert.insert(params![
                tag_id,
                Self::typ() as u32,
                self.id()
            ]).map_err(|err| Error::DatabaseError(String::from(format!("cannot insert tagging: {}", err.to_string()))))?;
        }
        Ok(())
    }
}

pub struct Project {
    id: u64,
    title: String,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    active: bool,
    started: Option<String>,
    finished: Option<String>,
}

impl Card for Project {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.title }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
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
                source: data.source,
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
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            started DATETIME,
            finished DATETIME,
            active BOOLEAN DEFAULT 1
        );
        CREATE INDEX ProjectsByName ON Projects(title);"#
    }

    fn sql_table() -> &'static str { "Projects" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Projects (id, title, created, modified, source, started, finished, active) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.title,
            self.created,
            self.modified,
            self.source,
            self.started,
            self.finished,
            self.active,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Task {
    id: u64,
    description: String,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    obsolete: bool,
    completed: Option<String>,
}

impl Card for Task {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.description }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
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
                                source: data.source,
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
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            completed DATETIME,
            obsolete BOOLEAN
        );
        CREATE INDEX TasksByDescription ON Tasks(title);"#
    }

    fn sql_table() -> &'static str { "Tasks" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Tasks (id, title, created, modified, source, completed, obsolete) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.description,
            self.created,
            self.modified,
            self.source,
            self.completed,
            self.obsolete,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Timelog {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    description: String,
    started: String,
    ended: Option<String>,
    category: Option<String>,
}

impl Card for Timelog {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.description }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
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
                                source: data.source,
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
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            started DATETIME NOT NULL,
            ended DATETIME,
            category VARCHAR
        );"#
    }

    fn sql_table() -> &'static str { "Timelogs" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Timelogs (id, title, created, modified, source, started, ended, category) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.description,
            self.created,
            self.modified,
            self.source,
            self.started,
            self.ended,
            self.category,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Status {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    message: String,
    began: Option<String>,
    ended: Option<String>,
}

impl Card for Status {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.message }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
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
                                source: data.source,
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
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            began DATETIME,
            ended DATETIME
        );"#
    }

    fn sql_table() -> &'static str { "Statuses" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Statuses (id, title, created, modified, source, began, ended) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.message,
            self.created,
            self.modified,
            self.source,
            self.began,
            self.ended,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Book {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    title: String,
    authors: String,
    year: i32,
    started: Option<String>,
    completed: Option<String>,
    cover: Option<String>,
    ident_code: Option<String>,
}

impl Card for Book {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.title }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Book }
    fn typ_str() -> &'static str { "book" }

    fn load(id: u64) -> Result<Book, Error> {
        load_card_from_json(id,
                            |data| Ok(Book {
                                id,
                                title: data.title,
                                created: data.created,
                                modified: data.modified,
                                source: data.source,
                                tags: data.tags,
                                links: data.links,
                                authors: get_property(&data.contents, "Authors")?,
                                year: get_property(&data.contents, "Year")?,
                                started: get_optional_property(&data.contents, "Started")?,
                                completed: get_optional_property(&data.contents, "Completed")?,
                                cover: get_optional_property(&data.contents, "Cover")?,
                                ident_code: get_optional_property(&data.contents, "IdentCode")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Books;
        CREATE TABLE Books (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            authors VARCHAR NOT NULL,
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            year INTEGER,
            started DATETIME,
            completed DATETIME,
            cover VARCHAR,
            ident VARCHAR
        );"#
    }

    fn sql_table() -> &'static str { "Books" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Books (id, title, authors, created, modified, source, year, started, completed, cover, ident) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.title,
            self.authors,
            self.created,
            self.modified,
            self.source,
            self.year,
            self.started,
            self.completed,
            self.cover,
            self.ident_code
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Purchase {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    item: String,
    date: String,
    price: i32,
    currency: String,
    used: bool,
    store: String,
}

impl Card for Purchase {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.item }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Purchase }
    fn typ_str() -> &'static str { "purchase" }

    fn load(id: u64) -> Result<Purchase, Error> {
        load_card_from_json(id,
                            |data| Ok(Purchase {
                                id,
                                item: data.title,
                                created: data.created,
                                modified: data.modified,
                                source: data.source,
                                tags: data.tags,
                                links: data.links,
                                date: get_property(&data.contents, "Date")?,
                                price: get_property(&data.contents, "Price")?,
                                currency: get_property(&data.contents, "Currency")?,
                                store: get_property(&data.contents, "Store")?,
                                used: get_property(&data.contents, "Used")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Purchases;
        CREATE TABLE Purchases (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            date DATETIME,
            price REAL,
            currency CHAR(3),
            used BOOLEAN,
            store VARCHAR
        );"#
    }

    fn sql_table() -> &'static str { "Purchases" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Purchases (id, title, created, modified, source, date, price, currency, used, store) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.item,
            self.created,
            self.modified,
            self.source,
            self.date,
            self.price,
            self.currency,
            self.used,
            self.store,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Metric {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    name: String,
    amount: f32,
    timestamp: String,
}

impl Card for Metric {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.name }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Metric }
    fn typ_str() -> &'static str { "metric" }

    fn load(id: u64) -> Result<Metric, Error> {
        load_card_from_json(id,
                            |data| Ok(Metric {
                                id,
                                name: data.title,
                                created: data.created,
                                modified: data.modified,
                                source: data.source,
                                tags: data.tags,
                                links: data.links,
                                amount: get_property(&data.contents, "Amount")?,
                                timestamp: get_property(&data.contents, "Timestamp")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Metrics;
        CREATE TABLE Metrics (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            timestamp DATETIME,
            amount REAL
        );"#
    }

    fn sql_table() -> &'static str { "Metrics" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Metrics (id, title, created, modified, source, timestamp, amount) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.name,
            self.created,
            self.modified,
            self.source,
            self.timestamp,
            self.amount,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Word {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    word: String,
    language: String,
    category: String,
    gender: Option<String>,
}

impl Card for Word {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.word }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Word }
    fn typ_str() -> &'static str { "word" }

    fn load(id: u64) -> Result<Word, Error> {
        load_card_from_json(id,
                            |data| Ok(Word {
                                id,
                                word: data.title,
                                created: data.created,
                                modified: data.modified,
                                source: data.source,
                                tags: data.tags,
                                links: data.links,
                                language: get_property(&data.contents, "Language")?,
                                category: get_property(&data.contents, "Category")?,
                                gender: get_optional_property(&data.contents, "Gender")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Words;
        CREATE TABLE Words (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            language CHAR(3),
            category VARCHAR,
            gender CHAR(1)
        );"#
    }

    fn sql_table() -> &'static str { "Words" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Words (id, title, created, modified, source, language, category, gender) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.word,
            self.created,
            self.modified,
            self.source,
            self.language,
            self.category,
            self.gender,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Note {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    title: String,
    text: String,
}

impl Card for Note {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.title }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Note }
    fn typ_str() -> &'static str { "note" }

    fn load(id: u64) -> Result<Note, Error> {
        load_card_from_json(id,
                            |data| Ok(Note {
                                id,
                                title: data.title,
                                created: data.created,
                                modified: data.modified,
                                source: data.source,
                                tags: data.tags,
                                links: data.links,
                                text: get_property(&data.contents, "Text")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Notes;
        CREATE TABLE Notes (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            text VARCHAR
        );"#
    }

    fn sql_table() -> &'static str { "Notes" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Notes (id, title, created, modified, source, text) VALUES(?1, ?2, ?3, ?4, ?5, ?6)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.title,
            self.created,
            self.modified,
            self.source,
            self.text,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Thought {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    description: String,
}

impl Card for Thought {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.description }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Thought }
    fn typ_str() -> &'static str { "thought" }

    fn load(id: u64) -> Result<Thought, Error> {
        load_card_from_json(id,
                            |data| Ok(Thought {
                                id,
                                description: data.title,
                                created: data.created,
                                modified: data.modified,
                                source: data.source,
                                tags: data.tags,
                                links: data.links,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Thoughts;
        CREATE TABLE Thoughts (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR
        );"#
    }

    fn sql_table() -> &'static str { "Thoughts" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Thoughts (id, title, created, modified, source) VALUES(?1, ?2, ?3, ?4, ?5)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.description,
            self.created,
            self.modified,
            self.source,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Achievement {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    description: String,
    date: Option<String>,
}

impl Card for Achievement {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.description }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Achievement }
    fn typ_str() -> &'static str { "achievement" }

    fn load(id: u64) -> Result<Achievement, Error> {
        load_card_from_json(id,
                            |data| Ok(Achievement {
                                id,
                                description: data.title,
                                created: data.created,
                                modified: data.modified,
                                source: data.source,
                                tags: data.tags,
                                links: data.links,
                                date: get_optional_property(&data.contents, "Date")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Achievements;
        CREATE TABLE Achievements (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            date VARCHAR
        );"#
    }

    fn sql_table() -> &'static str { "Achievements" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Achievements (id, title, created, modified, source, date) VALUES(?1, ?2, ?3, ?4, ?5, ?6)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.description,
            self.created,
            self.modified,
            self.source,
            self.date,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}

pub struct Notebook {
    id: u64,
    created: String,
    modified: String,
    source: Option<String>,
    tags: Vec<String>,
    links: Vec<String>,
    title: String,
    description: String,
    location: String,
    format: String,
    pages: Option<u32>,
    started: Option<String>,
    ended: Option<String>,
}

impl Card for Notebook {

    fn id(&self) -> u64 { self.id }
    fn title(&self) -> &String { &self.description }
    fn created(&self) -> &String { &self.created }
    fn modified(&self) -> &String { &self.modified }
    fn source(&self) -> &Option<String> { &self.source }
    fn tags(&self) -> std::slice::Iter<'_, String> { self.tags.iter() }
    fn links(&self) -> std::slice::Iter<'_, String> { self.links.iter() }
    fn typ() -> CardType { CardType::Notebook }
    fn typ_str() -> &'static str { "notebook" }

    fn load(id: u64) -> Result<Notebook, Error> {
        load_card_from_json(id,
                            |data| Ok(Notebook {
                                id,
                                title: data.title,
                                created: data.created,
                                modified: data.modified,
                                source: data.source,
                                tags: data.tags,
                                links: data.links,
                                description: get_property(&data.contents, "Description")?,
                                location: get_property(&data.contents, "Location")?,
                                format: get_property(&data.contents, "Format")?,
                                pages: get_optional_property(&data.contents, "Pages")?,
                                started: get_optional_property(&data.contents, "Started")?,
                                ended: get_optional_property(&data.contents, "Ended")?,
                            }))
    }

    fn sql_schema() -> &'static str {
        r#"
        DROP TABLE IF EXISTS Notebooks;
        CREATE TABLE Notebooks (
            id INTEGER PRIMARY KEY,
            title VARCHAR NOT NULL,
            created DATETIME NOT NULL,
            modified DATETIME NOT NULL,
            source VARCHAR,
            description VARCHAR,
            location VARCHAR,
            format VARCHAR,
            pages INTEGER,
            started VARCHAR,
            ended VARCHAR
        );"#
    }

    fn sql_table() -> &'static str { "Notebooks" }

    fn sql_write_stmt() -> &'static str {
        "INSERT OR REPLACE INTO Notebooks (id, title, created, modified, source, description, location, format, pages, started, ended) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
    }

    fn sql_write(&self, stmt: &mut rusqlite::Statement) -> Result<usize, Error> {
        stmt.execute(params![
            self.id,
            self.title,
            self.created,
            self.modified,
            self.source,
            self.description,
            self.location,
            self.format,
            self.pages,
            self.started,
            self.ended,
        ]).map_err(|err| Error::DatabaseError(err.to_string()))
    }
}
