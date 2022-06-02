use std::ops::Deref;
use std::path::{PathBuf};
use std::sync::mpsc;
use notify::{RecursiveMode, Watcher};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use warp::Filter;
use crate::cards::{Card, Project, Task, Status, Timelog, Book, Purchase, Metric, Word, Achievement, Note, Thought, Notebook};

// API:
// GET /<type>                  u64 list of cards of the given type
// GET /<type>?prop=val...      u64 list of cards of the given type that have the given property values
// GET /<type>?_where=q         u64 list of cards of the given type that match the given SQL query
// GET /<type>/count            u64 count of the number of cards of the given type
// GET /<type>/<id>             JSON object containing the contents of the given card
// GET /<type>/<str>            Same as by ID but tries to look up a card by the given fragment of its title
//
// Choices:
// - Exposing SQL directly; gives access to full-fledged query language at the expense of tying us to implementation details
//
// Missing
// - Looking up card(s) by tag
// - Looking up card(s) by links
// - Looking up cards of mixed types
//
// Explore
// - Using an external SQL DB on the system for storage such that the DB is visible/accessible
//   to everyone without going through gulper_index

////TODO: store cards.sqlite in a place where other tools can access it

mod cards;

mod report {
    use std::process::{Command, ExitStatus};
    use std::sync::mpsc;
    use std::{io, thread};
    use std::io::Write;
    use chrono::Utc;

    const R_BIN_PATH: &str = "M:/R/4.1.3/bin/x64";
    const RSTUDIO_BIN_PATH: &str = "C:/Program Files/RStudio/bin/quarto/bin";
    const DAILY_REPORT_RMD_PATH: &str = "C:/Dropbox/Data/R/DailyReport.Rmd";
    const DAILY_REPORT_HTML_PATH: &str = "C:/Dropbox/Data/R/DailyReport.html";
    const DAILY_REPORT_FOLDER_PATH: &str = "C:/Dropbox/Data/Reports/Daily";

    #[derive(PartialEq)]
    pub enum ReportThreadCommand {
        Quit,
        Refresh,
    }

    pub struct ReportThread {
        handle: thread::JoinHandle<()>,
        pub channel: mpsc::Sender<ReportThreadCommand>,
    }

    //Rscript -e "library(rmarkdown); rmarkdown::render('C:/Dropbox/Data/R/DailyReport.Rmd', 'html_document')"

    pub fn spawn_thread() -> ReportThread {

        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            loop {
                let command = rx.recv().unwrap();
                if command == ReportThreadCommand::Quit {
                    break
                }
                else if command == ReportThreadCommand::Refresh {
                    ////TODO: consume all pending Refresh commands before running a refresh
                    let output = Command::new(format!("{}/Rscript.exe", R_BIN_PATH))
                        .arg("-e")
                        .arg(format!("library(rmarkdown); rmarkdown::render('{}', 'html_document')", DAILY_REPORT_RMD_PATH))
                        .env("PATH", RSTUDIO_BIN_PATH)
                        .output()
                        .expect("Failed to run RScript.exe");

                    println!("Updated DailyReport.");
                    if !output.status.success() {
                        println!("   Failed!")
                    } else {
                        // Copy the last DailyReport to the Reports/Daily/ folder under
                        // the current date.
                        let now = Utc::now();
                        let dmy = now.format("%Y-%m-%d");
                        std::fs::copy(DAILY_REPORT_HTML_PATH, format!("{}/{}.html", DAILY_REPORT_FOLDER_PATH, dmy));
                    }
                }
            }
        });

        ReportThread {
            handle,
            channel: tx,
        }
    }

    pub fn quit_thread(thread: ReportThread) {
        thread.channel.send(ReportThreadCommand::Quit);
        thread.handle.join();
    }

    pub fn update_report(channel: &mpsc::Sender<ReportThreadCommand>) {
        channel.send(ReportThreadCommand::Refresh);
    }
}

fn remove_card_from_db<T: Card>(id: u64, db: &rusqlite::Connection, include_incoming_links: bool) -> Result<(), cards::Error> {

    let mut del_card_stmt = db.prepare(&format!("DELETE FROM {} WHERE id IS {}", T::sql_table(), id))
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    let mut del_links_stmt = if include_incoming_links {
        db.prepare(&format!("DELETE FROM Links WHERE (from_type IS {} AND from_id IS {}) OR (to_type IS {} AND to_id IS {})",
                            T::typ() as u32, id,
                            T::typ() as u32, id))
    } else {
        db.prepare(&format!("DELETE FROM Links WHERE from_type IS {} AND from_id IS {}",
                            T::typ() as u32, id))
    }
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    let mut del_tags_stmt = db.prepare(&format!("DELETE FROM Taggings WHERE card_type IS {} AND card_id IS {}", T::typ() as u32, id))
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    del_card_stmt.execute([])
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;
    del_links_stmt.execute([])
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;
    del_tags_stmt.execute([])
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    Ok(())
}

fn prepare_card_write_stmts<T: Card>(db: &rusqlite::Connection)
    -> Result<(rusqlite::Statement, rusqlite::Statement, rusqlite::Statement, rusqlite::Statement, rusqlite::Statement), rusqlite::Error> {
    Ok((db.prepare(T::sql_write_stmt())?,
        db.prepare("INSERT INTO Links (role, from_type, from_id, to_type, to_id) VALUES(?1, ?2, ?3, ?4, ?5)")?,
        db.prepare("INSERT OR IGNORE INTO Tags VALUES(?1)")?,
        db.prepare("SELECT rowid FROM Tags WHERE name IS ?1")?,
        db.prepare("INSERT INTO Taggings (tag_id, card_type, card_id) VALUES(?1, ?2, ?3)")?))
}

fn load_card_into_db<T: Card>(id: u64, db: &rusqlite::Connection) -> Result<(), cards::Error> {

    let (mut sql, mut link, mut tag_insert, mut tag_lookup, mut tagging_insert) = prepare_card_write_stmts::<T>(db)
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    let card = T::load(id)?;
    card.sql_write(&mut sql)?;
    card.sql_write_links(&mut link)?;
    card.sql_write_tags(&mut tag_insert, &mut tag_lookup, &mut tagging_insert)?;

    Ok(())
}

fn load_all_cards_into_db<T: Card>(db: &rusqlite::Connection) -> Result<(), cards::Error> {

    let (mut sql, mut link, mut tag_insert, mut tag_lookup, mut tagging_insert) = prepare_card_write_stmts::<T>(db)
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    db.execute("BEGIN TRANSACTION", [])
        .expect("Cannot begin transaction");

    for id in T::list() {
        let card = T::load(id)?;
        card.sql_write(&mut sql)?;
        card.sql_write_links(&mut link)?;
        card.sql_write_tags(&mut tag_insert, &mut tag_lookup, &mut tagging_insert)?;
    }

    db.execute("COMMIT", [])
        .expect("Cannot commit transaction");

    Ok(())
}

fn populate_db_from_scratch(db: &rusqlite::Connection) -> Result<(), cards::Error> {
    load_all_cards_into_db::<Project>(db)?;
    load_all_cards_into_db::<Task>(db)?;
    load_all_cards_into_db::<Status>(db)?;
    load_all_cards_into_db::<Timelog>(db)?;
    load_all_cards_into_db::<Purchase>(db)?;
    load_all_cards_into_db::<Metric>(db)?;
    load_all_cards_into_db::<Word>(db)?;
    load_all_cards_into_db::<Note>(db)?;
    load_all_cards_into_db::<Thought>(db)?;
    load_all_cards_into_db::<Achievement>(db)?;
    load_all_cards_into_db::<Notebook>(db)?;
    load_all_cards_into_db::<Book>(db)
}

fn init_db(db: &rusqlite::Connection) -> Result<(), cards::Error> {

    // For now, rebuild from scratch every time.
    let stmt = format!(r#"
        BEGIN;
        DROP TABLE IF EXISTS Tags;
        DROP TABLE IF EXISTS Taggings;
        DROP TABLE IF EXISTS Links;
        CREATE TABLE IF NOT EXISTS Tags (
            name VARCHAR PRIMARY KEY UNIQUE
        );
        CREATE TABLE IF NOT EXISTS Taggings (
            tag_id INTEGER,
            card_type INTEGER,
            card_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS Links (
            role VARCHAR,
            from_type INTEGER,
            from_id INTEGER,
            to_type INTEGER,
            to_id INTEGER
        );
        {}
        {}
        {}
        {}
        {}
        {}
        {}
        {}
        {}
        {}
        {}
        {}
        COMMIT;"#,
                       Project::sql_schema(),
                       Task::sql_schema(),
                       Status::sql_schema(),
                       Timelog::sql_schema(),
                       Purchase::sql_schema(),
                       Metric::sql_schema(),
                       Word::sql_schema(),
                       Note::sql_schema(),
                       Thought::sql_schema(),
                       Achievement::sql_schema(),
                       Notebook::sql_schema(),
                       Book::sql_schema());

    db.execute_batch(&stmt,)
        .map_err(|err| { cards::Error::DatabaseError(err.to_string())})?;

    populate_db_from_scratch(db)
}

struct FileWatcher(notify::RecommendedWatcher);

fn init_watcher<T: Card>(db: Pool<SqliteConnectionManager>, report_thread: mpsc::Sender<report::ReportThreadCommand>) -> FileWatcher {

    let mut watcher = notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {

        fn is_json_file(path: &PathBuf) -> bool {
            if let Some(ext) = path.extension() {
                ext == "json"
            }
            else {
                false
            }
        }

        fn add_card<T: Card>(name: String, db: Pool<SqliteConnectionManager>, report_thread: mpsc::Sender<report::ReportThreadCommand>) {
            if let Ok(id) = name.parse::<u64>() {
                let db = db.get()
                    .expect("Cannot get DB connection");
                db.execute("BEGIN TRANSACTION", [])
                    .expect("Cannot begin transaction");
                match load_card_into_db::<T>(id, &db) {
                    Err(e) => println!("Cannot write card '{}/{}': {:?}", T::typ_str(), name, e),
                    _ => (),
                }
                db.execute("COMMIT", [])
                    .expect("Cannot commit transaction");
                report::update_report(&report_thread);
            }
        }

        fn remove_card<T: Card>(name: String, db: Pool<SqliteConnectionManager>, report_thread: mpsc::Sender<report::ReportThreadCommand>, include_incoming_links: bool) {
            if let Ok(id) = name.parse::<u64>() {
                let db = db.get()
                    .expect("Cannot get DB connection");
                db.execute("BEGIN TRANSACTION", [])
                    .expect("Cannot begin transaction");
                match remove_card_from_db::<T>(id, &db, include_incoming_links) {
                    Err(e) => println!("Cannot remove card '{}/{}': {:?}", T::typ_str(), name, e),
                    _ => (),
                }
                db.execute("COMMIT", [])
                    .expect("Cannot commit transaction");
                report::update_report(&report_thread);
            }
        }

        fn update_card<T: Card>(name: String, db: Pool<SqliteConnectionManager>, report_thread: mpsc::Sender<report::ReportThreadCommand>) {
            if let Ok(id) = name.parse::<u64>() {
                let db = db.get()
                    .expect("Cannot get DB connection");
                db.execute("BEGIN TRANSACTION", [])
                    .expect("Cannot begin transaction");
                match remove_card_from_db::<T>(id, &db, false) {
                    Err(e) => println!("Cannot remove card '{}/{}': {:?}", T::typ_str(), name, e),
                    _ => (),
                }
                match load_card_into_db::<T>(id, &db) {
                    Err(e) => println!("Cannot write card '{}/{}': {:?}", T::typ_str(), name, e),
                    _ => (),
                }
                db.execute("COMMIT", [])
                    .expect("Cannot commit transaction");
                report::update_report(&report_thread);
            }
        }

        match res {
            Ok(event) => {
                match event.kind {
                    notify::EventKind::Create(_) => {
                        for path in event.paths.iter().filter(|p| is_json_file(p)) {
                            println!("Added card {}", path.to_str().unwrap());
                            if let Some(name) = path.file_stem() {
                                add_card::<T>(String::from(name.to_str().unwrap()), db.clone(), report_thread.clone());
                            };
                        }
                    },
                    notify::EventKind::Remove(_) => {
                        for path in event.paths.iter().filter(|p| is_json_file(p)) {
                            println!("Removed card {}", path.to_str().unwrap());
                            if let Some(name) = path.file_stem() {
                                ////FIXME: *If* the file comes *back* we have destroyed all incoming links and they are gone.
                                ////       (should we leave incoming links in the DB?)
                                remove_card::<T>(String::from(name.to_str().unwrap()), db.clone(), report_thread.clone(), true);
                            };
                        }
                    },
                    notify::EventKind::Modify(_) => {
                        ////FIXME: Getting two notifications for every single change.
                        for path in event.paths.iter().filter(|p| is_json_file(p)) {
                            println!("Modified card {}", path.to_str().unwrap());
                            if let Some(name) = path.file_stem() {
                                update_card::<T>(String::from(name.to_str().unwrap()), db.clone(), report_thread.clone());
                            };
                        }
                    },
                    _ => {}, // Ignore
                }
            }
            Err(e) => println!("FSWatcher error happened: {}", e.to_string()),
        }
    })
        .expect("Cannot create file system watcher");

    watcher.watch(T::path().as_path(), RecursiveMode::NonRecursive)
        .expect("Cannot watch card directory");

    FileWatcher(watcher)
}

mod filters {
    use std::collections::HashMap;
    use r2d2::Pool;
    use r2d2_sqlite::SqliteConnectionManager;
    use super::handlers;
    use warp::Filter;
    use crate::Card;

    pub fn cards<T: Card>(db: Pool<SqliteConnectionManager>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        count::<T>(db.clone())
            .or(list::<T>(db.clone()))
            .or(get::<T>(db.clone()))
    }

    pub fn count<T: Card>(db: Pool<SqliteConnectionManager>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path(T::typ_str())
            .and(warp::path("count"))
            .and(warp::path::end())
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::count::<T>)
    }

    pub fn list<T: Card>(db: Pool<SqliteConnectionManager>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path(T::typ_str())
            .and(warp::path::end())
            .and(warp::get())
            .and(warp::query::<HashMap<String, String>>())
            .and(with_db(db))
            .and_then(handlers::list::<T>)
    }

    pub fn get<T: Card>(db: Pool<SqliteConnectionManager>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path(T::typ_str())
            .and(warp::path::param())
            .and(warp::path::end())
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get::<T>)
    }

    fn with_db(db: Pool<SqliteConnectionManager>) -> impl Filter<Extract = (Pool<SqliteConnectionManager>,), Error = std::convert::Infallible> + Clone {
        warp::any().map(move || db.clone())
    }
}

mod handlers {
    use std::collections::HashMap;
    use std::convert::Infallible;
    use r2d2::Pool;
    use r2d2_sqlite::SqliteConnectionManager;
    use warp::http::{HeaderValue, StatusCode};
    use warp::http::header::CONTENT_TYPE;
    use warp::Reply;
    use warp::reply::Response;
    use crate::{Card, cards};

    pub async fn count<T: Card>(db: Pool<SqliteConnectionManager>) -> Result<impl warp::Reply, Infallible> {

        let db = db.get()
            .expect("Cannot get DB connection from pool");

        let count = db.query_row(format!("SELECT COUNT(*) FROM {}", T::sql_table()).as_str(), [],
                     |row| row.get::<usize, usize>(0))
            .expect("Cannot query project count");

        Ok(warp::reply::json(&count))
    }

    pub async fn list<T: Card>(query: HashMap<String, String>, db: Pool<SqliteConnectionManager>) -> Result<impl warp::Reply, Infallible> {

        let db = db.get()
            .expect("Cannot get DB connection from pool");

        let ids = T::sql_list_ids(&db, &query)
            .expect("Cannot list card IDs");

        Ok(warp::reply::json(&ids))
    }

    struct Json {
        inner: Result<Vec<u8>, ()>,
    }

    impl Reply for Json {
        #[inline]
        fn into_response(self) -> Response {
            match self.inner {
                Ok(body) => {
                    let mut res = Response::new(body.into());
                    res.headers_mut()
                        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                    res
                }
                Err(()) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
    }

    pub async fn get<T: Card>(name_or_id: String, db: Pool<SqliteConnectionManager>) -> Result<impl warp::Reply, Infallible> {

        let db = db.get()
            .expect("Cannot get DB connection from pool");

        let (s, code) = match T::sql_find_id(&db, &name_or_id) {
            Ok(id) => {
                match T::json(id) {
                    Ok(s) => (s, StatusCode::OK),
                    Err(e) => (format!("Could not load {}: {:?}", name_or_id, e), StatusCode::INTERNAL_SERVER_ERROR),
                }
            },
            Err(cards::Error::CantFindCard(e)) => (format!("Cannot find card: {}", e), StatusCode::NOT_FOUND),
            Err(e) => (format!("Error: {:?}", e), StatusCode::INTERNAL_SERVER_ERROR),
        };

        // T::json gives us a string that is already serialized JSON data.
        Ok(warp::reply::with_status(Json { inner: Ok(s.into_bytes()) }, code))
    }
}

#[tokio::main]
async fn main() {

    println!("Initializing database...");
    let manager = SqliteConnectionManager::file("cards.sqlite");
    let pool = r2d2::Pool::new(manager)
        .expect("Cannot create DB connection pool");

    init_db(pool.clone().get().expect("Cannot get DB connection").deref())
        .expect("Cannot initialize DB");

    let report_thread = report::spawn_thread();
    report::update_report(&report_thread.channel);

    let _project_watcher = init_watcher::<Project>(pool.clone(), report_thread.channel.clone());
    let _task_watcher = init_watcher::<Task>(pool.clone(), report_thread.channel.clone());
    let _status_watcher = init_watcher::<Status>(pool.clone(), report_thread.channel.clone());
    let _timelog_watcher = init_watcher::<Timelog>(pool.clone(), report_thread.channel.clone());
    let _purchase_watcher = init_watcher::<Purchase>(pool.clone(), report_thread.channel.clone());
    let _book_watcher = init_watcher::<Book>(pool.clone(), report_thread.channel.clone());
    let _metric_watcher = init_watcher::<Metric>(pool.clone(), report_thread.channel.clone());
    let _word_watcher = init_watcher::<Word>(pool.clone(), report_thread.channel.clone());
    let _note_watcher = init_watcher::<Note>(pool.clone(), report_thread.channel.clone());
    let _thought_watcher = init_watcher::<Thought>(pool.clone(), report_thread.channel.clone());
    let _achievement_watcher = init_watcher::<Achievement>(pool.clone(), report_thread.channel.clone());
    let _notebook_watcher = init_watcher::<Notebook>(pool.clone(), report_thread.channel.clone());

    println!("   Done.");

    let api = filters::cards::<Project>(pool.clone())
        .or(filters::cards::<Task>(pool.clone()))
        .or(filters::cards::<Status>(pool.clone()))
        .or(filters::cards::<Timelog>(pool.clone()))
        .or(filters::cards::<Purchase>(pool.clone()))
        .or(filters::cards::<Metric>(pool.clone()))
        .or(filters::cards::<Word>(pool.clone()))
        .or(filters::cards::<Note>(pool.clone()))
        .or(filters::cards::<Thought>(pool.clone()))
        .or(filters::cards::<Achievement>(pool.clone()))
        .or(filters::cards::<Notebook>(pool.clone()))
        .or(filters::cards::<Book>(pool.clone()));

    warp::serve(api)
        .run(([127, 0, 0, 1], 8000))
        .await;

    report::quit_thread(report_thread);
}
