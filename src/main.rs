use std::ops::Deref;
use std::path::{PathBuf};
use std::sync::mpsc::{Receiver, Sender};
use notify::{RecursiveMode, Watcher};
use notify::event::{CreateKind, DataChange, ModifyKind, RemoveKind, RenameMode};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use warp::Filter;
use crate::cards::{Card, Project, Task, Status, Timelog, CardType, get_path_to_cards};

mod cards;

fn prepare_card_write_stmts<T: Card>(db: &rusqlite::Connection) -> Result<(rusqlite::Statement, rusqlite::Statement), rusqlite::Error> {
    Ok((db.prepare(T::sql_write_stmt())?,
     db.prepare("INSERT INTO Links (role, from_type, from_id, to_type, to_id) VALUES(?1, ?2, ?3, ?4, ?5)")?))
}

fn load_card_into_db<T: Card>(id: u64, db: &rusqlite::Connection) -> Result<(), cards::Error> {

    let (mut sql, mut link) = prepare_card_write_stmts::<T>(db)
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    let card = T::load(id)?;
    card.sql_write(&mut sql)?;
    card.sql_write_links(&mut link)?;

    Ok(())
}

fn load_all_cards_into_db<T: Card>(db: &rusqlite::Connection) -> Result<(), cards::Error> {

    let (mut sql, mut link) = prepare_card_write_stmts::<T>(db)
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    for id in T::list() {
        let card = T::load(id)?;
        card.sql_write(&mut sql)?;
        card.sql_write_links(&mut link)?;
    }

    Ok(())
}

fn populate_db_from_scratch(db: &rusqlite::Connection) -> Result<(), cards::Error> {
    load_all_cards_into_db::<Project>(db)?;
    load_all_cards_into_db::<Task>(db)?;
    load_all_cards_into_db::<Status>(db)?;
    load_all_cards_into_db::<Timelog>(db)
}

fn init_db(db: &rusqlite::Connection) -> Result<(), cards::Error> {

    // For now, rebuild from scratch every time.
    let stmt = format!(r#"
        BEGIN;
        DROP TABLE IF EXISTS Tags;
        DROP TABLE IF EXISTS Taggings;
        DROP TABLE IF EXISTS Links;
        CREATE TABLE IF NOT EXISTS Tags (
            id INTEGER PRIMARY KEY UNIQUE,
            text VARCHAR NOT NULL
        );
        CREATE TABLE IF NOT EXISTS Taggings (
            id INTEGER PRIMARY KEY UNIQUE,
            tag_id INTEGER,
            card_type INTEGER,
            card_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS Links (
            id INTEGER PRIMARY KEY UNIQUE,
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
        COMMIT;"#,
                       Project::sql_schema(),
                       Task::sql_schema(),
                       Status::sql_schema(),
                       Timelog::sql_schema());

    db.execute_batch(&stmt,)
        .map_err(|err| { cards::Error::DatabaseError(err.to_string())})?;

    populate_db_from_scratch(db)
}

struct FileWatcher {
    watcher: notify::RecommendedWatcher,
}

fn init_watcher<T: Card>(db: Pool<SqliteConnectionManager>) -> FileWatcher {

    let mut watcher = notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {

        fn is_json_file(path: &PathBuf) -> bool {
            if let Some(ext) = path.extension() {
                ext == "json"
            }
            else {
                false
            }
        }

        fn add_card<T: Card>(name: String, db: Pool<SqliteConnectionManager>) {
            if let Ok(id) = name.parse::<u64>() {
                let db = db.get()
                    .expect("Cannot get DB connection");
                match load_card_into_db::<T>(id, &db) {
                    Err(e) => println!("Cannot write card '{}/{}': {:?}", T::typ_str(), name, e),
                    _ => (),
                }
            }
        }

        match res {
            Ok(event) => {
                match event.kind {
                    notify::EventKind::Create(_) => {
                        for path in event.paths.iter().filter(|p| is_json_file(p)) {
                            println!("Added card {}", path.to_str().unwrap());
                            if let Some(name) = path.file_stem() {
                                add_card::<T>(String::from(name.to_str().unwrap()), db.clone());
                            };
                        }
                    },
                    notify::EventKind::Remove(RemoveKind::File) => {},
                    notify::EventKind::Modify(ModifyKind::Data(DataChange::Content)) => {},
                    notify::EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {},
                    _ => {}, // Ignore
                }
            }
            Err(e) => println!("FSWatcher error happened: {}", e.to_string()),
        }
    })
        .expect("Cannot create file system watcher");

    watcher.watch(T::path().as_path(), RecursiveMode::NonRecursive)
        .expect("Cannot watch card directory");

    FileWatcher { watcher }
}

mod filters {
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

    pub async fn list<T: Card>(db: Pool<SqliteConnectionManager>) -> Result<impl warp::Reply, Infallible> {

        let db = db.get()
            .expect("Cannot get DB connection from pool");

        let ids = T::sql_list_ids(&db)
            .expect("can list task IDs");

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
            Err(cards::Error::CantFindCard(e)) => (format!("Cannot find card {}", e), StatusCode::NOT_FOUND),
            Err(e) => (format!("Error: {:?}", e), StatusCode::INTERNAL_SERVER_ERROR),
        };

        // T::json gives us a string that is already serialized JSON data.
        Ok(warp::reply::with_status(Json { inner: Ok(s.into_bytes()) }, code))
    }
}

mod watcher {
}

#[tokio::main]
async fn main() {

    println!("Initializing database...");
    let manager = SqliteConnectionManager::file("cards.sqlite");
    let pool = r2d2::Pool::new(manager)
        .expect("Cannot create DB connection pool");

    init_db(pool.clone().get().expect("Cannot get DB connection").deref())
        .expect("Cannot initialize DB");

    let _project_watcher = init_watcher::<Project>(pool.clone());
    let _task_watcher = init_watcher::<Task>(pool.clone());
    let _status_watcher = init_watcher::<Status>(pool.clone());
    let _timelog_watcher = init_watcher::<Timelog>(pool.clone());

    println!("   Done.");

    let api = filters::cards::<Project>(pool.clone())
        .or(filters::cards::<Task>(pool.clone()))
        .or(filters::cards::<Status>(pool.clone()))
        .or(filters::cards::<Timelog>(pool.clone()));

    warp::serve(api)
        .run(([127, 0, 0, 1], 8000))
        .await;
}
