// Fuck Rust...

#[macro_use] extern crate rocket;

use std::io::Cursor;
use std::str::FromStr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use notify::{RecursiveMode, Watcher};
use notify::event::{CreateKind, DataChange, ModifyKind, RemoveKind, RenameMode};
use rocket::{Rocket, Build, Request, Response, Orbit, Data, tokio};
use rocket::fairing::{AdHoc, Fairing, Info};
use rocket::futures::executor::block_on;
use rocket_sync_db_pools::{database, rusqlite};
use rocket::http::ContentType;
use rocket::response::Responder;
use crate::cards::{Card, Project, Task, Status, Timelog, CardType, get_path_to_cards};
use crate::tokio::task::JoinHandle;

#[database("sqlite_cards")]
struct CardsDb(rusqlite::Connection);

mod cards;

struct IdList {
    ids: Vec<u64>,
}

#[rocket::async_trait]
impl<'r> Responder<'r, 'static> for IdList {
    fn respond_to(self, request: &'r Request<'_>) -> rocket::response::Result<'static> {

        ////FIXME: this is probably the least efficient way possible to create a string of this size
        let mut s = String::from("[");
        let mut first = true;
        for id in self.ids.iter() {
            if !first {
                s.push_str(",");
            }
            s.push_str(&id.to_string());
            first = false;
        }
        s.push_str("]");

        Response::build()
            .header(ContentType::JSON)
            .streamed_body(Cursor::new(s))
            .ok()
    }
}

// Lists.
#[get("/project")]
async fn get_project_list(db: CardsDb) -> IdList {
    db.run(|c| {
        IdList { ids: Project::sql_list_ids(&c).expect("can list project IDs") }
    }).await
}
#[get("/task")]
async fn get_task_list(db: CardsDb) -> IdList {
    db.run(|c| {
        IdList { ids: Task::sql_list_ids(&c).expect("can list task IDs") }
    }).await
}
#[get("/status")]
async fn get_status_list(db: CardsDb) -> IdList {
    db.run(|c| {
        IdList { ids: Status::sql_list_ids(&c).expect("can list status IDs") }
    }).await
}
#[get("/timelog")]
async fn get_timelog_list(db: CardsDb) -> IdList {
    db.run(|c| {
        IdList { ids: Timelog::sql_list_ids(&c).expect("can list timelog IDs") }
    }).await
}

// Counts.
#[get("/project/count")]
async fn get_project_count(db: CardsDb) -> String {
    db.run(|c| {
        c.query_row(format!("SELECT COUNT(*) FROM {}", Project::sql_table()).as_str(), [],
            |row| row.get::<usize, usize>(0))
            .expect("can query project count").to_string()
    }).await
}
#[get("/task/count")]
async fn get_task_count(db: CardsDb) -> String {
    db.run(|c| {
        c.query_row(format!("SELECT COUNT(*) FROM {}", Task::sql_table()).as_str(), [],
                    |row| row.get::<usize, usize>(0))
            .expect("can query task count").to_string()
    }).await
}
#[get("/status/count")]
async fn get_status_count(db: CardsDb) -> String {
    db.run(|c| {
        c.query_row(format!("SELECT COUNT(*) FROM {}", Status::sql_table()).as_str(), [],
                    |row| row.get::<usize, usize>(0))
            .expect("can query status count").to_string()
    }).await
}
#[get("/timelog/count")]
async fn get_timelog_count(db: CardsDb) -> String {
    db.run(|c| {
        c.query_row(format!("SELECT COUNT(*) FROM {}", Timelog::sql_table()).as_str(), [],
                    |row| row.get::<usize, usize>(0))
            .expect("can query timelog count").to_string()
    }).await
}

// Contents.
async fn get_card<T: Card>(db: CardsDb, nameOrId: &str) -> (rocket::http::Status, (ContentType, String)) {
    let name_or_id_str = String::from(nameOrId);
    db.run(move |c| {
        match T::sql_find_id(&c, &name_or_id_str) {
            Ok(id) => {
                match T::json(id) {
                    Ok(s) => (rocket::http::Status::Ok, (ContentType::JSON, s)),
                    Err(e) => (rocket::http::Status::InternalServerError, (ContentType::Text, format!("{:?}", e)))
                }
            },
            Err(cards::Error::CantFindCard(s)) =>
                (rocket::http::Status::BadRequest, (ContentType::Text, format!("Cannot find {} '{}' ({})", T::typ_str(), &name_or_id_str, s))),
            Err(e) =>
                (rocket::http::Status::InternalServerError, (ContentType::Text, format!("{:?}", e)))
        }
    }).await
}
#[get("/project/<nameOrId>")]
async fn get_project(db: CardsDb, nameOrId: &str) -> (rocket::http::Status, (ContentType, String)) {
    get_card::<Project>(db, nameOrId).await
}
#[get("/task/<nameOrId>")]
async fn get_task(db: CardsDb, nameOrId: &str) -> (rocket::http::Status, (ContentType, String)) {
    get_card::<Task>(db, nameOrId).await
}
#[get("/status/<nameOrId>")]
async fn get_status(db: CardsDb, nameOrId: &str) -> (rocket::http::Status, (ContentType, String)) {
    get_card::<Status>(db, nameOrId).await
}
#[get("/timelog/<nameOrId>")]
async fn get_timelog(db: CardsDb, nameOrId: &str) -> (rocket::http::Status, (ContentType, String)) {
    get_card::<Timelog>(db, nameOrId).await
}

fn prepare_card_write_stmts<T: Card>(db: &mut rusqlite::Connection) -> Result<(rusqlite::Statement, rusqlite::Statement), rusqlite::Error> {
    Ok((db.prepare(T::sql_write_stmt())?,
     db.prepare("INSERT INTO Links (role, from_type, from_id, to_type, to_id) VALUES(?1, ?2, ?3, ?4, ?5)")?))
}

fn load_card_into_db<T: Card>(id: u64, db: &mut rusqlite::Connection) -> Result<(), cards::Error> {

    let (mut sql, mut link) = prepare_card_write_stmts::<T>(db)
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    let card = T::load(id)?;
    card.sql_write(&mut sql)?;
    card.sql_write_links(&mut link)?;

    Ok(())
}

fn load_all_cards_into_db<T: Card>(db: &mut rusqlite::Connection) -> Result<(), cards::Error> {

    let (mut sql, mut link) = prepare_card_write_stmts::<T>(db)
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    for id in T::list() {
        let card = T::load(id)?;
        card.sql_write(&mut sql)?;
        card.sql_write_links(&mut link)?;
    }

    Ok(())
}

fn populate_db_from_scratch(db: &mut rusqlite::Connection) -> Result<(), cards::Error> {
    load_all_cards_into_db::<Project>(db)?;
    load_all_cards_into_db::<Task>(db)?;
    load_all_cards_into_db::<Status>(db)?;
    load_all_cards_into_db::<Timelog>(db)
}

async fn init_db(rocket: Rocket<Build>) -> Rocket<Build> {

    // For now, rebuild from scratch every time.
    CardsDb::get_one(&rocket).await
        .expect("database mounted")
        .run(|db| {
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
               Project::sql_schema(), Task::sql_schema(), Status::sql_schema(), Timelog::sql_schema());

            db.execute_batch(&stmt,)
                .expect("can init DB");

            populate_db_from_scratch(db)
                .expect("can load cards into DB")
        }).await;

    rocket
}

enum FileChange {
    Added(String),
    Removed(String),
    Modified(String),
}

struct FileWatcher {
    //watcher: notify::RecommendedWatcher,
    watcher: JoinHandle<()>,
    //processor: JoinHandle<()>,
}

impl Fairing for FileWatcher {
    fn info(&self) -> Info {
        Info {
            name: "FileWatcher",
            kind: rocket::fairing::Kind::Ignite,
        }
    }
}


fn init_file_watcher<T: Card>(db: CardsDb) -> FileWatcher {

    let watcher = tokio::spawn(db.run(|c| {

    }));

    /*
    let (sender, receiver) = channel();

    // Watcher.
    thread::spawn(move|| {

        let mut watcher = notify::recommended_watcher(|res: Result<notify::Event, notify::Error>| {

            fn is_json_file(path: &PathBuf) -> bool {
                if let Some(ext) = path.extension() {
                    ext == "json"
                }
                else {
                    false
                }
            }

            match res {
                Ok(event) => {
                    match event.kind {
                        notify::EventKind::Create(_) => {
                            for path in event.paths.iter().filter(|p| is_json_file(p)) {
                                println!("Added card {}", path.to_str().unwrap());
                                if let Some(name) = path.file_stem() {
                                    sender.send(FileChange::Added(name.to_str().unwrap()))
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
            .expect("can create file system watcher");

        watcher.watch(T::path().as_path(), RecursiveMode::NonRecursive)
            .expect("can watch card directory");
    })

    let mut watcher = notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {

        fn is_json_file(path: &PathBuf) -> bool {
            if let Some(ext) = path.extension() {
                ext == "json"
            }
            else {
                false
            }
        }

        fn add_card<T: Card>(name: String, db: Rc<CardsDb>) {
            if let Ok(id) = name.parse::<u64>() {
                db.run(move |c| {
                    match load_card_into_db::<T>(id, c) {
                        Err(e) => println!("Cannot write card '{}/{}': {:?}", T::typ_str(), name, e),
                        _ => (),
                    }
                });
            }
        }

        match res {
            Ok(event) => {
                match event.kind {
                    notify::EventKind::Create(_) => {
                        for path in event.paths.iter().filter(|p| is_json_file(p)) {
                            println!("Added card {}", path.to_str().unwrap());
                            if let Some(name) = path.file_stem() {
                                add_card::<T>(String::from(name.to_str().unwrap()), Rc::clone(&db));
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
        .expect("can create file system watcher");

    watcher.watch(T::path().as_path(), RecursiveMode::NonRecursive)
        .expect("can watch card directory");
     */

    //FileWatcher { watcher }
    FileWatcher { watcher }
}

#[launch]
fn rocket() -> _ {

    let db_init = AdHoc::on_ignite("Rusqlite Stage", |rocket| async {
        rocket
            .attach(CardsDb::fairing())
            .attach(AdHoc::on_ignite("Rusqlite Init", init_db))
            //.attach(AdHoc::on_ignite("FSWatch Projects", init_file_watcher::<Project>))
            .attach(AdHoc::on_ignite("FSWatch Projects", |rocket| async {
                let db = CardsDb::get_one(&rocket).await.expect("can get connection to DB");
                //let dbRef = Rc::new(db);
                rocket
                    .attach(init_file_watcher::<Project>(db))
                    //.attach(init_file_watcher::<Task>(Rc::clone(&dbRef)))
            }))
    });

    rocket::build()
        .attach(db_init)
        .mount("/", routes![
            get_project_list, get_project_count, get_project,
            get_task_list, get_task_count, get_task,
            get_status_list, get_status_count, get_status,
            get_timelog_list, get_timelog_count, get_timelog,
        ])
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
