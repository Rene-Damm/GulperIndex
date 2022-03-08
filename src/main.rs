#[macro_use] extern crate rocket;

// [X] Can load current project cards into DB
// [ ] Can detect new project card is added
// [ ] Can add newly added project to DB
// [ ] Can detect existing project card is removed
// [ ] Can remove existing project from DB
// [ ] Can get project card via GET

use std::io::Cursor;
use rocket::{Rocket, Build, Request, Response};
use rocket::fairing::AdHoc;
use rocket_sync_db_pools::{database, rusqlite};
use rocket::http::ContentType;
use rocket::response::Responder;
use crate::cards::{Card, Project, Status, Timelog};

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
#[get("/project/<id>")]
fn get_project(id: u64) -> (rocket::http::Status, (ContentType, String)) {
    match Project::json(id) {
        Ok(s) => (rocket::http::Status::Ok, (ContentType::JSON, s)),
        Err(e) => (rocket::http::Status::InternalServerError, (ContentType::Text, format!("{:?}", e)))
    }
}
#[get("/status/<id>")]
fn get_status(id: u64) -> (rocket::http::Status, (ContentType, String)) {
    match Status::json(id) {
        Ok(s) => (rocket::http::Status::Ok, (ContentType::JSON, s)),
        Err(e) => (rocket::http::Status::InternalServerError, (ContentType::Text, format!("{:?}", e)))
    }
}
#[get("/timelog/<id>")]
fn get_timelog(id: u64) -> (rocket::http::Status, (ContentType, String)) {
    match Timelog::json(id) {
        Ok(s) => (rocket::http::Status::Ok, (ContentType::JSON, s)),
        Err(e) => (rocket::http::Status::InternalServerError, (ContentType::Text, format!("{:?}", e)))
    }
}

fn load_all_cards_into_db<T: Card>(db: &mut rusqlite::Connection) -> Result<(), cards::Error> {

    let mut sql = db.prepare(T::sql_write_stmt())
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;
    let mut link = db.prepare("INSERT INTO Links (role, from_type, from_id, to_type, to_id) VALUES(?1, ?2, ?3, ?4, ?5)")
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
                DROP TABLE IF EXISTS {};
                DROP TABLE IF EXISTS {};
                DROP TABLE IF EXISTS {};
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
                COMMIT;"#,
               Project::sql_table(), Status::sql_table(), Timelog::sql_table(),
               Project::sql_schema(), Status::sql_schema(), Timelog::sql_schema());

            db.execute_batch(&stmt,)
                .expect("can init DB");

            populate_db_from_scratch(db)
                .expect("can load cards into DB")
        }).await;

    rocket
}

#[launch]
fn rocket() -> _ {
    let db_init = AdHoc::on_ignite("Rusqlite Stage", |rocket| async {
        rocket.attach(CardsDb::fairing())
            .attach(AdHoc::on_ignite("Rusqlite Init", init_db))
    });

    rocket::build()
        .attach(db_init)
        .mount("/", routes![
            get_project_list, get_project_count, get_project,
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
