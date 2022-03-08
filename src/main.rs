#[macro_use] extern crate rocket;

//use rusqlite::{Connection, Result};
//use rusqlite::NO_PARAMS;

use rocket::{Rocket, Build};
use rocket::fairing::AdHoc;
use rocket_sync_db_pools::{database, rusqlite};
use crate::cards::{Cardlike, Project};

#[database("sqlite_cards")]
struct CardsDb(rusqlite::Connection);

mod cards;

#[get("/project/count")]
async fn get_project_count(db: CardsDb) -> String {
    db.run(|c| {
        let r = c.query_row("SELECT COUNT(*) FROM Projects", [],
            |row| row.get::<usize, usize>(0));
        r.expect("can query project count").to_string()
    }).await
}

#[get("/project/<id>")]
fn get_project(db: CardsDb, id: u64) -> String {
    format!("Not yet...")
}

fn load_all_cards_into_db<T: Cardlike>(db: &mut rusqlite::Connection) -> Result<(), cards::Error> {

    let mut sql = db.prepare(T::sql_write_stmt())
        .map_err(|err| cards::Error::DatabaseError(err.to_string()))?;

    for id in T::list() {
        let card = T::load(id)?;
        card.sql_write(&mut sql)?;
    }

    Ok(())
}

fn populate_db_from_scratch(db: &mut rusqlite::Connection) -> Result<(), cards::Error> {
    load_all_cards_into_db::<Project>(db)
}

async fn init_db(rocket: Rocket<Build>) -> Rocket<Build> {

    // For now, rebuild from scratch every time.
    CardsDb::get_one(&rocket).await
        .expect("database mounted")
        .run(|db| {
            let stmt = format!(r#"
                BEGIN;
                DROP TABLE IF EXISTS Projects;
                {}
                COMMIT;"#, Project::sql_schema());

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
        .mount("/", routes![get_project_count, get_project])
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
