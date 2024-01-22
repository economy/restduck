use duckdb::{params, Connection, Result};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::util::pretty::print_batches;

#[macro_use]
extern crate rocket;

#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

// result from duckdb is clobbering result from rocket -- need to return formatted response using structured output??
#[get("/")]
fn index() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        r"CREATE SEQUENCE seq;
          CREATE TABLE IF NOT EXISTS person (
                  id              INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
                  name            TEXT NOT NULL,
                  data            BLOB
                  );
         ")?;

    let me = Person {
        id: 0,
        name: "Adam".to_string(),
        data: None,
    };

    conn.execute(
        "INSERT INTO person (name, data) VALUES (?, ?)",
        params![me.name, me.data],
    )?;

    let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: row.get(1)?,
            data: row.get(2)?,
        })
    })?;

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }

    // query table by arrow
    let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    print_batches(&rbs);
    Ok(())

}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![index])
}