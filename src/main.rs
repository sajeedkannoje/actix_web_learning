use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use chrono;
// #[get("/")]
// async fn hello() -> impl Responder {
//     HttpResponse::Ok().body("Hello world!")
// }

// #[post("/echo")]
// async fn echo(req_body: String) -> impl Responder {
//     HttpResponse::Ok().body(req_body)
// }

// async fn manual_hello() -> impl Responder {
//     HttpResponse::Ok().body("Hey there!")
// }

// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     HttpServer::new(|| {
//         App::new()
//             .service(hello)
//             .service(echo)
//             .route("/", web::get().to(manual_hello));

//             App::new()
//             .service(hello)
//             .service(echo)
//             .route("/test", web::post().to(manual_hello))
//     })
//     .bind(("127.0.0.1", 8080))?
//     .run()
//     .await
// }

use cassandra_cpp::{Cluster, Session};
async fn connect() -> cassandra_cpp::Result<Session> {
    let mut cluster = Cluster::default();
    cluster.set_contact_points("127.0.0.1").unwrap();
    cluster.connect_async().await
}
use cassandra_cpp::PreparedStatement;
async fn prepare(session: &Session) -> cassandra_cpp::Result<PreparedStatement> {
    session
        .prepare(
            "INSERT INTO test.measurements \
           (sensor_id, day, ts, temperature, humidity) \
           VALUES (?, ?, ?, ?, ?)",
        )
        .unwrap()
        .await
}

// use actix_web::{App, HttpServer, get};
#[get("/")]
async fn hello() -> String {
    "Hello Actix".to_owned()
}
#[derive(Debug, serde::Deserialize)]
struct SensorReadout {
    sensor_id: i32,
    temperature: f32,
    humidity: f32,
}
// #[derive(Debug, serde::Deserialize)]
struct CassandraCtx {
    session: Session,
    insert_stmt: PreparedStatement,
}
async fn save_sensor_readout(
    ctx: &CassandraCtx,
    readout: &SensorReadout,
 ) -> cassandra_cpp::Result<()> {
    let timestamp = chrono::Utc::now().timestamp_millis();
    let day = (timestamp / (24 * 3600 * 1000)) as i32;
    use cassandra_cpp::BindRustType;
    let mut stmt = ctx.insert_stmt.bind();
    stmt.bind(0, readout.sensor_id)?;
    stmt.bind(1, day)?;
    stmt.bind(2, timestamp)?;
    stmt.bind(3, readout.temperature)?;
    stmt.bind(4, readout.humidity)?;
    ctx.session.execute(&stmt).await?;
    Ok(())
 }
#[post("/measurements")]
async fn post_sensor_readout(
   ctx: web::Data<CassandraCtx>,
   data: web::Json<SensorReadout>,
) -> actix_web::Result<String> {
   match save_sensor_readout(ctx.get_ref(), &data).await {
      Ok(()) => Ok("OK".to_owned()),
      Err(e) => {
         eprintln!("Error saving {:?} to database: {}", &data, &e);
         Err(actix_web::error::ErrorInternalServerError(
            "Failed to save the object to the database",
         ))
      }
   }
}
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    use std::process;
    let session = connect().await.unwrap_or_else(|e| {
        eprintln!("Failed to connect to the database: {}", e);
        process::exit(1);
    });
    let insert_stmt = prepare(&session).await.unwrap_or_else(|e| {
        eprintln!("Failed to prepare the statement: {}", e);
        process::exit(1);
    });

    let ctx = CassandraCtx {
        session,
        insert_stmt,
    };
    let ctx = web::Data::new(ctx);
    async fn save_sensor_readout(
        ctx: &CassandraCtx,
        readout: &SensorReadout,
    ) -> cassandra_cpp::Result<()> {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let day = (timestamp / (24 * 3600 * 1000)) as i32;
        use cassandra_cpp::BindRustType;
        let mut stmt = ctx.insert_stmt.bind();
        stmt.bind(0, readout.sensor_id)?;
        stmt.bind(1, day)?;
        stmt.bind(2, timestamp)?;
        stmt.bind(3, readout.temperature)?;
        stmt.bind(4, readout.humidity)?;
        ctx.session.execute(&stmt).await?;
        Ok(())
    }
    HttpServer::new(move || {
        App::new()
            .app_data(ctx.clone())
            .service(post_sensor_readout)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
    //  HttpServer::new(move || App::new().service(post_sensor_readout))
    //       .bind("127.0.0.1:8080")?
    //       .run()
    //       .await
}
