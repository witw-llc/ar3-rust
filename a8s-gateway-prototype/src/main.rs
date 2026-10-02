use ar3_server_prototype::{
    Error, Result,
    api::{App, Service, router, validate_bind},
    model::*,
    provider::{LocalObjects, LocalStorage, S3Remote, bounded_read},
    store::Store,
};
use std::{net::SocketAddr, path::Path};

#[tokio::main(worker_threads = 2)]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("ar3-server: {e}");
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    match a.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["init", db, joined] => {
            Store::init(db, joined)?;
            println!("Initialized synthetic state with explicit cutoff.");
        }
        ["status", db] => {
            println!("{}", Store::open(db)?.status()?);
        }
        ["serve", db, objects, bind] => {
            let addr: SocketAddr = bind.parse().map_err(|_| Error("invalid_input"))?;
            validate_bind(addr)?;
            let service = Service::open(Path::new(db), Path::new(objects))?;
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|_| Error("listener_unavailable"))?;
            eprintln!(
                "ar3-server: local synthetic API on {} (SQLite {}); no HTTPS or live providers",
                listener
                    .local_addr()
                    .map_err(|_| Error("listener_unavailable"))?,
                rusqlite::version()
            );
            axum::serve(listener, router(App::new(service)))
                .with_graceful_shutdown(shutdown())
                .await
                .map_err(|_| Error("listener_unavailable"))?;
        }
        ["backup", db, snapshot] => {
            Store::open(db)?.backup(Path::new(snapshot))?;
            println!("Snapshot created; provider doubles are separate test state.");
        }
        ["restore", snapshot, db] => {
            println!(
                "{}",
                Store::restore(Path::new(snapshot), Path::new(db))?.status()?
            );
        }
        ["cutover", db] => {
            let mut s = Store::open(db)?;
            s.cutover()?;
            println!(
                "Cutover recorded; previous pending deliveries remain uncertain and outbound remains blocked."
            );
        }
        ["reviewed-requeue", db, mid] => {
            Store::open(db)?.reviewed_requeue("synthetic", mid)?;
            println!("Selected message requeued.");
        }
        ["publish", db, objects, mid] => {
            let mut s = Store::open(db)?;
            let mut remote = S3Remote(LocalObjects::new(objects)?);
            let mut storage = LocalStorage::new(objects)?;
            s.publish("synthetic", mid, &mut remote, &mut storage)?;
            println!("Selected message accepted by local provider double.");
        }
        ["ingest", db, objects, file] => {
            let raw = bounded_read(Path::new(file), MAX_JSON)?;
            let e = Envelope::parse(&raw)?;
            let mut s = Store::open(db)?;
            let mut storage = LocalStorage::new(objects)?;
            let custody = s.ingest(
                "synthetic",
                &e.key()?,
                &raw,
                &[
                    Principal::synthetic("analyst"),
                    Principal::synthetic("builder"),
                ],
                &mut storage,
            )?;
            println!("{}", serde_json::json!({"custody":custody}));
        }
        ["outbox", db] => {
            println!(
                "{}",
                serde_json::to_string(&Store::open(db)?.outbox("synthetic")?)?
            );
        }
        _ => {
            eprintln!(
                "ar3-server: use init DB JOINED_ULID | serve DB OBJECTS LOOPBACK:PORT | status DB | ingest DB OBJECTS JSON | outbox DB | publish DB OBJECTS MESSAGE_ID | backup DB SNAPSHOT | restore SNAPSHOT NEW_DB | cutover DB | reviewed-requeue DB MESSAGE_ID"
            );
            return Err(Error("invalid_input"));
        }
    }
    Ok(())
}
async fn shutdown() {
    #[cfg(unix)]
    {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=signal.recv()=>{} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
