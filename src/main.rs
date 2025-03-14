pub mod mongo_connection;
pub mod nats_server;

use async_nats::Client as NatsClient;
use mongodb::bson::Document;
use mongodb::change_stream::event::OperationType;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal::ctrl_c;
use tokio::time::{sleep, Duration};

async fn run_mongo_oplog_watching(shutdown: Arc<AtomicBool>) {
    // MongoDB connection parameters
    let mongo_connection = "mongodb://localhost:27017";
    let mongo_database = "gravel_db";

    println!("Starting MongoDB oplog watching...");

    let database =
        match mongo_connection::get_mongo_connection(mongo_connection, mongo_database).await {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Error on getting MongoDB connection: {}", e);
                return;
            }
        };

    // Create a change stream pipeline to watch all operations
    // You can customize this pipeline to filter specific operations or collections
    let pipeline = vec![];

    let mut stream = match database
        .collection::<Document>("watch_test")
        .watch().pipeline(pipeline.clone())
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Error on getting MongoDB change stream: {}", e);
            return;
        }
    };

    // Process MongoDB change stream events until shutdown is signaled
    println!("Watching for MongoDB changes...");
    let mut resume_token = None;
    while stream.is_alive() {
        let change = stream.next_if_any().await;

        if shutdown.load(Ordering::SeqCst) {
            println!("Shutting down MongoDB oplog watching...");
            break;
        }

        match change {
            Ok(Some(change)) => {
                // Process the change event
                println!("Received MongoDB change: {:?}", change);

                // Here you can add logic to handle different types of operations
                let op_type = change.operation_type;

                    match op_type {
                        OperationType::Insert => {
                            println!("Insert operation detected");
                            // Handle insert operation
                        },
                        OperationType::Update => {
                            println!("Update operation detected");
                            // Handle update operation
                        },
                        OperationType::Delete => {
                            println!("Delete operation detected");
                            // Handle delete operation
                        },
                        OperationType::Replace => {
                            println!("Replace operation detected");
                            // Handle replace operation
                        },
                        _ => {
                            println!("Other operation type: {:?}", op_type);
                            // Handle other operation types
                        }
                    }

            },
            Ok(None) => {
                println!("No changes detected");
            }
            Err(e) => {
                eprintln!("Error on getting MongoDB change: {}", e);

                // Try to reconnect after a brief pause
                sleep(Duration::from_secs(1)).await;

                // Attempt to recreate the change stream
                match database
                    .collection::<Document>("watch_test")
                    .watch().pipeline(pipeline.clone())
                    .await
                {
                    Ok(new_stream) => {
                        stream = new_stream;
                        println!("Successfully reconnected to MongoDB change stream");
                    },
                    Err(reconnect_err) => {
                        eprintln!("Failed to reconnect to MongoDB change stream: {}", reconnect_err);

                        // If we can't reconnect, check if we should exit
                        if shutdown.load(Ordering::SeqCst) {
                            break;
                        }

                        // Wait before trying again
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    
        resume_token = stream.resume_token();
    }

    println!("MongoDB oplog watching stopped");
}

#[tokio::main]
async fn main() {
    // Handle to shutdown the application gracefully
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    // Setup Ctrl+C handler
    tokio::spawn(async move {
        if let Err(e) = ctrl_c().await {
            eprintln!("Failed to listen for ctrl+c: {}", e);
            return;
        }
        println!("Received shutdown signal");
        shutdown_clone.store(true, Ordering::SeqCst);
    });

    // Start the NATS server and get a client connection
    let nats_client = match nats_server::get_nats_server_connection(
        "./nats-server-v2.10.26-windows-amd64/nats-server",
        "nats://localhost:4222",
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Error on getting NATS client: {}", e);
            return;
        }
    };

    run_mongo_oplog_watching(shutdown).await;

    println!("Shutting down...");

    nats_server::drop_manually();
}
