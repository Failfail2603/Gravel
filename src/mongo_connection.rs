use std::{time::Duration, sync::Mutex};
use lazy_static::lazy_static;

use mongodb::{error::Error as MongoError, Client, Database};

// Structure to manage the MongoDB connection
pub struct MongoConnection {
    client: Client,
}

// Create a static instance of MongoConnection that will live for the duration of the program
lazy_static! {
    static ref MONGO_CONNECTION_INSTANCE: Mutex<Option<MongoConnection>> = Mutex::new(None);
}

impl MongoConnection {
    // Start a new MongoDB connection
    pub async fn start(connection_string: &str) -> Result<Self, MongoError> {
        println!("Starting MongoDB connection...");
        
        // Connect to MongoDB
        let client = Client::with_uri_str(connection_string).await?;
        
        Ok(MongoConnection { client })
    }
}

pub async fn get_mongo_connection(connection_string: &str, database: &str) -> Result<Database, Box<dyn std::error::Error>> {
    // Initialize the MongoDB connection if it hasn't been started yet
    
        let mut mongo_connection = MONGO_CONNECTION_INSTANCE.lock().unwrap();
        if mongo_connection.is_none() {
            println!("Starting MongoDB connection for the first time...");
            match MongoConnection::start(connection_string).await {
                Ok(connection) => {
                    *mongo_connection = Some(connection);
                },
                Err(_e) => {
                    
                    return Err("Failed to connect to MongoDB".into());
                }
            }
        }
    

    // Get the MongoDB connection
    Ok(mongo_connection.as_ref().unwrap().client.database(database))
}