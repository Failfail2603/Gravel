use std::{process::{Child, Command}, time::Duration, sync::Mutex};

use async_nats::Client;
use lazy_static::lazy_static;

// Structure to manage the NATS server child process
pub struct NatsServer {
    process: Child,
}

// Create a static instance of NatsServer that will live for the duration of the program
lazy_static! {
    static ref NATS_SERVER_INSTANCE: Mutex<Option<NatsServer>> = Mutex::new(None);
}

impl NatsServer {
    // Start a new NATS server process
    pub fn start(binary_path: &str) -> Result<Self, std::io::Error> {
        println!("Starting NATS server...");
        
        // Start the NATS server as a child process
        // Adjust the command based on how NATS is installed on your system
        let process = Command::new(binary_path)
            .spawn()?;
            
        println!("NATS server started with PID: {}", process.id());
        
        // Give the server a moment to start up
        std::thread::sleep(Duration::from_secs(2));
        
        Ok(NatsServer { process })
    }
}

impl Drop for NatsServer {
    // Ensure the NATS server is killed when this object is dropped
    fn drop(&mut self) {
        println!("Shutting down NATS server...");
        
        // Try to kill the process gracefully
        match self.process.kill() {
            Ok(_) => println!("NATS server stopped successfully"),
            Err(e) => eprintln!("Failed to stop NATS server: {}", e),
        }
    }
}

pub fn drop_manually() {
    let mut server_instance = NATS_SERVER_INSTANCE.lock().unwrap();

    if server_instance.is_some() {
        server_instance.take();
    }
}

pub async fn get_nats_server_connection(binary_path: &str, connection_string: &str) -> Result<Client, Box<dyn std::error::Error>> {
    // Initialize the NATS server if it hasn't been started yet
    {
        
        let mut server_instance = match NATS_SERVER_INSTANCE.lock() {
            Ok(guard) => guard,
            Err(_e) => {
                return Err("Failed to acquire lock on NATS server instance".into())
            },
        };
        
        if server_instance.is_none() {
            println!("Starting NATS server for the first time...");
            *server_instance = Some(match NatsServer::start(binary_path) {
                Ok(server) => server,
                Err(_e) => {
                    return Err("Failed to start NATS server".into());
                },
            });
        }
    }

    // Connect to the NATS server
    println!("Connecting to NATS...");
    let client= match async_nats::connect(connection_string).await {
        Ok(client) => client,
        Err(_e) => {
            return Err("Failed to connect to NATS server".into());
        },
    };

    Ok(client)
}