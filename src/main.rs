use async_nats::Client;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal::ctrl_c;
use tokio::time::{sleep, Duration};

// Structure to manage the NATS server child process
struct NatsServer {
    process: Child,
}

impl NatsServer {
    // Start a new NATS server process
    fn start() -> Result<Self, std::io::Error> {
        println!("Starting NATS server...");
        
        // Start the NATS server as a child process
        // Adjust the command based on how NATS is installed on your system
        let process = Command::new("/nats-server-v2.10.26-windows-amd64/nats-server")
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start the NATS server
    let _nats_server = match NatsServer::start() {
        Ok(server) => server,
        Err(e) => {
            eprintln!("Failed to start NATS server: {}", e);
            return Err(Box::new(e));
        }
    };

    // Flag to track shutdown status
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    // Set up signal handler for graceful shutdown
    tokio::spawn(async move {
        if let Err(e) = ctrl_c().await {
            eprintln!("Failed to listen for ctrl+c: {}", e);
            return;
        }
        println!("Received shutdown signal");
        shutdown_clone.store(true, Ordering::SeqCst);
    });

    // Connect to the NATS server
    println!("Connecting to NATS...");
    let client = async_nats::connect("nats://localhost:4222").await;
    println!("Connected to NATS server");

    // Subscribe to the default channel
    let mut subscriber = client.subscribe("default".into()).await;
    println!("Subscribed to 'default' channel");

    // Process messages until shutdown signal is received
    while !shutdown.load(Ordering::SeqCst) {
        tokio::select! {
            message = subscriber.next() => {
                match message {
                    Some(msg) => {
                        let payload = String::from_utf8_lossy(&msg.payload);
                        println!("Received message: {}", payload);
                    },
                    None => {
                        println!("Subscription closed");
                        break;
                    }
                }
            },
            _ = sleep(Duration::from_millis(100)) => {
                // Just a small delay to prevent CPU spinning
            }
        }
    }

    println!("Shutting down client...");
    client.close().await?;
    println!("Client shut down successfully");

    // The NatsServer will be dropped automatically when it goes out of scope,
    // which will kill the child process due to the Drop implementation

    Ok(())
}
