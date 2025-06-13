use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use jsonrpc_http_server::jsonrpc_core::{Error, ErrorCode, IoHandler, Params, Value};
use jsonrpc_http_server::{DomainsValidation, ServerBuilder};

#[derive(Debug)]
struct WordIndex {
    lines: Vec<String>,
    index: HashMap<String, Vec<usize>>,
}

impl WordIndex {
    fn new(filename: &str) -> Result<Self, std::io::Error> {
        let path = Path::new(filename);
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let mut lines = Vec::new();
        let mut index: HashMap<String, Vec<usize>> = HashMap::new();

        for (line_num, line_result) in reader.lines().enumerate() {
            let line = line_result?;
            lines.push(line.clone());

            let words = line
                .split_whitespace()
                .map(|word| {
                    word.to_lowercase()
                        .chars()
                        .filter(|c| c.is_alphanumeric())
                        .collect::<String>()
                })
                .filter(|word| !word.is_empty());

            for word in words {
                index.entry(word).or_default().push(line_num);
            }
        }
        Ok(WordIndex { lines, index })
    }

    pub fn search(&self, query: &str) -> Vec<usize> {
        let query_words: Vec<String> = query
            .split_whitespace()
            .map(|word| {
                word.to_lowercase()
                    .chars()
                    .filter(|c| c.is_alphanumeric())
                    .collect::<String>()
            })
            .filter(|word| !word.is_empty())
            .collect();

        if query_words.is_empty() {
            return Vec::new();
        }

        let mut result_line_nums: Option<HashSet<usize>> = None;

        for word in query_words {
            if let Some(line_nums_for_word) = self.index.get(&word) {
                let current_word_set: HashSet<usize> =
                    line_nums_for_word.iter().cloned().collect();
                if let Some(ref mut existing_set) = result_line_nums {
                    existing_set.retain(|line_num| current_word_set.contains(line_num));
                } else {
                    result_line_nums = Some(current_word_set);
                }
            } else {
                return Vec::new();
            }
        }

        if let Some(final_set) = result_line_nums {
            let mut sorted_results: Vec<usize> = final_set.into_iter().collect();
            sorted_results.sort_unstable();
            sorted_results
        } else {
            Vec::new()
        }
    }

    pub fn fetch(&self, line_number: usize) -> Option<String> {
        if line_number < self.lines.len() {
            Some(self.lines[line_number].clone())
        } else {
            None
        }
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long, value_delimiter = ',', help = "IP:PORT addresses to listen on (comma-separated)")]
    addresses: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.addresses.is_empty() {
        eprintln!("Error: No addresses provided. Please specify at least one address using --addresses ip:port.");
        std::process::exit(1);
    }

    println!("Loading database from db.txt...");
    let word_index = match WordIndex::new("db.txt") {
        Ok(wi) => Arc::new(wi),
        Err(e) => {
            eprintln!("Failed to load db.txt: {}", e);
            std::process::exit(1);
        }
    };
    println!("Database loaded successfully.");

    let mut handler = IoHandler::new();

    // RPC "search" method
    let wi_search = Arc::clone(&word_index);
    handler.add_method("search", move |params: Params| {
        let wi = Arc::clone(&wi_search);
        async move {
            match params.parse::<(String,)>() {
                Ok((query,)) => {
                    let results = wi.search(&query);
                    Ok(Value::Array(
                        results.into_iter().map(|n| Value::Number(n.into())).collect(),
                    ))
                }
                Err(_) => Err(Error {
                    code: ErrorCode::InvalidParams,
                    message: "Invalid parameters: Expected a single string query.".into(),
                    data: None,
                }),
            }
        }
    });

    // RPC "fetch" method
    let wi_fetch = Arc::clone(&word_index);
    handler.add_method("fetch", move |params: Params| {
        let wi = Arc::clone(&wi_fetch);
        async move {
            match params.parse::<(usize,)>() {
                Ok((line_number,)) => match wi.fetch(line_number) {
                    Some(line) => Ok(Value::String(line)),
                    None => Err(Error {
                        code: ErrorCode::ServerError(-32001), // Custom error code
                        message: "Invalid record ID: Line number out of bounds.".into(),
                        data: None,
                    }),
                },
                Err(_) => Err(Error {
                    code: ErrorCode::InvalidParams,
                    message: "Invalid parameters: Expected a single unsigned integer line number."
                        .into(),
                    data: None,
                }),
            }
        }
    });

    let mut server_handles = Vec::new();

    for addr_str in cli.addresses {
        println!("Attempting to start server on {}...", addr_str);
        match addr_str.parse::<std::net::SocketAddr>() {
            Ok(socket_addr) => {
                let server = ServerBuilder::new(handler.clone()) // Clone handler for each server
                    .cors(DomainsValidation::Disabled)
                    .start_http(&socket_addr);

                match server {
                    Ok(s) => {
                        println!("Server listening on http://{}", socket_addr);
                        server_handles.push(s); // Store the server handle (optional for just waiting)
                    }
                    Err(e) => {
                        eprintln!("Failed to start server on {}: {:?}", socket_addr, e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Invalid address format '{}': {}", addr_str, e);
            }
        }
    }

    if server_handles.is_empty() {
        eprintln!("No servers were started successfully.");
        return Ok(());
    }

    println!("Servers started. Press Ctrl+C to shut down.");
    tokio::signal::ctrl_c().await?;
    println!("Ctrl+C received, shutting down servers.");

    // Optional: explicitly close servers if needed, though dropping handles might be enough
    // for handle in server_handles {
    //     handle.close();
    // }

    Ok(())
}
