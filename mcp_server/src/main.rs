use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use env_logger; // Added env_logger
use jsonrpc_http_server::jsonrpc_core::{Error, ErrorCode, IoHandler, Params, Value};
use jsonrpc_http_server::{DomainsValidation, ServerBuilder};
use log; // Added log
use serde::{Deserialize, Serialize}; // Added for InitializeParams/Result

// Structs for the 'initialize' RPC method
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ClientInfo {
    name: String,
    version: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct InitializeParams {
    protocol_version: Option<String>,
    capabilities: serde_json::Value,
    client_info: Option<ClientInfo>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ToolCapabilities {
    list_changed: bool,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SearchCapabilities {
    enabled: bool,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct FetchCapabilities {
    enabled: bool,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ServerCapabilities {
    tools: ToolCapabilities,
    search: SearchCapabilities,
    fetch: FetchCapabilities,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct InitializeResult {
    capabilities: ServerCapabilities,
}

#[derive(Debug)]
pub struct WordIndex {
    pub lines: Vec<String>,
    pub index: HashMap<String, Vec<usize>>,
}

impl WordIndex {
    pub fn new(filename: &str) -> Result<Self, std::io::Error> {
        log::debug!("WordIndex::new called with filename: {}", filename);
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
        log::debug!("WordIndex::search called with query: '{}'", query);
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

        log::trace!("Parsed query_words: {:?}", query_words);

        if query_words.is_empty() {
            log::debug!("Empty query_words, returning empty results.");
            return Vec::new();
        }

        let mut result_line_nums: Option<HashSet<usize>> = None;

        for word in query_words {
            log::trace!("Processing word: '{}'", word);
            if let Some(line_nums_for_word) = self.index.get(&word) {
                log::trace!("Found line numbers for '{}': {:?}", word, line_nums_for_word);
                let current_word_set: HashSet<usize> =
                    line_nums_for_word.iter().cloned().collect();
                if let Some(ref mut existing_set) = result_line_nums {
                    existing_set.retain(|line_num| current_word_set.contains(line_num));
                    log::trace!("Retained line numbers: {:?}", existing_set);
                } else {
                    result_line_nums = Some(current_word_set);
                    log::trace!("Initialized result_line_nums with: {:?}", result_line_nums);
                }
            } else {
                log::debug!("Word '{}' not found in index, returning empty results.", word);
                return Vec::new();
            }
        }

        if let Some(final_set) = result_line_nums {
            let mut sorted_results: Vec<usize> = final_set.into_iter().collect();
            sorted_results.sort_unstable();
            log::debug!("Search successful, returning results: {:?}", sorted_results);
            sorted_results
        } else {
            log::debug!("No results found after processing all words.");
            Vec::new()
        }
    }

    pub fn fetch(&self, line_number: usize) -> Option<String> {
        log::debug!("WordIndex::fetch called with line_number: {}", line_number);
        if line_number < self.lines.len() {
            let line = self.lines[line_number].clone();
            log::trace!("Fetched line for number {}: '{}'", line_number, line);
            Some(line)
        } else {
            log::debug!("Line number {} out of bounds (lines.len() is {}).", line_number, self.lines.len());
            None
        }
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long, value_delimiter = ',', help = "IP:PORT addresses to listen on (comma-separated)")]
    addresses: Vec<String>,
    #[clap(short, long, action = clap::ArgAction::Count, help = "Enable verbose logging. Use -vv for more verbose output.")]
    verbose: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Initialize logger based on verbose level
    match cli.verbose {
        0 => std::env::set_var("RUST_LOG", "info"),
        1 => std::env::set_var("RUST_LOG", "debug"),
        _ => std::env::set_var("RUST_LOG", "trace"),
    }
    env_logger::init();

    log::info!("Verbose level: {}", cli.verbose); // Replaced println with log::info

    if cli.addresses.is_empty() {
        log::error!("Error: No addresses provided. Please specify at least one address using --addresses ip:port."); // Replaced eprintln with log::error
        std::process::exit(1);
    }

    log::info!("Loading database from db.txt..."); // Replaced println with log::info
    let word_index = match WordIndex::new("db.txt") {
        Ok(wi) => Arc::new(wi),
        Err(e) => {
            log::error!("Failed to load db.txt: {}", e); // Replaced eprintln with log::error
            std::process::exit(1);
        }
    };
    log::info!("Database loaded successfully."); // Replaced println with log::info

    let mut handler = IoHandler::new();

    // RPC "search" method
    let wi_search = Arc::clone(&word_index);
    handler.add_method("search", move |params: Params| {
        let wi = Arc::clone(&wi_search);
        async move {
            log::debug!("RPC 'search' method called with params: {:?}", params);
            match params.parse::<(String,)>() {
                Ok((query,)) => {
                    log::trace!("Parsed query for 'search': '{}'", query);
                    let results = wi.search(&query);
                    log::trace!("Results for 'search' query '{}': {:?}", query, results);
                    Ok(Value::Array(
                        results.into_iter().map(|n| Value::Number(n.into())).collect(),
                    ))
                }
                Err(e) => {
                    log::error!("Failed to parse params for 'search': {:?}", e);
                    Err(Error {
                        code: ErrorCode::InvalidParams,
                        message: "Invalid parameters: Expected a single string query.".into(),
                        data: None,
                    })
                }
            }
        }
    });

    // RPC "initialize" method
    handler.add_method("initialize", |params: Params| async move {
        log::debug!("RPC method 'initialize' called with params: {:?}", params);
        match params.parse::<InitializeParams>() {
            Ok(parsed_params) => {
                log::info!("Successfully parsed initialize parameters: {:?}", parsed_params);
                if let Some(client_info) = &parsed_params.client_info {
                    log::info!(
                        "Client name: {}, version: {:?}",
                        client_info.name,
                        client_info.version.as_deref().unwrap_or("N/A")
                    );
                }

                let result = InitializeResult {
                    capabilities: ServerCapabilities {
                        tools: ToolCapabilities { list_changed: true },
                        search: SearchCapabilities { enabled: true },
                        fetch: FetchCapabilities { enabled: true },
                    },
                };
                match serde_json::to_value(result) {
                    Ok(val) => Ok(val),
                    Err(e) => {
                        log::error!("Failed to serialize InitializeResult: {}", e);
                        Err(Error::internal_error())
                    }
                }
            }
            Err(e) => {
                log::error!("Failed to parse initialize parameters: {}", e);
                Err(Error {
                    code: ErrorCode::InvalidParams,
                    message: format!("Invalid parameters for initialize: {}", e),
                    data: None,
                })
            }
        }
    });

    // RPC "fetch" method
    let wi_fetch = Arc::clone(&word_index);
    handler.add_method("fetch", move |params: Params| {
        let wi = Arc::clone(&wi_fetch);
        async move {
            log::debug!("RPC 'fetch' method called with params: {:?}", params);
            match params.parse::<(usize,)>() {
                Ok((line_number,)) => {
                    log::trace!("Parsed line_number for 'fetch': {}", line_number);
                    match wi.fetch(line_number) {
                        Some(line) => {
                            log::trace!("Fetched line for 'fetch' line_number {}: '{}'", line_number, line);
                            Ok(Value::String(line))
                        }
                        None => {
                            log::warn!("Invalid record ID for 'fetch' line_number {}: Line number out of bounds.", line_number);
                            Err(Error {
                                code: ErrorCode::ServerError(-32001), // Custom error code
                                message: "Invalid record ID: Line number out of bounds.".into(),
                                data: None,
                            })
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to parse params for 'fetch': {:?}", e);
                    Err(Error {
                        code: ErrorCode::InvalidParams,
                        message: "Invalid parameters: Expected a single unsigned integer line number."
                            .into(),
                        data: None,
                    })
                }
            }
        }
    });

    let mut server_handles = Vec::new();

    for addr_str in cli.addresses {
        log::info!("Attempting to start server on {}...", addr_str); // Replaced println with log::info
        match addr_str.parse::<std::net::SocketAddr>() {
            Ok(socket_addr) => {
                let server = ServerBuilder::new(handler.clone()) // Clone handler for each server
                    .cors(DomainsValidation::Disabled)
                    .start_http(&socket_addr);

                match server {
                    Ok(s) => {
                        log::info!("Server listening on http://{}", socket_addr); // Replaced println with log::info
                        server_handles.push(s); // Store the server handle (optional for just waiting)
                    }
                    Err(e) => {
                        log::error!("Failed to start server on {}: {:?}", socket_addr, e); // Replaced eprintln with log::error
                    }
                }
            }
            Err(e) => {
                log::error!("Invalid address format '{}': {}", addr_str, e); // Replaced eprintln with log::error
            }
        }
    }

    if server_handles.is_empty() {
        log::error!("No servers were started successfully."); // Replaced eprintln with log::error
        return Ok(());
    }

    log::info!("Servers started. Press Ctrl+C to shut down."); // Replaced println with log::info
    tokio::signal::ctrl_c().await?;
    log::info!("Ctrl+C received, shutting down servers."); // Replaced println with log::info

    // Optional: explicitly close servers if needed, though dropping handles might be enough
    // for handle in server_handles {
    //     handle.close();
    // }

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    // use std::fs; // Removed unused import
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Helper function to create a WordIndex from test_db.txt
    fn word_index_from_test_db() -> WordIndex {
        // To ensure tests can run regardless of where `cargo test` is invoked,
        // we need to locate test_db.txt relative to the Cargo.toml of this package.
        // This assumes test_db.txt is in the root of the mcp_server package.
        // let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string()); // Removed unused variable
        // let db_path = std::path::Path::new(&manifest_dir).join("../test_db.txt");  // Removed unused variable
        // The previous step created test_db.txt in mcp_server/test_db.txt, but the server itself is in mcp_server.
        // So from within mcp_server (where Cargo.toml is), test_db.txt is just "test_db.txt"
        // However, the original instruction was "mcp_server/test_db.txt".
        // Let's assume the `WordIndex::new` is called from the root of the mcp_server crate.
        // The test file `test_db.txt` was created in `mcp_server/test_db.txt`.
        // So the path for the tests should be just "test_db.txt" if tests are run from `mcp_server` directory.
        // Or it could be `../mcp_server/test_db.txt` if tests are run from `/app`
        // Let's try to be robust by checking CARGO_MANIFEST_DIR
        // The file was created at "mcp_server/test_db.txt".
        // If CARGO_MANIFEST_DIR is /app/mcp_server, then path is "test_db.txt"
        // If running from /app, then path is "mcp_server/test_db.txt"
        // The original code used "test_db.txt" directly. Let's stick to that for the helper.
        // The tests will be run from the `mcp_server` crate root.
        WordIndex::new("test_db.txt").expect("Failed to load test_db.txt for testing. Ensure it is in the mcp_server directory.")
    }

    #[test]
    fn test_word_index_new_success() {
        let wi = word_index_from_test_db();
        assert!(!wi.lines.is_empty(), "Lines should not be empty after loading test_db.txt");
        assert!(!wi.index.is_empty(), "Index should not be empty after loading test_db.txt");
    }

    #[test]
    fn test_word_index_new_file_not_found() {
        match WordIndex::new("non_existent_file.txt") {
            Ok(_) => panic!("Should have failed for a non-existent file"),
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound),
        }
    }

    #[test]
    fn test_word_index_new_empty_file() {
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        // writeln!(temp_file, "").expect("Failed to write to temp file"); // Write an empty line to avoid EOF error on read_line
        // The original WordIndex::new reads lines and pushes them. An empty file will result in an empty lines vector.
        // If the file has one empty line, lines vector will have one empty string.

        // Test with a file that has one empty line
        writeln!(temp_file, "").expect("Failed to write one empty line to temp file");
        let wi_one_empty_line = WordIndex::new(temp_file.path().to_str().unwrap())
            .expect("Failed to load file with one empty line");
        assert_eq!(wi_one_empty_line.lines.len(), 1, "Should have one line for a file with one empty line");
        assert!(wi_one_empty_line.lines[0].is_empty(), "The first line should be empty");
        assert!(wi_one_empty_line.index.is_empty(), "Index should be empty if only an empty line exists");

        // Test with a truly empty file (0 bytes)
        let temp_file_truly_empty = NamedTempFile::new().expect("Failed to create truly empty temp file");
        // Do not write anything to make it truly empty
        let wi_truly_empty = WordIndex::new(temp_file_truly_empty.path().to_str().unwrap())
            .expect("Failed to load a truly empty file");
        assert!(wi_truly_empty.lines.is_empty(), "Lines should be empty for a truly empty file");
        assert!(wi_truly_empty.index.is_empty(), "Index should be empty for a truly empty file");
    }

    #[test]
    fn test_search_single_word_exists() {
        let wi = word_index_from_test_db();
        let results = wi.search("hello");
        assert_eq!(results, vec![0]);
    }

    #[test]
    fn test_search_multiple_words_same_line() {
        let wi = word_index_from_test_db();
        let results = wi.search("test line");
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn test_search_multiple_words_different_lines() {
        let wi = word_index_from_test_db();
        let results = wi.search("hello test"); // "hello" is line 0, "test" is line 1
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_word_not_exists() {
        let wi = word_index_from_test_db();
        let results = wi.search("nonexistentword");
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_empty_query() {
        let wi = word_index_from_test_db();
        let results = wi.search(" "); // space only
        assert!(results.is_empty());
        let results_empty = wi.search(""); // truly empty
        assert!(results_empty.is_empty());
    }

    #[test]
    fn test_search_mixed_casing() {
        let wi = word_index_from_test_db();
        let results = wi.search("UPPERCASE"); // db has "UPPERCASE"
        assert_eq!(results, vec![3]);
        let results_lower = wi.search("uppercase"); // query lowercase
        assert_eq!(results_lower, vec![3]);
         let results_in_db_lower = wi.search("LoWeRcAsE"); // db has "lowercase"
        assert_eq!(results_in_db_lower, vec![3]);
    }

    #[test]
    fn test_search_with_punctuation() {
        let wi = word_index_from_test_db();
        let results = wi.search("world!"); // Query with punctuation
        assert_eq!(results, vec![0]);
        let results_comma = wi.search("comma,");
        assert_eq!(results_comma, vec![4]);
        let results_period = wi.search("period.");
        assert_eq!(results_period, vec![4]);
    }

    #[test]
    fn test_search_numbers() {
        let wi = word_index_from_test_db();
        let results = wi.search("123");
        assert_eq!(results, vec![5]);
        let results_multi = wi.search("123 numbers");
        assert_eq!(results_multi, vec![5]);
    }

    #[test]
    fn test_search_line_after_empty_line() {
        let wi = word_index_from_test_db();
        // The file has:
        // An empty line follows this one. (line 6)
        //                                (line 7 - empty)
        // A line after an empty line.    (line 8)
        let results = wi.search("after empty line");
        assert_eq!(results, vec![8]);
    }

    #[test]
    fn test_search_repeated_words() {
        let wi = word_index_from_test_db();
        let results = wi.search("repeated");
        assert_eq!(results, vec![9]); // "repeated repeated words."
        let results_double = wi.search("repeated repeated");
        assert_eq!(results_double, vec![9]);
    }

    #[test]
    fn test_fetch_existing_line() {
        let wi = word_index_from_test_db();
        let line = wi.fetch(0);
        assert_eq!(line, Some("Hello world!".to_string()));
        let line_2 = wi.fetch(8);
        assert_eq!(line_2, Some("A line after an empty line.".to_string()));
    }

    #[test]
    fn test_fetch_out_of_bounds() {
        let wi = word_index_from_test_db();
        let line = wi.fetch(100); // test_db.txt has 10 lines (0-9)
        assert_eq!(line, None);
    }

    #[test]
    fn test_fetch_line_is_empty() {
        let wi = word_index_from_test_db();
        // test_db.txt line 7 is empty
        let line = wi.fetch(7);
        assert_eq!(line, Some("".to_string()));
    }

    #[test]
    fn test_rpc_initialize_method_success() {
        let mut handler = IoHandler::new();

        // Register the initialize method (copied and adapted from main.rs)
        handler.add_method("initialize", |params: Params| async move {
            // Using println! for logs in test if logger is not setup for test environment
            // println!("RPC method 'initialize' called with params: {:?}", params);
            match params.parse::<InitializeParams>() {
                Ok(parsed_params) => {
                    // println!("Successfully parsed initialize parameters: {:?}", parsed_params);
                    if let Some(client_info) = &parsed_params.client_info {
                        // println!(
                        //     "Client name: {}, version: {:?}",
                        //     client_info.name,
                        //     client_info.version.as_deref().unwrap_or("N/A")
                        // );
                    }
                    let result = InitializeResult {
                        capabilities: ServerCapabilities {
                            tools: ToolCapabilities { list_changed: true },
                            search: SearchCapabilities { enabled: true },
                            fetch: FetchCapabilities { enabled: true },
                        },
                    };
                    match serde_json::to_value(result) {
                        Ok(val) => Ok(val),
                        Err(_e) => Err(Error::internal_error()), // Simplified error for test
                    }
                }
                Err(e) => Err(Error {
                    code: ErrorCode::InvalidParams,
                    message: format!("Invalid parameters for initialize: {}", e),
                    data: None,
                }),
            }
        });

        let request_json = r#"{
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": "1.0",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-vscode-client",
                    "version": "0.0.1"
                }
            },
            "id": 123
        }"#;

        let response_str_opt = handler.handle_request_sync(request_json);
        assert!(response_str_opt.is_some(), "Handler should produce a response");

        let response_str = response_str_opt.unwrap();
        // println!("Response: {}", response_str); // For debugging the test

        let response_json: serde_json::Value = serde_json::from_str(&response_str)
            .expect("Response should be valid JSON");

        assert_eq!(response_json["jsonrpc"], "2.0");
        assert_eq!(response_json["id"], 123);
        assert!(response_json["error"].is_null(), "Response should not have an error part. Error: {}", response_json["error"]);

        let result = response_json.get("result").expect("Response should have a result part");
        assert!(result.is_object(), "Result should be an object");

        let capabilities = result.get("capabilities").expect("Result should have capabilities");
        assert!(capabilities.is_object(), "Capabilities should be an object");

        // Check for new capabilities
        let tools_cap = capabilities.get("tools").expect("Capabilities should have tools");
        assert_eq!(tools_cap.get("listChanged").expect("Tools should have listChanged").as_bool().unwrap(), true);

        let search_cap = capabilities.get("search").expect("Capabilities should have search");
        assert_eq!(search_cap.get("enabled").expect("Search should have enabled").as_bool().unwrap(), true);

        let fetch_cap = capabilities.get("fetch").expect("Capabilities should have fetch");
        assert_eq!(fetch_cap.get("enabled").expect("Fetch should have enabled").as_bool().unwrap(), true);
    }

    #[test]
    fn test_rpc_initialize_method_invalid_params() {
        let mut handler = IoHandler::new();
        // Register initialize method (same as above)
        handler.add_method("initialize", |params: Params| async move {
            match params.parse::<InitializeParams>() {
                Ok(_parsed_params) => {
                    let result = InitializeResult {
                        capabilities: ServerCapabilities {
                            tools: ToolCapabilities { list_changed: false },
                            search: SearchCapabilities { enabled: true },
                            fetch: FetchCapabilities { enabled: true },
                        },
                    };
                    match serde_json::to_value(result) {
                        Ok(val) => Ok(val),
                        Err(_e) => Err(Error::internal_error()),
                    }
                }
                Err(e) => Err(Error {
                    code: ErrorCode::InvalidParams,
                    message: format!("Invalid parameters for initialize: {}", e),
                    data: None,
                }),
            }
        });

        // Sending params as an array, which is invalid for InitializeParams struct
        let request_json_invalid = r#"{
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": ["param1", "param2"],
            "id": 456
        }"#;

        let response_str_opt = handler.handle_request_sync(request_json_invalid);
        assert!(response_str_opt.is_some(), "Handler should produce a response for invalid params");

        let response_str = response_str_opt.unwrap();
        let response_json: serde_json::Value = serde_json::from_str(&response_str)
            .expect("Response should be valid JSON");

        assert_eq!(response_json["jsonrpc"], "2.0");
        assert_eq!(response_json["id"], 456);
        assert!(response_json["result"].is_null(), "Response should not have a result part for an error");

        let error = response_json.get("error").expect("Response should have an error part");
        assert_eq!(error["code"], ErrorCode::InvalidParams.code());
        assert!(error["message"].as_str().unwrap().contains("Invalid parameters"));
    }
}
