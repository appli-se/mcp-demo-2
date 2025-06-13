use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use jsonrpc_http_server::jsonrpc_core::{Error, ErrorCode, IoHandler, Params, Value};
use jsonrpc_http_server::{DomainsValidation, ServerBuilder};

#[derive(Debug)]
pub struct WordIndex {
    pub lines: Vec<String>,
    pub index: HashMap<String, Vec<usize>>,
}

impl WordIndex {
    pub fn new(filename: &str) -> Result<Self, std::io::Error> {
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
}
