use html_minifier::HTMLMinifier;
use std::fs::File;
use std::io::{Read, Write};

fn main() {
    // Minify the HTML file
    minify_html();

    // Inform Cargo about the additional files to include
    println!("cargo:rerun-if-changed=assets/index.html");
}

fn minify_html() {
    // Set the path to the HTML file you want to minify
    let html_file_path = "assets/index.html";

    // Read the HTML file
    let mut file = File::open(html_file_path).expect("Failed to open HTML file");
    let mut html_content = Vec::new();
    file.read_to_end(&mut html_content)
        .expect("Failed to read HTML file");

    // Create the HTMLMinifier instance
    let mut html_minifier = HTMLMinifier::new();

    // Minify the HTML content
    html_minifier
        .digest(&html_content)
        .expect("Failed to minify HTML");

    // Get the minified HTML content
    let minified_content = html_minifier.get_html();

    // Write the minified content back to the file
    let mut output_file = File::create(html_file_path).expect("Failed to create output file");
    output_file
        .write_all(&minified_content)
        .expect("Failed to write minified content to file");
}
