mod core; // Import the core module

fn main() -> std::io::Result<()> {
    Ok(core::lsm::test::main()) // Call the main function from test.rs
}