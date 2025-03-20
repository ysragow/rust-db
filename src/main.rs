use std::io;
mod query;
mod write;

fn main() {
    println!("Welcome to a simple database!");
    println!("What would you like to do?  Press 'g' to generate new data, or 'q' to query.");
    let mut input = String::new();
    loop {
        input.clear();
        io::stdin().read_line(&mut input).expect("Failed to read line.");
        match input.as_str() {
            "g\n" => break,
            "q\n" => break,
            _ => println!("Please type either 'g' or 'q'"),
        };
    };
    let c = &input[0..1];
    println!("You typed '{c}'")

}
