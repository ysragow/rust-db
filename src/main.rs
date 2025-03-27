use std::io;
mod query;
mod write;
mod index;
mod parse;

fn main() {
    println!("Welcome to a simple database!");
    println!("What would you like to do?  Press 'g' to generate new data, or 'q' to query.");
    let mut input = String::new();
    loop {
        input.clear();
        io::stdin().read_line(&mut input).expect("Failed to read line.");
        match input.as_str() {
            "g\n" => {
                get_write();
                break
            },
            "q\n" => {
                get_query();
                break
            },
            _ => println!("Please type either 'g' or 'q'"),
        };
    };
}


fn get_write() {
    let mut input = String::new();
    println!("Please input a scale");
    loop {
        input.clear();
        io::stdin().read_line(&mut input).expect("Failed to read line.");
        // println!("{:?}", input.trim().parse::<f64>());
        match input.trim().parse::<f64>() {
             Err(_) => println!("Please input a floating point!"),
             Ok(_) => break,
        }
    }

    match write::generate(&input) {
        Err(_) => panic!("death"),
        Ok(_) => {},
    }
}


fn get_query() {}
