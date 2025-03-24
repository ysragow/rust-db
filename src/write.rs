// use parquet;
use std::fs::File;
use std::process::Command;
use std::io::Result;
use std::str;

const FILES: (&str, &str) = ("partsupp.tbl", "");

pub fn generate(scale: &str) -> Result<()> {
    println!("Generating data with scale factor {scale}...\r");
    match dbgen(&scale) {
        Err(_) => panic!("Failed to execute command"),
        Ok(stdout) => {
            match File::open("../dbgen/partsupp.tbl") {
                Err(_) => panic!("dbgen did not execute correctly.  The following is the output:\n{stdout}"),
                Ok(_) => {}
            }
        }
    }
    println!("Converting the data to parquet...\r");
    Ok(())
    // let mut file = File::open("foo.txt")?;
    // let mut contents = String::new();
    // file.read_to_string(&mut contents)?;



}

fn dbgen(scale: &str) -> Result<String> {
    let output = Command::new("dbgen/dbgen").output()?;
    let stdout = output.stderr;
    match str::from_utf8(&stdout) {
        Err(_) => panic!("Invalid Utf8 expression"),
        Ok(outstr) => Ok(outstr.to_string()),
    }
    // Command::new("../dbgen/dbgen")
    //     .arg("-fs")
    //     .arg(scale)
    //     .output()
}
