use arrow_array;
use parquet;
use std::fs::File;
use std::process::Command;
use std::result::Result;
use std::str;
use std::env;
use std::path::Path;
use crate::query;


enum WriteError<'a> {
    // BadOutput: Output could not be read
    BadOutput,

    // FileNotFound: name of file in FILES which was not found
    FileNotFound(&'a str, String),

    // Failed: Return error output
    Failed(String),
}


const FILES: [&str; 8] = [
    "customer.tbl",
    "lineitem.tbl",
    "orders.tbl",
    "nation.tbl",
    "part.tbl",
    "partsupp.tbl",
    "region.tbl",
    "supplier.tbl",
];

const COLUMNS: [Vec<&str>; 8] = [
    vec![
         "C_CUSTKEY INTEGER",
         "C_NAME VARCHAR(25)",
         "C_ADDRESS VARCHAR(40)",
         "C_NATIONKEY INTEGER NOT NULL",
         "C_PHONE CHAR(15)",
         "C_ACCTBAL DECIMAL(15,2)",
         "C_MKTSEGMENT CHAR(10)",
         "C_COMMENT VARCHAR(117)",
    ],
    vec!["L_ORDERKEY INTEGER",
         "L_PARTKEY INTEGER",
         "L_SUPPKEY INTEGER",
         "L_LINENUMBER INTEGER",
         "L_QUANTITY DECIMAL(15,2)",
         "L_EXTENDEDPRICE DECIMAL(15,2)",
         "L_DISCOUNT DECIMAL(15,2)",
         "L_TAX DECIMAL(15,2)",
         "L_RETURNFLAG CHAR(1)",
         "L_LINESTATUS CHAR(1)",
         "L_SHIPDATE DATE",
         "L_COMMITDATE DATE",
         "L_RECEIPTDATE DATE",
         "L_SHIPINSTRUCT CHAR(25)",
         "L_SHIPMODE CHAR(10)",
         "L_COMMENT VARCHAR(44)",
    ],
    vec!["O_ORDERKEY INTEGER",
         "O_CUSTKEY INTEGER",
         "O_ORDERSTATUS CHAR(1)",
         "O_TOTALPRICE DECIMAL(15,2)",
         "O_ORDERDATE DATE",
         "O_ORDERPRIORITY CHAR(15)",
         "O_CLERK CHAR(15)",
         "O_SHIPPRIORITY INTEGER",
         "O_COMMENT VARCHAR(79)",
    ],
    vec!["N_NATIONKEY INTEGER",
         "N_NAME CHAR(25)",
         "N_REGIONKEY INTEGER",
         "N_COMMENT VARCHAR(152)",
    ],
    vec!["P_PARTKEY INTEGER",
         "P_NAME VARCHAR(55)",
         "P_MFGR CHAR(25)",
         "P_BRAND CHAR(10)",
         "P_TYPE VARCHAR(25)",
         "P_SIZE INTEGER",
         "P_CONTAINER CHAR(10)",
         "P_RETAILPRICE DECIMAL(15,2)",
         "P_COMMENT VARCHAR(23)",
    ],
    vec!["PS_PARTKEY INTEGER",
         "PS_SUPPKEY INTEGER",
         "PS_AVAILQTY INTEGER",
         "PS_SUPPLYCOST DECIMAL(15,2)",
         "PS_COMMENT VARCHAR(199)",
    ],
    vec!["R_REGIONKEY INTEGER",
         "R_NAME CHAR(25)",
         "R_COMMENT VARCHAR(152)",
    ],
    vec!["S_SUPPKEY INTEGER",
         "S_NAME CHAR(25)",
         "S_ADDRESS VARCHAR(40)",
         "S_NATIONKEY INTEGER",
         "S_PHONE CHAR(15)",
         "S_ACCTBAL DECIMAL(15,2)",
         "S_COMMENT VARCHAR(101)",
    ],

]

    
pub fn generate(scale: &str) -> std::io::Result<()> {
    println!("Generating data with scale factor {scale}...\r");
    match dbgen(&scale) {
        Err(why) => panic!("{}", match why {
            WriteError::Failed(output) => format!("Failed with the following output:\n{output}"),
            WriteError::FileNotFound(f, output) => format!("File {f} was not generated.  The following was output:\n{output}"),
            WriteError::BadOutput => format!("Output could not be interpreted"),
        }),
        Ok(_) => {},
    }
    println!("Converting the data to parquet...\r");
    Ok(())
    // let mut file = File::open("foo.txt")?;
    // let mut contents = String::new();
    // file.read_to_string(&mut contents)?;
}


fn dbgen(scale: &str) -> Result<(), WriteError> {
    
    // Change the directory so that dbgen executes in ./data
    let mut path = match env::current_dir() {
        Err(_) => return Err(WriteError::Failed("Could not get path".to_string())),
        Ok(x) => x
    };
    path.push("data");
    env::set_current_dir(path.as_path());
    
    // Execute dbgen
    let output = match Command::new("../dbgen/dbgen")
        .arg("-fs")
        .arg(scale)
        .output() {
        Err(_) => return Err(WriteError::Failed("Execution did not occur".to_string())),
        Ok(new_output) => new_output,
    };
    
    // Switch back to the prior directory
    env::set_current_dir(match path.parent() {Some(parent) => parent, None => panic!("what")});
    
    // If dbgen did not work, then return an error.
    let stdout = output.stderr;
    for FILE in FILES {
        match File::open("data/".to_owned() + &FILE) {
            Err(_) => return match str::from_utf8(&stdout) {
                Err(_) => Err(WriteError::BadOutput),
                Ok(outstr) => Err(WriteError::FileNotFound(&FILE, outstr.to_string())),
            },
            Ok(_) => {},
        };
    }
    Ok(())
}


//fn tbl_to_parquet() -> Result<(), write_error> {}
