use arrow::array::Array;
use arrow::array::{RecordBatch, StringArray, Date32Array, Int32Array, Float32Array};
use arrow::datatypes::Schema;
use chrono;
use chrono::Datelike;
use failure::ensure;
use std::convert::From;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{File, remove_file};
use std::io::{BufReader, BufRead};
use parquet::arrow::ArrowWriter; 
use std::path::Path;
use std::process::Command;
use std::result::Result;
use std::str;
use std::num::{ParseFloatError, ParseIntError};
use std::sync::Arc;
use crate::query;


const BLOCK_SIZE: u32 = 1048576;
const EPOCH_DAY: i32 = 719163;


enum WriteError<'a> {
    // BadOutput: Output could not be read
    BadOutput,

    // FileNotFound: name of file in FILES which was not found
    FileNotFound(&'a str, String),

    // Failed: Return error output
    Failed(String),

    // ReadFailed: Failed to read a file
    ReadFailed(String),

    // BadColumn: The column name was not recognized
    BadColumn(&'a str),

    // BadRow: This row was not formatted correctly
    BadRow(String),
}

#[derive(Debug)]
enum ParseError {
    FloatError(String),
    IntegerError(String),
    DateError(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let txt = "Cannot interpret";
        match self {
            ParseError::FloatError(dp) => write!(f, "{} {} {}", txt, dp, "as float."),
            ParseError::IntegerError(dp) => write!(f, "{} {} {}", txt, dp, "as integer."),
            ParseError::DateError(dp) => write!(f, "{} {} {}", txt, dp, "as date."),
        }
    }
}

impl From<ParseIntError> for ParseError {
    fn from(e: ParseIntError) -> Self {
        Self::IntegerError(e.to_string())
    }
}

impl From<ParseFloatError> for ParseError {
    fn from(e: ParseFloatError) -> Self {
        Self::FloatError(e.to_string())
    }
}

impl From<chrono::ParseError> for ParseError {
    fn from(e: chrono::ParseError) -> Self {
        Self::DateError(e.to_string())
    }
}

enum DPType {
    Int,
    Dec,
    Date,
    Char(u16),
}

enum Column {
    IntCol(Vec<i32>),
    DecCol(Vec<f32>),
    DatCol(Vec<i32>),
    ChaCol(Vec<String>, u16),
}


const FILES: [&str; 8] = [
    "customer",
    "lineitem",
    "orders",
    "nation",
    "part",
    "partsupp",
    "region",
    "supplier",
];


const COLUMNS: [&[&str]; 8] = [
    &[
         "C_CUSTKEY INTEGER",
         "C_NAME VARCHAR(25)",
         "C_ADDRESS VARCHAR(40)",
         "C_NATIONKEY INTEGER",
         "C_PHONE CHAR(15)",
         "C_ACCTBAL DECIMAL(15,2)",
         "C_MKTSEGMENT CHAR(10)",
         "C_COMMENT VARCHAR(117)",
    ],
    &["L_ORDERKEY INTEGER",
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
    &["O_ORDERKEY INTEGER",
         "O_CUSTKEY INTEGER",
         "O_ORDERSTATUS CHAR(1)",
         "O_TOTALPRICE DECIMAL(15,2)",
         "O_ORDERDATE DATE",
         "O_ORDERPRIORITY CHAR(15)",
         "O_CLERK CHAR(15)",
         "O_SHIPPRIORITY INTEGER",
         "O_COMMENT VARCHAR(79)",
    ],
    &["N_NATIONKEY INTEGER",
         "N_NAME CHAR(25)",
         "N_REGIONKEY INTEGER",
         "N_COMMENT VARCHAR(152)",
    ],
    &["P_PARTKEY INTEGER",
         "P_NAME VARCHAR(55)",
         "P_MFGR CHAR(25)",
         "P_BRAND CHAR(10)",
         "P_TYPE VARCHAR(25)",
         "P_SIZE INTEGER",
         "P_CONTAINER CHAR(10)",
         "P_RETAILPRICE DECIMAL(15,2)",
         "P_COMMENT VARCHAR(23)",
    ],
    &["PS_PARTKEY INTEGER",
         "PS_SUPPKEY INTEGER",
         "PS_AVAILQTY INTEGER",
         "PS_SUPPLYCOST DECIMAL(15,2)",
         "PS_COMMENT VARCHAR(199)",
    ],
    &["R_REGIONKEY INTEGER",
         "R_NAME CHAR(25)",
         "R_COMMENT VARCHAR(152)",
    ],
    &["S_SUPPKEY INTEGER",
         "S_NAME CHAR(25)",
         "S_ADDRESS VARCHAR(40)",
         "S_NATIONKEY INTEGER",
         "S_PHONE CHAR(15)",
         "S_ACCTBAL DECIMAL(15,2)",
         "S_COMMENT VARCHAR(101)",
    ],

];

    
pub fn generate(scale: &str) -> std::io::Result<()> {
    println!("Generating data with scale factor {scale}...\r");
    match dbgen(&scale) {
        Err(why) => panic_on_write_error(why),
        Ok(_) => {},
    }
    println!("Converting the data to parquet...\r");
    let n = FILES.len();
    for i in 0..n {
        if let Err(why) = tbl_to_parquet(i) {
            panic_on_write_error(why);
        } else {
            remove_file(&("data/".to_string() + &FILES[i] + ".tbl"));
        }
    };
    Ok(())
    // let mut file = File::open("foo.txt")?;
    // let mut contents = String::new();
    // file.read_to_string(&mut contents)?;
}


fn panic_on_write_error(why: WriteError) {
    panic!("{}", match why {
        WriteError::Failed(output) => format!("Failed with the following output:\n{output}"),
        WriteError::FileNotFound(f, output) => format!("File {f} was not generated.  The following was output:\n{output}"),
        WriteError::BadOutput => format!("Output could not be interpreted"),
        WriteError::ReadFailed(f) => format!("Failed to read file {f}."),
        WriteError::BadColumn(col) => format!("Badly formatted column: {col}"),
        WriteError::BadRow(row) => format!("Badly formatted row: {row}"),
    });
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
    let std_out = output.stderr;
    for FILE in FILES {
        match File::open("data/".to_owned() + &FILE + ".tbl") {
            Err(_) => return match str::from_utf8(&std_out) {
                Err(_) => Err(WriteError::BadOutput),
                Ok(outstr) => Err(WriteError::FileNotFound(&FILE, outstr.to_string())),
            },
            Ok(_) => {},
        };
    }
    Ok(())
}


fn tbl_to_parquet<'a>(input_index: usize) -> Result<(), WriteError<'a>> {
    let file_name = "data/".to_owned() + FILES[input_index];
    println!("Converting file {file_name}, at index {input_index}.");
    let column_heads = COLUMNS[input_index];
    let n: usize = column_heads.len();
    let tbl_name = file_name.to_string() + ".tbl";
    let file = match File::open(&tbl_name) {
        Ok(f) => f,
        Err(_) => return Err(WriteError::FileNotFound(FILES[input_index], "(no output)".to_string())),
    };
    let mut tuple_iter = BufReader::new(file).lines();
    let mut finished = false;
    let mut block_count = 0;
    // Process:
    // - Add to vectors until block size is surpassed, or until no more tuples 
    // - Write parquet file.  If more tuples, then continue again with a new file.
    while !finished {
        // Initialize the output which will become the record batch
        let mut output: Vec<(String, Column)> = Vec::with_capacity(n);
        for i in 0..n {
            let col = column_heads[i as usize];
            let mut split = col.split(' ');
            let name = match split.next() {
                Some(x) => x,
                None => return Err(WriteError::BadColumn(&col)),
            };
            let type_str = match split.next() {
                Some(x) => x,
                None => return Err(WriteError::BadColumn(&col)),
            };
            match split.next() {
                Some(_) => return Err(WriteError::BadColumn(&col)),
                None => {},
            };
            let dptype = match {match &type_str[0..3] {
                "CHA" => get_size(type_str, false),
                "VAR" => get_size(type_str, true),
                "DAT" => Ok(DPType::Date),
                "INT" => Ok(DPType::Int),
                "DEC" => Ok(DPType::Dec),
                _ => return Err(WriteError::BadColumn(&col)),
            }}{
                Ok(t) => t,
                Err(_) => return Err(WriteError::BadColumn(&col)),
            };
            output.push((name.to_owned(), match dptype {
                DPType::Date => Column::DatCol(Vec::<i32>::new()),
                DPType::Char(size) => Column::ChaCol(Vec::<String>::new(), size),
                DPType::Int => Column::IntCol(Vec::<i32>::new()),
                DPType::Dec => Column::DecCol(Vec::<f32>::new()),
            }));   
        }
        let mut row_count = 0;
        while row_count < BLOCK_SIZE {
            row_count = row_count + 1;
            let mut tuple_wrapped = tuple_iter.next();
            let tuple = match tuple_wrapped {
                Some(t1) => match t1 {
                    Err(_) => {
                        let line_num = (block_count * BLOCK_SIZE) + row_count;
                        return Err(WriteError::ReadFailed(format!("line {line_num} of file {file_name}")))
                    },
                    Ok(t2) => t2,
                },
                None => {
                    finished = true;
                    break;
                }
            };
            let tup_len = tuple.len();
            // println!("Reading line {row_count}, of length {tup_len}");
            let tuple_iter = tuple.split("|");
            for (i, datapoint) in tuple_iter.enumerate() {
                if i >= output.len() {
                    break;
                }
                match parse_and_push(&mut output[i as usize].1, datapoint.trim()) {
                    Ok(dp) => dp,
                    Err(_) => return Err(WriteError::BadRow(tuple.to_string())),
                };
            }
        }
        println!("Row Count: {row_count}");
        // Write the parquet file here
        let mut arc_output = Vec::<(String, Arc<(dyn Array)>)>::new();
        for (name, column) in output {
            match column {
                Column::ChaCol(v, size) => {
                    let new_array = StringArray::from_iter_values(v);
                    arc_output.push((name.to_owned(), Arc::new(new_array)));
                },
                Column::DatCol(v) => {
                    let new_array = Date32Array::from_iter_values(v);
                    arc_output.push((name.to_owned(), Arc::new(new_array)));
                },
                Column::IntCol(v) => {
                    let new_array = Int32Array::from_iter_values(v);
                    arc_output.push((name.to_owned(), Arc::new(new_array)));
                },
                Column::DecCol(v) => {
                    let new_array = Float32Array::from_iter_values(v);
                    arc_output.push((name.to_owned(), Arc::new(new_array)));
                },
            };
        }
        let batch = RecordBatch::try_from_iter(arc_output).expect("Record batch failed");
        let mut pq_name = file_name.to_string() + &block_count.to_string() + ".parquet";

        println!("Writing to file {pq_name}...");
        let file = match File::create_new(&pq_name) {
            Ok(f) => f,
            Err(_) => {
                remove_file(&pq_name);
                File::create_new(&pq_name).expect("It should work this time...")
            },
        };
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();
        block_count = block_count + 1;
    }
    Ok(()) 
}


fn get_size(type_str: &str, is_varchar: bool) -> Result<DPType, ParseIntError> {
    let bottom = if is_varchar {8} else {5};
    let top = {type_str.len() - 1} as usize;
    let size_str = &type_str[bottom..top];
    Ok(DPType::Char(size_str.parse()?))
}


fn parse_and_push(col: &mut Column, dp: &str) -> Result<(), ParseError> {
    // This is a separate function primarily for readability purposes
    // And so that the question mark notation can be used instead of writing 4 separate match statements
    // Ensure that dp has been trimmed already before passing it into this function
    match col {
        Column::ChaCol(v, size) => {
            let mut parsed = String::with_capacity(size.to_owned() as usize);
            parsed.push_str(dp);
            v.push(parsed);
        },
        Column::DatCol(v) => {
            let n_date: chrono::NaiveDate = dp.parse()?;
            let parsed: i32 = n_date.num_days_from_ce() - EPOCH_DAY;
            v.push(parsed);
        },
        Column::IntCol(v) => {
            let parsed: i32 = dp.parse()?;
            v.push(parsed);
        },
        Column::DecCol(v) => {
            let parsed: f32 = dp.parse()?;
            v.push(parsed);
        },
        // DPType::Char(c) => {
        //     let mut parsed = String::with_capacity(c.to_owned() as usize);
        //     let if Column::parsed.push_str(dp);
        //     DataPoint::Cha(parsed)
        // }
        // DPType::Date => {
        //     let parsed: chrono::NaiveDate = dp.parse()?;
        //     DataPoint::Dat(parsed)
        // },
        // DPType::Int => {
        //     let parsed: u32 = dp.parse()?;
        //     DataPoint::Int(parsed)
        // },
        // DPType::Dec => {
        //     let parsed: f32 = dp.parse()?;
        //     DataPoint::Dec(parsed)
        // },
    };
    Ok(())
}

