# Rust DB: A simple database

This is a simple database implementation in Rust which is a work in progress.
Its purpose is for me to to become proficient in Rust.

When complete, it will have two main capabilities:
- Generating [TPC-H data](https://github.com/electrum/tpch-dbgen) with arbitrary scales and arbitrary block sizes.
- Receiving and processing SQL queries that can be handled in parallel.

Under the hood, this will be divided into four files:
- *write.rs*: For generating data and putting it into parquet files.  This is currently partially implemented.
- *query.rs*: Implements the interface for sending queries.  Not yet implemented.
- *index.rs*: Given a SQL query, indexes the relevant files.  Not yet implemented.
- *parse.rs*: Parses a SQL query so that index.rs knows how to read it.  Not yet implemented.
