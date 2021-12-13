<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Apache IoTDB

[![Main Mac and Linux](https://github.com/apache/iotdb/actions/workflows/main-unix.yml/badge.svg)](https://github.com/apache/iotdb/actions/workflows/main-unix.yml)
[![Main Win](https://github.com/apache/iotdb/actions/workflows/main-win.yml/badge.svg)](https://github.com/apache/iotdb/actions/workflows/main-win.yml)
[![coveralls](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)
[![GitHub release](https://img.shields.io/github/release/apache/iotdb.svg)](https://github.com/apache/iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb.svg)
![](https://img.shields.io/github/downloads/apache/iotdb/total.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macox%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)


Apache IoTDB (Database for Internet of Things) is an IoT native database with high performance for
data management and analysis, deployable on the edge and the cloud. Due to its light-weight
architecture, high performance and rich feature set together with its deep integration with
Apache Hadoop, Spark and Flink, Apache IoTDB can meet the requirements of massive data storage,
high-speed data ingestion and complex data analysis in the IoT industrial fields.

# Apache IoTDB Client for Rust

## Overview

This is the Rust client of Apache IoTDB.

Apache IoTDB website: https://iotdb.apache.org
Apache IoTDB Github: https://github.com/apache/iotdb

## Prerequisites

apache-iotdb 0.12.0 and newer.</br>
rust 1.56.0 and newer.

## How to Use the Client (Quick Start)

## Usage

Put this in your `Cargo.toml`:

```toml
[dependencies]
iotdb-client-rs="0.3.4"
chrono="0.4.19"
prettytable-rs="0.8.0"
```
## Example

```rust
use std::vec;

use chrono;

use chrono::Local;
use iotdb_client_rs::client::remote::{Config, RpcSession};
use iotdb_client_rs::client::{MeasurementSchema, Result, RowRecord, Session, Tablet, Value};
use iotdb_client_rs::protocal::{TSCompressionType, TSDataType, TSEncoding};
use prettytable::{cell, Row, Table};

fn main() {
    run().expect("failed to run session_example.");
}

fn run() -> Result<()> {
    let config = Config {
        host: String::from("127.0.0.1"),
        port: 6667,
        username: String::from("root"),
        password: String::from("root"),
        ..Default::default()
    };
    let mut session = RpcSession::new(&config)?;
    session.open()?;

    //time_zone
    let tz = session.get_time_zone()?;
    if tz != "Asia/Shanghai" {
        session.set_time_zone("Asia/Shanghai")?;
    }

    //set_storage_group
    session.set_storage_group("root.ln1")?;
    session.delete_storage_group("root.ln1")?;

    //delete_storage_groups
    session.set_storage_group("root.ln1")?;
    session.set_storage_group("root.ln2")?;
    session.delete_storage_groups(vec!["root.ln1", "root.ln2"])?;

    //if storage group 'root.sg_rs' exist, remove it.
    // session
    //     .delete_storage_group("root.sg_rs")
    //     .unwrap_or_default();

    //create_timeseries
    session.create_timeseries(
        "root.sg_rs.dev2.status",
        TSDataType::Float,
        TSEncoding::Plain,
        TSCompressionType::SNAPPY,
        None,
        None,
        None,
        None,
    )?;
    session.delete_timeseries(vec!["root.sg_rs.dev2.status"])?;

    //create_multi_timeseries
    session.create_multi_timeseries(
        vec!["root.sg3.dev1.temperature", "root.sg3.dev1.desc"],
        vec![TSDataType::Float, TSDataType::Text],
        vec![TSEncoding::Plain, TSEncoding::Plain],
        vec![TSCompressionType::SNAPPY, TSCompressionType::SNAPPY],
        None,
        None,
        None,
        None,
    )?;
    session.delete_timeseries(vec!["root.sg3.dev1.temperature", "root.sg3.dev1.desc"])?;

    //insert_record
    session.insert_record(
        "root.sg_rs.dev5",
        vec!["online", "desc"],
        vec![Value::Bool(false), Value::Text("F4145".to_string())],
        Local::now().timestamp_millis(),
        false,
    )?;
    session.delete_timeseries(vec!["root.sg_rs.dev5.online", "root.sg_rs.dev5.desc"])?;

    //insert_string_record
    session.insert_string_record(
        "root.sg_rs.wf02.wt02",
        vec!["id", "location"],
        vec!["SN:001", "BeiJing"],
        Local::now().timestamp_millis(),
        false,
    )?;
    session.delete_timeseries(vec![
        "root.sg_rs.wf02.wt02.id",
        "root.sg_rs.wf02.wt02.location",
    ])?;

    //insert_records
    session.insert_records(
        vec!["root.sg_rs.dev1"],
        vec![vec![
            "restart_count",
            "tick_count",
            "price",
            "temperature",
            "description",
            "status",
        ]],
        vec![vec![
            Value::Int32(1),
            Value::Int64(2018),
            Value::Double(1988.1),
            Value::Float(12.1),
            Value::Text("Test Device 1".to_string()),
            Value::Bool(false),
        ]],
        vec![Local::now().timestamp_millis()],
    )?;
    session.delete_timeseries(vec![
        "root.sg_rs.dev1.restart_count",
        "root.sg_rs.dev1.tick_count",
        "root.sg_rs.dev1.price",
        "root.sg_rs.dev1.temperature",
        "root.sg_rs.dev1.description",
        "root.sg_rs.dev1.status",
    ])?;

    //insert_records_of_one_device
    session.insert_records_of_one_device(
        "root.sg_rs.dev0",
        vec![
            Local::now().timestamp_millis(),
            Local::now().timestamp_millis() - 1,
        ],
        vec![
            vec!["restart_count", "tick_count", "price"],
            vec!["temperature", "description", "status"],
        ],
        vec![
            vec![Value::Int32(1), Value::Int64(2018), Value::Double(1988.1)],
            vec![
                Value::Float(36.8),
                Value::Text("thermograph".to_string()),
                Value::Bool(false),
            ],
        ],
        false,
    )?;

    //table
    let mut ts = Local::now().timestamp_millis();
    let mut tablet1 = create_tablet(5, ts);
    tablet1.sort();
    ts += 5;
    let mut tablet2 = create_tablet(10, ts);
    ts += 10;
    let mut tablet3 = create_tablet(2, ts);
    tablet1.sort();

    //insert_tablet
    session.insert_tablet(&tablet1)?;
    tablet2.sort();
    tablet3.sort();

    //insert_tablets
    session.insert_tablets(vec![&tablet2, &tablet3])?;
    session.insert_records_of_one_device(
        "root.sg_rs.dev1",
        vec![1, 16],
        vec![vec!["status"], vec!["status"]],
        vec![vec![Value::Bool(true)], vec![Value::Bool(true)]],
        true,
    )?;

    //delete_data
    session.delete_data(vec!["root.sg_rs.dev1.status"], 1, 16)?;

    //execute_query_statement
    let dataset = session.execute_query_statement("select * from root.sg_rs.device2", None)?;
    // Get columns, column types and values from the dataset
    // For example:
    let width = 18;
    let column_count = dataset.get_column_names().len();
    let print_line_sep = || println!("{:=<width$}", '=', width = (width + 1) * column_count + 1);

    print_line_sep();
    dataset
        .get_column_names()
        .iter()
        .for_each(|c| print!("|{:>width$}", c.split('.').last().unwrap(), width = width));
    print!("|\n");
    print_line_sep();
    dataset.get_data_types().iter().for_each(|t| {
        let type_name = format!("{:?}", t);
        print!("|{:>width$}", type_name, width = width)
    });
    print!("|\n");
    print_line_sep();
    dataset.for_each(|r| {
        r.values.iter().for_each(|v| match v {
            Value::Bool(v) => print!("|{:>width$}", v, width = width),
            Value::Int32(v) => print!("|{:>width$}", v, width = width),
            Value::Int64(v) => print!("|{:>width$}", v, width = width),
            Value::Float(v) => print!("|{:>width$}", v, width = width),
            Value::Double(v) => print!("|{:>width$}", v, width = width),
            Value::Text(v) => print!("|{:>width$}", v, width = width),
            Value::Null => print!("|{:>width$}", "null", width = width),
        });
        print!("|\n");
    });
    print_line_sep();

    //execute_statement
    let dataset = session.execute_statement("show timeseries", None)?;
    let mut table = Table::new();
    table.set_titles(Row::new(
        dataset
            .get_column_names()
            .iter()
            .map(|c| cell!(c))
            .collect(),
    ));
    dataset.for_each(|r: RowRecord| {
        table.add_row(Row::new(
            r.values.iter().map(|v: &Value| cell!(v)).collect(),
        ));
    });
    table.printstd();

    //execute_batch_statement
    session.execute_batch_statement(vec![
        "insert into root.sg_rs.dev6(time,s5) values(1,true)",
        "insert into root.sg_rs.dev6(time,s5) values(2,true)",
        "insert into root.sg_rs.dev6(time,s5) values(3,true)",
    ])?;

    //execute_raw_data_query
    let dataset = session.execute_raw_data_query(
        vec![
            "root.sg_rs.device2.restart_count",
            "root.sg_rs.device2.tick_count",
            "root.sg_rs.device2.description",
        ],
        0,
        i64::MAX,
    )?;
    let mut table = Table::new();
    table.set_titles(Row::new(
        dataset
            .get_column_names()
            .iter()
            .map(|c| cell!(c))
            .collect(),
    ));
    dataset.for_each(|r: RowRecord| {
        table.add_row(Row::new(
            r.values.iter().map(|v: &Value| cell!(v)).collect(),
        ));
    });
    table.printstd();

    //execute_update_statement
    if let Some(dataset) =
        session.execute_update_statement("delete timeseries root.sg_rs.dev1.*")?
    {
        dataset.for_each(|r| println!("timestamp: {} {:?}", r.timestamp, r.values));
    }
    session.close()?;
    Ok(())
}

fn create_tablet(row_count: i32, start_timestamp: i64) -> Tablet {
    let mut tablet = Tablet::new(
        "root.sg_rs.device2",
        vec![
            MeasurementSchema::new(
                String::from("status"),
                TSDataType::Boolean,
                TSEncoding::Plain,
                TSCompressionType::SNAPPY,
                None,
            ),
            MeasurementSchema::new(
                String::from("restart_count"),
                TSDataType::Int32,
                TSEncoding::RLE,
                TSCompressionType::SNAPPY,
                None,
            ),
            MeasurementSchema::new(
                String::from("tick_count"),
                TSDataType::Int64,
                TSEncoding::RLE,
                TSCompressionType::SNAPPY,
                None,
            ),
            MeasurementSchema::new(
                String::from("temperature"),
                TSDataType::Float,
                TSEncoding::Plain,
                TSCompressionType::SNAPPY,
                None,
            ),
            MeasurementSchema::new(
                String::from("price"),
                TSDataType::Double,
                TSEncoding::Gorilla,
                TSCompressionType::SNAPPY,
                None,
            ),
            MeasurementSchema::new(
                String::from("description"),
                TSDataType::Text,
                TSEncoding::Plain,
                TSCompressionType::SNAPPY,
                None,
            ),
        ],
    );
    (0..row_count).for_each(|row| {
        let ts = start_timestamp + row as i64;
        tablet
            .add_row(
                vec![
                    Value::Bool(ts % 2 == 0),
                    Value::Int32(row),
                    Value::Int64(row as i64),
                    Value::Float(row as f32 + 0.1),
                    Value::Double(row as f64 + 0.2),
                    Value::Text(format!("ts: {}", ts).to_string()),
                ],
                ts,
            )
            .unwrap_or_else(|err| eprintln!("Add row failed, reason '{}'", err));
    });
    tablet
}
```
