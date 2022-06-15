//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

use std::{collections::BTreeMap, vec};

use chrono;

use chrono::Local;
use iotdb::client::remote::{Config, RpcSession};
use iotdb::client::{MeasurementSchema, Result, RowRecord, Session, Tablet, Value};
use iotdb::protocal::{TSCompressionType, TSDataType, TSEncoding};
use prettytable::{cell, Row, Table};
use structopt::StructOpt;

fn main() {
    run().expect("failed to run session_example.");
}

fn run() -> Result<()> {
    #[derive(StructOpt)]
    #[structopt(name = "session_example")]
    struct Opt {
        #[structopt(short = "h", long, default_value = "127.0.0.1")]
        host: String,

        #[structopt(short = "P", long, default_value = "6667")]
        port: i32,

        #[structopt(short = "u", long, default_value = "root")]
        user: String,

        #[structopt(short = "p", long, default_value = "root")]
        password: String,

        #[structopt(short = "c", long)]
        clean: bool,
    }

    let opt = Opt::from_args();
    let config = Config {
        host: opt.host,
        port: opt.port,
        username: opt.user,
        password: opt.password,
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
    if opt.clean {
        session
            .delete_storage_group("root.sg_rs")
            .unwrap_or_default();
    }

    //create_timeseries
    {
        session.create_timeseries(
            "root.sg_rs.dev2.status",
            TSDataType::Float,
            TSEncoding::Plain,
            TSCompressionType::SNAPPY,
            Some(BTreeMap::from([
                ("prop1".to_owned(), "1".to_owned()),
                ("prop2".to_owned(), "2".to_owned()),
            ])),
            Some(BTreeMap::from([
                ("attr1".to_owned(), "1".to_owned()),
                ("attr2".to_owned(), "2".to_owned()),
            ])),
            Some(BTreeMap::from([
                ("tag1".to_owned(), "1".to_owned()),
                ("tag2".to_owned(), "2".to_owned()),
            ])),
            Some("stats".to_string()),
        )?;
        session.delete_timeseries(vec!["root.sg_rs.dev2.status"])?;
    }

    //create_multi_timeseries
    {
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
    }

    //insert_record
    {
        session.insert_record(
            "root.sg_rs.dev5",
            vec!["online", "desc"],
            vec![Value::Bool(false), Value::Text("F4145".to_string())],
            Local::now().timestamp_millis(),
            false,
        )?;
        session.delete_timeseries(vec!["root.sg_rs.dev5.online", "root.sg_rs.dev5.desc"])?;
    }

    //insert_string_record
    {
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
    }

    //insert_records
    {
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
                Value::Int32(i32::MAX),
                Value::Int64(1639704010752),
                Value::Double(1988.1),
                Value::Float(36.8),
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
    }

    //insert_records_of_one_device
    {
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
    }

    //tablet
    let mut ts = Local::now().timestamp_millis();
    let mut tablet1 = create_tablet(5, ts);

    ts += 5;
    let mut tablet2 = create_tablet(10, ts);

    ts += 10;
    let mut tablet3 = create_tablet(2, ts);

    //insert_tablet
    tablet1.sort();
    session.insert_tablet(&tablet1)?;

    //insert_tablets
    {
        tablet2.sort();
        tablet3.sort();
        session.insert_tablets(vec![&tablet2, &tablet3])?;
    }

    //delete_data
    session.delete_data(vec!["root.sg_rs.dev1.status"], 1, 16)?;

    //execute_query_statement
    {
        let dataset = session.execute_query_statement("select * from root.sg_rs.device2", None)?;
        // Get columns, column types and values from the dataset
        // For example:
        let width = 18;
        let column_count = dataset.get_column_names().len();
        let print_line_sep =
            || println!("{:=<width$}", '=', width = (width + 1) * column_count + 1);

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
    }

    //execute_statement
    {
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
    }

    //execute_batch_statement
    {
        session.execute_batch_statement(vec![
            "insert into root.sg_rs.dev6(time,s5) values(1,true)",
            "insert into root.sg_rs.dev6(time,s5) values(2,true)",
            "insert into root.sg_rs.dev6(time,s5) values(3,true)",
        ])?;
        session.delete_timeseries(vec!["root.sg_rs.dev6"])?;
    }
    //execute_raw_data_query
    {
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
    }

    //execute_update_statement
    {
        if let Some(dataset) =
            session.execute_update_statement("delete timeseries root.sg_rs.dev0.*")?
        {
            dataset.for_each(|r| println!("timestamp: {} {:?}", r.timestamp, r.values));
        }
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
