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

use std::error::Error;
use std::vec;

use chrono;

use chrono::Local;
use iotdb_client_rs::client::remote::{Config, RpcSession};
use iotdb_client_rs::client::{DataSet, MeasurementSchema, Session, Tablet, Value};
use iotdb_client_rs::protocal::{TSCompressionType, TSDataType, TSEncoding};
use prettytable::{cell, Cell, Row, Table};

fn print_dataset(dataset: Box<dyn DataSet>) -> Result<(), Box<dyn Error>> {
    let mut table = Table::new();

    let mut title_cells: Vec<Cell> = Vec::new();

    let is_ignore_timestamp = dataset.is_ignore_timestamp();
    if !is_ignore_timestamp {
        title_cells.push(cell!("Time"));
    }

    dataset
        .get_column_names()
        .iter()
        .for_each(|name| title_cells.push(cell!(name)));

    table.set_titles(Row::new(title_cells));

    dataset.for_each(|record| {
        let mut row_cells: Vec<Cell> = Vec::new();
        if !is_ignore_timestamp {
            row_cells.push(cell!(record.timestamp.to_string()));
        }

        record
            .values
            .iter()
            .for_each(|v| row_cells.push(cell!(v.to_string())));

        table.add_row(Row::new(row_cells));
    });
    table.printstd();
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config {
        host: String::from("127.0.0.1"),
        port: 6667,
        username: String::from("root"),
        password: String::from("root"),
        ..Default::default()
    };

    //rpc session
    let session = RpcSession::new(&config)?;
    run_example(session)?;

    //Local filesystem session
    // let session = DirectSession::new("/data/apache-iotdb-0.12.3-server-bin");
    // run_example(session)?;

    Ok(())
}

fn run_example<T: Session>(mut session: T) -> Result<(), Box<dyn Error>> {
    session.open()?;

    let tz = session.get_time_zone()?;
    if tz != "Asia/Shanghai" {
        session.set_time_zone("Asia/Shanghai")?;
    }

    session.set_storage_group("root.ln1")?;
    session.delete_storage_group("root.ln1")?;

    session.set_storage_group("root.ln1")?;
    session.set_storage_group("root.ln2")?;
    session.delete_storage_groups(vec!["root.ln1", "root.ln2"])?;

    //create_timeseries
    session.create_timeseries(
        "root.sg1.dev2.status",
        TSDataType::Float,
        TSEncoding::Plain,
        TSCompressionType::SNAPPY,
        None,
        None,
        None,
        None,
    )?;
    session.delete_timeseries(vec!["root.sg1.dev2.status"])?;

    //insert_record
    session.insert_record(
        "root.sg1.dev5",
        vec!["online", "desc"],
        vec![Value::Bool(false), Value::Text("F4145".to_string())],
        Local::now().timestamp_millis(),
        false,
    )?;
    session.delete_timeseries(vec!["root.sg1.dev5.online", "root.sg1.dev5.desc"])?;

    //insert_records
    session.insert_records(
        vec!["root.sg1.dev1"],
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
        "root.sg1.dev1.restart_count",
        "root.sg1.dev1.tick_count",
        "root.sg1.dev1.price",
        "root.sg1.dev1.temperature",
        "root.sg1.dev1.description",
        "root.sg1.dev1.status",
    ])?;

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

    //delete_timeseries
    session.insert_string_record(
        "root.ln.wf02.wt02",
        vec!["id", "location"],
        vec!["SN:001", "BeiJing"],
        Local::now().timestamp_millis(),
        false,
    )?;
    session.delete_timeseries(vec!["root.ln.wf02.wt02.id", "root.ln.wf02.wt02.location"])?;

    //insert_records_of_one_device
    session.insert_records_of_one_device(
        "root.sg1.dev0",
        vec![
            Local::now().timestamp_millis(),
            Local::now().timestamp_millis() + 1,
        ],
        vec![
            vec!["restart_count", "tick_count", "price"],
            vec!["temperature", "description", "status"],
        ],
        vec![
            vec![Value::Int32(1), Value::Int64(2018), Value::Double(1988.1)],
            vec![
                Value::Float(12.1),
                Value::Text("Test Device 1".to_string()),
                Value::Bool(false),
            ],
        ],
        false,
    )?;

    //tablet
    let mut ts = Local::now().timestamp_millis();

    let tablet1 = create_tablet(5, ts);
    ts += 5;

    let tablet2 = create_tablet(10, ts);
    ts += 10;

    let tablet3 = create_tablet(2, ts);

    session.insert_tablet(&tablet1, true)?;
    session.insert_tablets(vec![&tablet2, &tablet3], true)?;

    //delete_data
    session.insert_records_of_one_device(
        "root.sg1.dev1",
        vec![1, 16],
        vec![vec!["status"], vec!["status"]],
        vec![vec![Value::Bool(true)], vec![Value::Bool(true)]],
        true,
    )?;
    session.delete_data(vec!["root.sg1.dev1.status"], 1, 16)?;

    let dataset = session.execute_query_statement("select * from root.ln.device2", None)?;
    print_dataset(dataset)?;
    // dataset.for_each(|r| println!("timestamp: {} {:?}", r.timestamp, r.values));
    // let timestamps: Vec<i64> = dataset.map(|r| r.timestamp).collect();
    // let count = dataset.count();

    let ds = session.execute_statement("show timeseries", None)?;
    print_dataset(ds)?;

    session.execute_batch_statement(vec![
        "insert into root.sg1.dev6(time,s5) values(1,true)",
        "insert into root.sg1.dev6(time,s5) values(2,true)",
        "insert into root.sg1.dev6(time,s5) values(3,true)",
    ])?;

    let ds = session.execute_raw_data_query(
        vec![
            "root.ln.device2.restart_count",
            "root.ln.device2.tick_count",
            "root.ln.device2.description",
        ],
        0,
        i64::MAX,
    )?;
    print_dataset(ds)?;

    if let Some(dataset) = session.execute_update_statement("delete timeseries root.sg1.dev1.*")? {
        print_dataset(dataset)?;
    }

    session.close()?;

    Ok(())
}

fn create_tablet(row_count: i32, start_timestamp: i64) -> Tablet {
    let mut tablet = Tablet::new(
        "root.ln.device2",
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
        tablet.add_row(
            vec![
                Value::Bool(ts % 2 == 0),
                Value::Int32(row),
                Value::Int64(row as i64),
                Value::Float(row as f32 + 0.1),
                Value::Double(row as f64 + 0.2),
                Value::Text(format!("ts: {}", ts).to_string()),
            ],
            ts,
        );
    });
    tablet
}
