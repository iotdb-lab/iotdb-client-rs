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

pub mod direct;
pub mod remote;
mod rpc;

use crate::protocal::{TSCompressionType, TSDataType, TSEncoding};
use std::collections::BTreeMap;
use std::error::Error;

pub type Dictionary = BTreeMap<String, String>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MeasurementSchema {
    pub measurement: String,
    pub data_type: TSDataType,
    pub encoding: TSEncoding,
    pub compressor: TSCompressionType,
    pub properties: Option<Dictionary>,
}

impl MeasurementSchema {
    pub fn new(
        measurement: String,
        data_type: TSDataType,
        encoding: TSEncoding,
        compressor: TSCompressionType,
        properties: Option<Dictionary>,
    ) -> Self {
        Self {
            measurement,
            data_type,
            encoding,
            compressor,
            properties,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Tablet {
    device_id: String,
    measurement_schemas: Vec<MeasurementSchema>,
    timestamps: Vec<i64>,
    columns: Vec<Vec<Value>>,
}

impl Into<Vec<u8>> for &Tablet {
    fn into(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::new();
        self.columns.iter().for_each(|column| {
            column.iter().for_each(|v| {
                let mut value_data: Vec<u8> = v.into();
                value_data.remove(0); //first item is datatype, remove it.
                buffer.append(&mut value_data);
            });
        });
        buffer
    }
}

impl Tablet {
    pub fn new(device_id: &str, measurement_schemas: Vec<MeasurementSchema>) -> Self {
        let mut columns: Vec<Vec<Value>> = Vec::new();
        measurement_schemas
            .iter()
            .for_each(|_| columns.push(Vec::new()));
        Self {
            device_id: device_id.to_string(),
            timestamps: Vec::new(),
            columns: columns,
            measurement_schemas: measurement_schemas.clone(),
        }
    }

    pub fn sort(&self) {
        todo!()
    }

    pub fn device_id(&self) -> String {
        self.device_id.clone()
    }

    pub fn measurement_schemas(&self) -> Vec<MeasurementSchema> {
        self.measurement_schemas.clone()
    }

    pub fn add_row(&mut self, row: Vec<Value>, timestamp: i64) {
        //todo: check
        self.timestamps.push(timestamp);
        self.columns
            .iter_mut()
            .zip(row.iter())
            .for_each(|(column, value)| column.push(value.clone()));
    }

    pub fn row_count(&self) -> usize {
        self.timestamps.len()
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Text(String),
    Null,
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match &self {
            Value::Bool(v) => v.to_string(),
            Value::Int32(v) => v.to_string(),
            Value::Int64(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::Double(v) => v.to_string(),
            Value::Text(v) => v.to_string(),
            Value::Null => "null".to_string(),
        }
    }
}

impl From<Vec<u8>> for Value {
    fn from(mut bytes: Vec<u8>) -> Self {
        match bytes.remove(0) {
            0 => Value::Bool(bytes.remove(0) == 1_u8),
            1 => Value::Int32(i32::from_be_bytes(bytes.try_into().unwrap())),
            2 => Value::Int64(i64::from_be_bytes(bytes.try_into().unwrap())),
            3 => Value::Float(f32::from_be_bytes(bytes.try_into().unwrap())),
            4 => Value::Double(f64::from_be_bytes(bytes.try_into().unwrap())),
            5 => Value::Text(String::from_utf8(bytes).unwrap()),
            _ => Value::Null,
        }
    }
}

impl Into<Vec<u8>> for &Value {
    fn into(self) -> Vec<u8> {
        match self {
            Value::Bool(v) => match v {
                true => vec![TSDataType::Boolean as u8, 1],
                false => vec![TSDataType::Boolean as u8, 0],
            },
            Value::Int32(v) => {
                let mut buff: Vec<u8> = Vec::new();
                buff.push(TSDataType::Int32 as u8);
                buff.append(&mut v.to_be_bytes().to_vec());
                buff
            }
            Value::Int64(v) => {
                let mut buff: Vec<u8> = Vec::new();
                buff.push(TSDataType::Int64 as u8);
                buff.append(&mut v.to_be_bytes().to_vec());
                buff
            }
            Value::Float(v) => {
                let mut buff: Vec<u8> = Vec::new();
                buff.push(TSDataType::Float as u8);
                buff.append(&mut v.to_be_bytes().to_vec());
                buff
            }
            Value::Double(v) => {
                let mut buff: Vec<u8> = Vec::new();
                buff.push(TSDataType::Double as u8);
                buff.append(&mut v.to_be_bytes().to_vec());
                buff
            }
            Value::Text(t) => {
                let mut buff: Vec<u8> = Vec::new();
                let len: i32 = t.len() as i32;
                buff.push(TSDataType::Text as u8);
                buff.append(&mut len.to_be_bytes().to_vec());
                buff.append(&mut t.as_bytes().to_vec());
                buff
            }
            Value::Null => vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct RowRecord {
    pub timestamp: i64,
    pub values: Vec<Value>,
}
pub trait DataSet: Iterator<Item = RowRecord> {
    fn get_column_names(&self) -> Vec<String>;
    fn is_ignore_timestamp(&self) -> bool;
}

pub trait Session {
    fn open(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    fn set_storage_group(&mut self, storage_group_id: &str) -> Result<(), Box<dyn Error>>;

    fn delete_storage_group(&mut self, storage_group_id: &str) -> Result<(), Box<dyn Error>>;

    fn delete_storage_groups(&mut self, storage_group_ids: Vec<&str>)
        -> Result<(), Box<dyn Error>>;

    fn create_timeseries<T>(
        &mut self,
        path: &str,
        data_type: TSDataType,
        encoding: TSEncoding,
        compressor: TSCompressionType,
        props: T,
        attributes: T,
        tags: T,
        measurement_alias: Option<String>,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Into<Option<Dictionary>>;

    fn create_multi_timeseries<T>(
        &mut self,
        paths: Vec<&str>,
        data_types: Vec<TSDataType>,
        encodings: Vec<TSEncoding>,
        compressors: Vec<TSCompressionType>,
        props_list: T,
        attributes_list: T,
        tags_list: T,
        measurement_alias_list: Option<Vec<String>>,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Into<Option<Vec<Dictionary>>>;

    fn delete_timeseries(&mut self, paths: Vec<&str>) -> Result<(), Box<dyn Error>>;

    fn delete_data(
        &mut self,
        paths: Vec<&str>,
        start_time: i64,
        end_time: i64,
    ) -> Result<(), Box<dyn Error>>;

    fn insert_string_record<T>(
        &mut self,
        device_id: &str,
        measurements: Vec<&str>,
        values: Vec<&str>,
        timestamp: i64,
        is_aligned: T,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Into<Option<bool>>;

    fn get_time_zone(&mut self) -> Result<String, Box<dyn Error>>;

    fn set_time_zone(&mut self, time_zone: &str) -> Result<(), Box<dyn Error>>;

    fn execute_statement<T>(
        &mut self,
        statement: &str,
        timeout_ms: T,
    ) -> Result<Box<dyn DataSet>, Box<dyn Error>>
    where
        T: Into<Option<i64>>;

    fn execute_query_statement<T>(
        &mut self,
        statement: &str,
        timeout_ms: T,
    ) -> Result<Box<dyn DataSet>, Box<dyn Error>>
    where
        T: Into<Option<i64>>;

    fn insert_record<T>(
        &mut self,
        device_id: &str,
        measurements: Vec<&str>,
        values: Vec<Value>,
        timestamp: i64,
        is_aligned: T,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Into<Option<bool>>;

    fn insert_records_of_one_device(
        &mut self,
        device_id: &str,
        timestamps: Vec<i64>,
        measurements: Vec<Vec<&str>>,
        values: Vec<Vec<Value>>,
        sorted: bool,
    ) -> Result<(), Box<dyn Error>>;

    fn insert_records(
        &mut self,
        device_ids: Vec<&str>,
        measurements: Vec<Vec<&str>>,
        values: Vec<Vec<Value>>,
        timestamps: Vec<i64>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn insert_tablet(&mut self, tablet: &Tablet, sorted: bool) -> Result<(), Box<dyn Error>>;

    fn insert_tablets(&mut self, tablets: Vec<&Tablet>, sorted: bool)
        -> Result<(), Box<dyn Error>>;

    fn execute_batch_statement(&mut self, statemens: Vec<&str>) -> Result<(), Box<dyn Error>>;

    fn execute_raw_data_query(
        &mut self,
        paths: Vec<&str>,
        start_time: i64,
        end_time: i64,
    ) -> Result<Box<dyn DataSet>, Box<dyn Error>>;

    fn execute_update_statement(
        &mut self,
        statement: &str,
    ) -> Result<Option<Box<dyn DataSet>>, Box<dyn Error>>;
}