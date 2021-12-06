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

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::vec;
use thrift::transport::TIoChannel;

use thrift::{
    protocol::{
        TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol,
        TInputProtocol, TOutputProtocol,
    },
    transport::{TFramedReadTransport, TFramedWriteTransport, TTcpChannel},
};

use crate::client::rpc::{
    TSCreateMultiTimeseriesReq, TSCreateTimeseriesReq, TSIServiceSyncClient, TSInsertRecordsReq,
    TSInsertStringRecordReq, TSInsertTabletReq, TSOpenSessionReq, TSProtocolVersion,
    TTSIServiceSyncClient,
};
use crate::protocal::{TSDataType, FLAG, SUCCESS_STATUS};

use super::rpc::{
    TSDeleteDataReq, TSExecuteStatementReq, TSInsertRecordReq, TSInsertRecordsOfOneDeviceReq,
    TSInsertTabletsReq, TSQueryDataSet, TSSetTimeZoneReq,
};
use super::{
    rpc::{TSCloseSessionReq, TSStatus},
    RowRecord,
};
use super::{DataSet, Dictionary, Session, Value};

#[derive(Debug, Clone)]

pub struct Config {
    pub host: String,
    pub port: i32,
    pub username: String,
    pub password: String,
    pub timeout_ms: Option<i64>,
    pub fetch_size: Option<i32>,
    pub timezone: Option<String>,
    pub enable_compression: bool,
    pub protocol_version: TSProtocolVersion,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: String::from("127.0.0.1"),
            port: 6667,
            username: String::from("root"),
            password: String::from("root"),
            timeout_ms: Some(30000),
            fetch_size: Some(1024),
            timezone: Some(String::from("Asia/Shanghai")),
            enable_compression: false,
            protocol_version: TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V3,
        }
    }
}

pub struct RpcSession {
    config: Config,
    session_id: Option<i64>,
    request_statement_id: i64,
    client: TSIServiceSyncClient<Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>>,
}

impl RpcSession {
    pub fn new(config: &Config) -> Result<Self, Box<dyn Error>> {
        let mut c = TTcpChannel::new();
        c.open(format!("{}:{}", config.host, config.port))?;
        let (i_chan, o_chan) = c.split()?;

        let (i_prot, o_prot) = (
            TFramedReadTransport::new(i_chan),
            TFramedWriteTransport::new(o_chan),
        );

        let (input_protocol, output_protocol): (Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>) =
            match config.enable_compression {
                false => (
                    Box::new(TBinaryInputProtocol::new(i_prot, true)),
                    Box::new(TBinaryOutputProtocol::new(o_prot, true)),
                ),
                true => (
                    Box::new(TCompactInputProtocol::new(i_prot)),
                    Box::new(TCompactOutputProtocol::new(o_prot)),
                ),
            };

        Ok(Self {
            config: config.clone(),
            session_id: None,
            request_statement_id: -1,
            client: TSIServiceSyncClient::new(input_protocol, output_protocol),
        })
    }
}

impl Iterator for RpcDataSet {
    type Item = RowRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.has_cached_results() {
            let mut values: Vec<Value> = Vec::new();

            let ts = self.query_data_set.time.drain(0..8).collect::<Vec<u8>>();
            self.timestamp = i64::from_be_bytes(ts.try_into().unwrap());

            for (column_index, column_data) in self.query_data_set.value_list.iter_mut().enumerate()
            {
                if self.row_index % 8 == 0 {
                    self.bitmaps[column_index] =
                        self.query_data_set.bitmap_list[column_index].remove(0);
                }

                let bitmap = self.bitmaps[column_index];
                let shift = self.row_index % 8;
                let null = ((FLAG >> shift) & (bitmap & 0xff)) == 0;

                // self.is_null(column_index, self.row_index);

                if !null {
                    let original_column_index = self.column_index_map.get(&column_index).unwrap();
                    let data_type = self.data_types.get(*original_column_index).unwrap();

                    let mut bytes: Vec<u8> = Vec::new();
                    match data_type {
                        TSDataType::Boolean => {
                            bytes.push(TSDataType::Boolean as u8);
                            bytes.push(column_data.remove(0));
                        }
                        TSDataType::Int32 => {
                            bytes.push(TSDataType::Int32 as u8);
                            bytes.extend(column_data.drain(0..4));
                        }
                        TSDataType::Int64 => {
                            bytes.push(TSDataType::Int64 as u8);
                            bytes.extend(column_data.drain(0..8));
                        }
                        TSDataType::Float => {
                            bytes.push(TSDataType::Float as u8);
                            bytes.extend(column_data.drain(0..4));
                        }
                        TSDataType::Double => {
                            bytes.push(TSDataType::Double as u8);
                            bytes.extend(column_data.drain(0..8));
                        }
                        TSDataType::Text => {
                            bytes.push(TSDataType::Text as u8);
                            let len = i32::from_be_bytes(
                                column_data
                                    .drain(0..4)
                                    .collect::<Vec<u8>>()
                                    .try_into()
                                    .unwrap(),
                            );
                            bytes.extend(column_data.drain(0..len as usize).collect::<Vec<u8>>());
                        }
                    }
                    values.push(Value::from(bytes));
                } else {
                    values.push(Value::Null);
                }
            }

            self.row_index += 1;

            let output_values: Vec<Value> = self
                .column_names
                .iter()
                .map(|column_name| {
                    values[*(self.column_name_index_map.get(column_name).unwrap()) as usize].clone()
                })
                .collect();
            Some(RowRecord {
                timestamp: self.timestamp,
                values: output_values,
            })
        } else {
            None
        }
    }
}

pub struct RpcDataSet {
    // session: &'a RpcSession,
    is_ignore_time_stamp: Option<bool>,
    timestamp: i64,
    column_names: Vec<String>,
    data_types: Vec<TSDataType>,
    query_data_set: TSQueryDataSet,
    column_index_map: HashMap<usize, usize>,
    column_name_index_map: BTreeMap<String, i32>,
    bitmaps: Vec<u8>,
    row_index: usize,
}

impl RpcDataSet {
    fn is_null(&self, column_index: usize, row_index: usize) -> bool {
        let bitmap = self.bitmaps[column_index];
        let shift = row_index % 8;
        return ((FLAG >> shift) & (bitmap & 0xff)) == 0;
    }

    fn has_cached_results(&self) -> bool {
        self.query_data_set.time.len() > 0
        //todo process fetch_result
    }
}

impl DataSet for RpcDataSet {
    fn is_ignore_timestamp(&self) -> bool {
        if let Some(v) = self.is_ignore_time_stamp {
            v
        } else {
            false
        }
    }

    fn get_column_names(&self) -> Vec<String> {
        self.column_names.clone()
    }
}

fn check_status(status: TSStatus) -> Result<(), Box<dyn Error>> {
    if status.code == SUCCESS_STATUS {
        Ok(())
    } else {
        //todo check substatus
        Err(status
            .message
            .unwrap_or(format!("Unknow, code: {}", status.code).to_string())
            .into())
    }
}

fn fire_closed_error() -> Result<(), Box<dyn Error>> {
    Err("Operation can't be performed, the session is closed.".into())
}

impl Session for RpcSession {
    fn open(&mut self) -> Result<(), Box<dyn Error>> {
        let resp = self.client.open_session(TSOpenSessionReq::new(
            self.config.protocol_version,
            self.config
                .timezone
                .clone()
                .unwrap_or(Config::default().timezone.unwrap()),
            self.config.username.clone(),
            self.config.password.clone(),
            None,
        ))?;
        if resp.status.code == SUCCESS_STATUS {
            self.session_id = resp.session_id;

            if let Some(session_id) = self.session_id {
                self.request_statement_id = self.client.request_statement_id(session_id)?;
            }

            Ok(())
        } else {
            Err(resp.status.message.unwrap().into())
        }
    }

    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let status = self
                .client
                .close_session(TSCloseSessionReq::new(session_id))?;
            self.session_id = None;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn set_storage_group(&mut self, storage_group_id: &str) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let status = self
                .client
                .set_storage_group(session_id, storage_group_id.to_string())?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn delete_storage_group(&mut self, storage_group_id: &str) -> Result<(), Box<dyn Error>> {
        self.delete_storage_groups(vec![storage_group_id])
    }

    fn delete_storage_groups(
        &mut self,
        storage_group_ids: Vec<&str>,
    ) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let status = self.client.delete_storage_groups(
                session_id,
                storage_group_ids.iter().map(|x| x.to_string()).collect(),
            )?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn create_timeseries<T>(
        &mut self,
        path: &str,
        data_type: crate::protocal::TSDataType,
        encoding: crate::protocal::TSEncoding,
        compressor: crate::protocal::TSCompressionType,
        props: T,
        attributes: T,
        tags: T,
        measurement_alias: Option<String>,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Into<Option<Dictionary>>,
    {
        if let Some(session_id) = self.session_id {
            let status = self.client.create_timeseries(TSCreateTimeseriesReq::new(
                session_id,
                path.to_string(),
                data_type.into(),
                encoding.into(),
                compressor.into(),
                props,
                tags,
                attributes,
                measurement_alias,
            ))?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn create_multi_timeseries<T>(
        &mut self,
        paths: Vec<&str>,
        data_types: Vec<crate::protocal::TSDataType>,
        encodings: Vec<crate::protocal::TSEncoding>,
        compressors: Vec<crate::protocal::TSCompressionType>,
        props_list: T,
        attributes_list: T,
        tags_list: T,
        measurement_alias_list: Option<Vec<String>>,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Into<Option<Vec<Dictionary>>>,
    {
        if let Some(session_id) = self.session_id {
            let status = self
                .client
                .create_multi_timeseries(TSCreateMultiTimeseriesReq::new(
                    session_id,
                    paths.iter().map(|x| x.to_string()).collect(),
                    data_types
                        .iter()
                        .map(|t| {
                            let n: i32 = t.into();
                            n
                        })
                        .collect(),
                    encodings
                        .iter()
                        .map(|e| {
                            let n: i32 = e.into();
                            n
                        })
                        .collect(),
                    compressors
                        .iter()
                        .map(|c| {
                            let n: i32 = c.into();
                            n
                        })
                        .collect(),
                    props_list,
                    attributes_list,
                    tags_list,
                    measurement_alias_list,
                ))?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn delete_timeseries(&mut self, paths: Vec<&str>) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let status = self
                .client
                .delete_timeseries(session_id, paths.iter().map(|x| x.to_string()).collect())?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn delete_data(
        &mut self,
        paths: Vec<&str>,
        start_time: i64,
        end_time: i64,
    ) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let status = self.client.delete_data(TSDeleteDataReq::new(
                session_id,
                paths.iter().map(|p| p.to_string()).collect(),
                start_time,
                end_time,
            ))?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn insert_string_record<T>(
        &mut self,
        device_id: &str,
        measurements: Vec<&str>,
        values: Vec<&str>,
        timestamp: i64,
        is_aligned: T,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Into<Option<bool>>,
    {
        if let Some(session_id) = self.session_id {
            let status = self
                .client
                .insert_string_record(TSInsertStringRecordReq::new(
                    session_id,
                    device_id.to_string(),
                    measurements.iter().map(|x| x.to_string()).collect(),
                    values.iter().map(|x| x.to_string()).collect(),
                    timestamp,
                    is_aligned,
                ))?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn get_time_zone(&mut self) -> Result<String, Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let resp = self.client.get_time_zone(session_id)?;
            let status = resp.status;

            if status.code == SUCCESS_STATUS {
                Ok(resp.time_zone)
            } else {
                Err(status
                    .message
                    .unwrap_or(format!("Unknow, code: {}", status.code).to_string())
                    .into())
            }
        } else {
            Err("Operation can't be performed, the session is closed.".into())
        }
    }

    fn set_time_zone(&mut self, time_zone: &str) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let status = self
                .client
                .set_time_zone(TSSetTimeZoneReq::new(session_id, time_zone.to_string()))?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn execute_statement<T>(
        &mut self,
        statement: &str,
        timeout_ms: T,
    ) -> Result<Box<dyn DataSet>, Box<dyn Error>>
    where
        T: Into<Option<i64>>,
    {
        if let Some(session_id) = self.session_id {
            let resp = self.client.execute_statement(TSExecuteStatementReq {
                session_id: session_id,
                statement: statement.to_string(),
                statement_id: self.request_statement_id,
                fetch_size: self.config.fetch_size,
                timeout: timeout_ms.into(),
                enable_redirect_query: None,
                jdbc_query: None,
            })?;
            let status = resp.status;
            if status.code == SUCCESS_STATUS {
                {
                    let column_names: Vec<String> = resp.columns.unwrap();

                    let column_name_index_map = match resp.column_name_index_map {
                        Some(map) => map,
                        None => {
                            let mut map: BTreeMap<String, i32> = BTreeMap::new();
                            for (index, name) in column_names.iter().enumerate() {
                                map.insert(name.to_string(), index as i32);
                            }
                            map
                        }
                    };

                    let data_types: Vec<TSDataType> = resp
                        .data_type_list
                        .unwrap()
                        .iter()
                        .map(|t| TSDataType::from(t))
                        .collect();

                    let mut column_index_map: HashMap<usize, usize> = HashMap::new();

                    let column_count = column_names.len();
                    for (index, name) in column_names.iter().enumerate() {
                        column_index_map
                            .insert(*column_name_index_map.get(name).unwrap() as usize, index);
                    }

                    return Ok(Box::new(RpcDataSet {
                        // session: self,
                        timestamp: -1,
                        is_ignore_time_stamp: resp.ignore_time_stamp,
                        query_data_set: resp.query_data_set.unwrap(),
                        column_names: column_names,
                        data_types: data_types,
                        bitmaps: vec![0_u8; column_count],
                        row_index: 0,
                        column_index_map: column_index_map,
                        column_name_index_map: column_name_index_map,
                    }));
                }
            } else {
                return Err(status
                    .message
                    .unwrap_or(format!("Unknow, code: {}", status.code).to_string())
                    .into());
            }
        } else {
            Err("Operation can't be performed, the session is closed.".into())
        }
    }

    fn execute_query_statement<T>(
        &mut self,
        statement: &str,
        timeout_ms: T,
    ) -> Result<Box<dyn DataSet>, Box<dyn Error>>
    where
        T: Into<Option<i64>>,
    {
        if let Some(session_id) = self.session_id {
            let resp = self.client.execute_query_statement(TSExecuteStatementReq {
                session_id: session_id,
                statement: statement.to_string(),
                statement_id: self.request_statement_id,
                fetch_size: self.config.fetch_size,
                timeout: timeout_ms.into(),
                enable_redirect_query: None,
                jdbc_query: None,
            })?;
            let status = resp.status;

            if status.code == SUCCESS_STATUS {
                let column_names: Vec<String> = resp.columns.unwrap();

                let column_name_index_map = match resp.column_name_index_map {
                    Some(v) => v,
                    None => {
                        let mut map: BTreeMap<String, i32> = BTreeMap::new();
                        for (index, name) in column_names.iter().enumerate() {
                            map.insert(name.to_string(), index as i32);
                        }
                        map
                    }
                };

                let data_types: Vec<TSDataType> = resp
                    .data_type_list
                    .unwrap()
                    .iter()
                    .map(|t| TSDataType::from(t))
                    .collect();

                let mut column_index_map: HashMap<usize, usize> = HashMap::new();

                let column_count = column_names.len();
                for (index, name) in column_names.iter().enumerate() {
                    column_index_map
                        .insert(*column_name_index_map.get(name).unwrap() as usize, index);
                }

                return Ok(Box::new(RpcDataSet {
                    // session: self,
                    timestamp: -1,
                    is_ignore_time_stamp: resp.ignore_time_stamp,
                    query_data_set: resp.query_data_set.unwrap(),
                    column_names: column_names,
                    data_types: data_types,
                    bitmaps: vec![0_u8; column_count],
                    row_index: 0,
                    column_index_map: column_index_map,
                    column_name_index_map: column_name_index_map,
                }));
            } else {
                return Err(status
                    .message
                    .unwrap_or(format!("Unknow, code: {}", status.code).to_string())
                    .into());
            }
        } else {
            Err("Operation can't be performed, the session is closed.".into())
        }
    }

    fn insert_record<T>(
        &mut self,
        device_id: &str,
        measurements: Vec<&str>,
        values: Vec<Value>,
        timestamp: i64,
        is_aligned: T,
    ) -> Result<(), Box<dyn Error>>
    where
        T: Into<Option<bool>>,
    {
        if let Some(session_id) = self.session_id {
            let mut values_bytes: Vec<u8> = Vec::new();
            values.iter().for_each(|v| {
                let mut value_data: Vec<u8> = v.into();
                values_bytes.append(&mut value_data);
            });
            let status = self.client.insert_record(TSInsertRecordReq::new(
                session_id,
                device_id.to_string(),
                measurements.iter().map(|x| x.to_string()).collect(),
                values_bytes,
                timestamp,
                is_aligned,
            ))?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn insert_records_of_one_device(
        &mut self,
        device_id: &str,
        timestamps: Vec<i64>,
        measurements: Vec<Vec<&str>>,
        values: Vec<Vec<super::Value>>,
        sorted: bool,
    ) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let values_list = values
                .iter()
                .map(|vec| {
                    let mut values: Vec<u8> = Vec::new();
                    for value in vec.iter() {
                        let mut value_data: Vec<u8> = value.into();
                        values.append(&mut value_data);
                    }
                    values
                })
                .collect();
            let status =
                self.client
                    .insert_records_of_one_device(TSInsertRecordsOfOneDeviceReq::new(
                        session_id,
                        device_id.to_string(),
                        measurements
                            .iter()
                            .map(|vec| vec.iter().map(|s| s.to_string()).collect())
                            .collect(),
                        values_list,
                        timestamps,
                        false,
                    ))?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn insert_records(
        &mut self,
        device_ids: Vec<&str>,
        measurements: Vec<Vec<&str>>,
        values: Vec<Vec<super::Value>>,
        timestamps: Vec<i64>,
    ) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let values_list = values
                .iter()
                .map(|vec| {
                    let mut values: Vec<u8> = Vec::new();
                    for value in vec.iter() {
                        let mut value_data: Vec<u8> = value.into();
                        values.append(&mut value_data);
                    }
                    values
                })
                .collect();
            let status = self.client.insert_records(TSInsertRecordsReq {
                session_id: session_id,
                prefix_paths: device_ids
                    .iter()
                    .map(|device_id| device_id.to_string())
                    .collect(),
                measurements_list: measurements
                    .iter()
                    .map(|ms| ms.iter().map(|m| m.to_string()).collect())
                    .collect(),
                values_list: values_list,
                timestamps: timestamps,
                is_aligned: None,
            })?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn insert_tablet(
        &mut self,
        tablet: &super::Tablet,
        sorted: bool,
    ) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let mut timestamps_list: Vec<u8> = Vec::new();
            tablet
                .timestamps
                .iter()
                .for_each(|ts| timestamps_list.append(&mut ts.to_be_bytes().to_vec()));

            if !sorted {
                tablet.sort();
            }
            let status = self.client.insert_tablet(TSInsertTabletReq {
                session_id: session_id,
                prefix_path: tablet.device_id(),
                measurements: tablet
                    .measurement_schemas
                    .iter()
                    .map(|f| f.measurement.to_string())
                    .collect(),
                values: tablet.into(),
                timestamps: timestamps_list,
                types: tablet
                    .measurement_schemas()
                    .iter()
                    .map(|measurement_schema| {
                        let t: i32;
                        t = (&measurement_schema.data_type).into();
                        t
                    })
                    .collect(),
                size: tablet.row_count() as i32,
                is_aligned: Some(false),
            })?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn insert_tablets(
        &mut self,
        tablets: Vec<&super::Tablet>,
        sorted: bool,
    ) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            if !sorted {
                tablets.iter().for_each(|t| t.sort());
            }
            let status = self.client.insert_tablets(TSInsertTabletsReq {
                session_id: session_id,
                prefix_paths: tablets.iter().map(|t| t.device_id()).collect(),
                measurements_list: tablets
                    .iter()
                    .map(|tablet| {
                        tablet
                            .measurement_schemas
                            .iter()
                            .map(|f| f.measurement.to_string())
                            .collect()
                    })
                    .collect(),
                values_list: tablets
                    .iter()
                    .map(|tablet| {
                        let values: Vec<u8> = (*tablet).into();
                        values
                    })
                    .collect(),
                timestamps_list: tablets
                    .iter()
                    .map(|tablet| {
                        let mut ts_item: Vec<u8> = Vec::new();
                        tablet
                            .timestamps
                            .iter()
                            .for_each(|ts| ts_item.append(&mut ts.to_be_bytes().to_vec()));
                        ts_item
                    })
                    .collect(),
                types_list: tablets
                    .iter()
                    .map(|tablet| {
                        tablet
                            .measurement_schemas()
                            .iter()
                            .map(|f| {
                                let t: i32 = (&(*f).data_type).into();
                                t
                            })
                            .collect()
                    })
                    .collect(),
                size_list: tablets
                    .iter()
                    .map(|tablet| tablet.row_count() as i32)
                    .collect(),
                is_aligned: Some(false),
            })?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn execute_batch_statement(&mut self, statemens: Vec<&str>) -> Result<(), Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let status =
                self.client
                    .execute_batch_statement(super::rpc::TSExecuteBatchStatementReq {
                        session_id: session_id,
                        statements: statemens.iter().map(|f| f.to_string()).collect(),
                    })?;
            check_status(status)
        } else {
            fire_closed_error()
        }
    }

    fn execute_raw_data_query(
        &mut self,
        paths: Vec<&str>,
        start_time: i64,
        end_time: i64,
    ) -> Result<Box<dyn DataSet>, Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let resp = self
                .client
                .execute_raw_data_query(super::rpc::TSRawDataQueryReq {
                    session_id: session_id,
                    paths: paths.iter().map(|f| f.to_string()).collect(),
                    fetch_size: self.config.fetch_size,
                    start_time: start_time,
                    end_time: end_time,
                    statement_id: self.request_statement_id,
                    enable_redirect_query: None,
                    jdbc_query: None,
                })?;
            let status = resp.status;
            if status.code == SUCCESS_STATUS {
                if let Some(query_data_set) = resp.query_data_set {
                    let column_names: Vec<String> = resp.columns.unwrap();

                    let column_name_index_map = match resp.column_name_index_map {
                        Some(v) => v,
                        None => {
                            let mut map: BTreeMap<String, i32> = BTreeMap::new();
                            for (index, name) in column_names.iter().enumerate() {
                                map.insert(name.to_string(), index as i32);
                            }
                            map
                        }
                    };

                    let data_types: Vec<TSDataType> = resp
                        .data_type_list
                        .unwrap()
                        .iter()
                        .map(|t| TSDataType::from(t))
                        .collect();

                    let mut column_index_map: HashMap<usize, usize> = HashMap::new();

                    let column_count = column_names.len();
                    for (index, name) in column_names.iter().enumerate() {
                        column_index_map
                            .insert(*column_name_index_map.get(name).unwrap() as usize, index);
                    }

                    return Ok(Box::new(RpcDataSet {
                        // session: self,
                        timestamp: -1,
                        is_ignore_time_stamp: resp.ignore_time_stamp,
                        query_data_set: query_data_set,
                        column_names: column_names,
                        data_types: data_types,
                        bitmaps: vec![0_u8; column_count],
                        row_index: 0,
                        column_index_map: column_index_map,
                        column_name_index_map: column_name_index_map,
                    }));
                } else {
                    Err("Did't get the result.".into())
                }
            } else {
                Err(status
                    .message
                    .unwrap_or(format!("Unknow, code: {}", status.code).to_string())
                    .into())
            }
        } else {
            Err("Operation can't be performed, the session is closed.".into())
        }
    }

    fn execute_update_statement(
        &mut self,
        statement: &str,
    ) -> Result<Option<Box<dyn DataSet>>, Box<dyn Error>> {
        if let Some(session_id) = self.session_id {
            let resp = self
                .client
                .execute_update_statement(TSExecuteStatementReq {
                    session_id: session_id,
                    statement: statement.to_string(),
                    statement_id: self.request_statement_id,
                    fetch_size: self.config.fetch_size,
                    timeout: self.config.timeout_ms,
                    enable_redirect_query: None,
                    jdbc_query: None,
                })?;
            let status = resp.status;
            if status.code == SUCCESS_STATUS {
                if let Some(query_data_set) = resp.query_data_set {
                    let column_names: Vec<String> = resp.columns.unwrap();

                    let column_name_index_map = match resp.column_name_index_map {
                        Some(v) => v,
                        None => {
                            let mut map: BTreeMap<String, i32> = BTreeMap::new();
                            for (index, name) in column_names.iter().enumerate() {
                                map.insert(name.to_string(), index as i32);
                            }
                            map
                        }
                    };

                    let data_types: Vec<TSDataType> = resp
                        .data_type_list
                        .unwrap()
                        .iter()
                        .map(|t| TSDataType::from(t))
                        .collect();

                    let mut column_index_map: HashMap<usize, usize> = HashMap::new();

                    let column_count = column_names.len();
                    for (index, name) in column_names.iter().enumerate() {
                        column_index_map
                            .insert(*column_name_index_map.get(name).unwrap() as usize, index);
                    }

                    return Ok(Some(Box::new(RpcDataSet {
                        // session: self,
                        timestamp: -1,
                        is_ignore_time_stamp: resp.ignore_time_stamp,
                        query_data_set: query_data_set,
                        column_names: column_names,
                        data_types: data_types,
                        bitmaps: vec![0_u8; column_count],
                        row_index: 0,
                        column_index_map: column_index_map,
                        column_name_index_map: column_name_index_map,
                    })));
                } else {
                    Ok(None)
                }
            } else {
                Err(status
                    .message
                    .unwrap_or(format!("Unknow, code: {}", status.code).to_string())
                    .into())
            }
        } else {
            Err("Operation can't be performed, the session is closed.".into())
        }
    }
}
