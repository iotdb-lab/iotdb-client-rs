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

use super::{DataSet, Dictionary, RowRecord, Session, Value};

use std::error::Error;

pub struct DirectDataSet {
    session: DirectSession,
}

impl DataSet for DirectDataSet {
    fn is_ignore_timestamp(&self) -> bool {
        todo!()
    }

    fn get_column_names(&self) -> Vec<String> {
        todo!()
    }

    fn get_data_types(&self) -> Vec<crate::protocal::TSDataType> {
        todo!()
    }
}

impl Iterator for DirectDataSet {
    type Item = RowRecord;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct DirectSession {
    // /data/apache-iotdb-1.12.3
    pub iotdb_home_directory: String,
}

impl DirectSession {
    pub fn new(iotdb_home: &str) -> Self {
        Self {
            iotdb_home_directory: iotdb_home.to_string(),
        }
    }
}

impl<'a> Session<'a> for DirectSession {
    fn open(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    fn set_storage_group(&mut self, storage_group_id: &str) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn delete_storage_group(&mut self, storage_group_id: &str) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn delete_storage_groups(
        &mut self,
        storage_group_ids: Vec<&str>,
    ) -> Result<(), Box<dyn Error>> {
        todo!()
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
        todo!()
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
        todo!()
    }

    fn delete_timeseries(&mut self, paths: Vec<&str>) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn delete_data(
        &mut self,
        paths: Vec<&str>,
        start_time: i64,
        end_time: i64,
    ) -> Result<(), Box<dyn Error>> {
        todo!()
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
        todo!()
    }

    fn get_time_zone(&mut self) -> Result<String, Box<dyn Error>> {
        todo!()
    }

    fn set_time_zone(&mut self, time_zone: &str) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn execute_statement<T>(
        &'a mut self,
        statement: &str,
        timeout_ms: T,
    ) -> Result<Box<dyn 'a + DataSet>, Box<dyn Error>>
    where
        T: Into<Option<i64>>,
    {
        todo!()
    }

    fn execute_query_statement<T>(
        &'a mut self,
        statement: &str,
        timeout_ms: T,
    ) -> Result<Box<dyn 'a + DataSet>, Box<dyn Error>>
    where
        T: Into<Option<i64>>,
    {
        todo!()
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
        todo!()
    }

    fn insert_records_of_one_device(
        &mut self,
        device_id: &str,
        timestamps: Vec<i64>,
        measurements: Vec<Vec<&str>>,
        values: Vec<Vec<Value>>,
        sorted: bool,
    ) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn insert_records(
        &mut self,
        device_ids: Vec<&str>,
        measurements: Vec<Vec<&str>>,
        values: Vec<Vec<Value>>,
        timestamps: Vec<i64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    fn insert_tablet(
        &mut self,
        tablet: &super::Tablet,
        sorted: bool,
    ) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn insert_tablets(
        &mut self,
        tablets: Vec<&super::Tablet>,
        sorted: bool,
    ) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn execute_batch_statement(&mut self, statemens: Vec<&str>) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn execute_raw_data_query(
        &'a mut self,
        paths: Vec<&str>,
        start_time: i64,
        end_time: i64,
    ) -> Result<Box<dyn 'a + DataSet>, Box<dyn Error>> {
        todo!()
    }

    fn execute_update_statement(
        &'a mut self,
        statement: &str,
    ) -> Result<Option<Box<dyn 'a + DataSet>>, Box<dyn Error>> {
        todo!()
    }
}
