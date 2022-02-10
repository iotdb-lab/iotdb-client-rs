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
pub mod client;
pub mod protocal;

#[cfg(test)]
mod tests {
    use crate::client::Value;
    use std::vec::Vec;

    #[test]
    fn test_value_to_string() {
        let values = vec![
            Value::Bool(true),
            Value::Int32(1),
            Value::Int64(2),
            Value::Float(3.1),
            Value::Double(4.1),
            Value::Text(String::from("iotdb")),
            Value::Null,
        ];

        let strings = vec!["true", "1", "2", "3.1", "4.1", "iotdb", "null"];

        for (v, s) in values.into_iter().zip(strings.into_iter()) {
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn test_value_into() {
        let values = vec![
            Value::Bool(true),
            Value::Int32(1),
            Value::Int64(2),
            Value::Float(3.1),
            Value::Double(4.1),
            Value::Text(String::from("iotdb")),
        ];

        let bytes = vec![
            vec![0u8, 1u8],
            vec![vec![1u8], 1_i32.to_be_bytes().to_vec()]
                .into_iter()
                .flatten()
                .collect(),
            vec![vec![2u8], 2_i64.to_be_bytes().to_vec()]
                .into_iter()
                .flatten()
                .collect(),
            vec![vec![3u8], 3.1_f32.to_be_bytes().to_vec()]
                .into_iter()
                .flatten()
                .collect(),
            vec![vec![4u8], 4.1_f64.to_be_bytes().to_vec()]
                .into_iter()
                .flatten()
                .collect(),
            vec![
                vec![5u8],                    //datatype text
                5_i32.to_be_bytes().to_vec(), //len of iotdb
                "iotdb".to_string().as_bytes().to_vec(),
            ]
            .into_iter()
            .flatten()
            .collect(),
        ];

        for (v, bys) in values.into_iter().zip(bytes.into_iter()) {
            let value_bys: Vec<u8> = (&v).into();
            assert_eq!(value_bys, bys);
        }
    }
}
