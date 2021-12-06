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

pub const FLAG: u8 = 0x80;

pub const SUCCESS_STATUS: i32 = 200;
pub const STILL_EXECUTING_STATUS: i32 = 201;
pub const INVALID_HANDLE_STATUS: i32 = 202;
pub const INCOMPATIBLE_VERSION: i32 = 203;
pub const NODE_DELETE_FAILED_ERROR: i32 = 298;
pub const ALIAS_ALREADY_EXIST_ERROR: i32 = 299;
pub const PATH_ALREADY_EXIST_ERROR: i32 = 300;
pub const PATH_NOT_EXIST_ERROR: i32 = 301;
pub const UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR: i32 = 302;
pub const METADATA_ERROR: i32 = 303;
pub const TIMESERIES_NOT_EXIST: i32 = 304;
pub const OUT_OF_TTLERROR: i32 = 305;
pub const CONFIG_ADJUSTER: i32 = 306;
pub const MERGE_ERROR: i32 = 307;
pub const SYSTEM_CHECK_ERROR: i32 = 308;
pub const SYNC_DEVICE_OWNER_CONFLICT_ERROR: i32 = 309;
pub const SYNC_CONNECTION_EXCEPTION: i32 = 310;
pub const STORAGE_GROUP_PROCESSOR_ERROR: i32 = 311;
pub const STORAGE_GROUP_ERROR: i32 = 312;
pub const STORAGE_ENGINE_ERROR: i32 = 313;
pub const TSFILE_PROCESSOR_ERROR: i32 = 314;
pub const PATH_ILLEGAL: i32 = 315;
pub const LOAD_FILE_ERROR: i32 = 316;
pub const STORAGE_GROUP_NOT_READY: i32 = 317;

pub const EXECUTE_STATEMENT_ERROR: i32 = 400;
pub const SQLPARSE_ERROR: i32 = 401;
pub const GENERATE_TIME_ZONE_ERROR: i32 = 402;
pub const SET_TIME_ZONE_ERROR: i32 = 403;
pub const NOT_STORAGE_GROUP_ERROR: i32 = 404;
pub const QUERY_NOT_ALLOWED: i32 = 405;
pub const AST_FORMAT_ERROR: i32 = 406;
pub const LOGICAL_OPERATOR_ERROR: i32 = 407;
pub const LOGICAL_OPTIMIZE_ERROR: i32 = 408;
pub const UNSUPPORTED_FILL_TYPE_ERROR: i32 = 409;
pub const PATH_ERRO_R: i32 = 410;
pub const QUERY_PROCESS_ERROR: i32 = 411;
pub const WRITE_PROCESS_ERROR: i32 = 412;
pub const WRITE_PROCESS_REJECT: i32 = 413;

pub const UNSUPPORTED_INDEX_FUNC_ERROR: i32 = 421;
pub const UNSUPPORTED_INDEX_TYPE_ERROR: i32 = 422;

pub const INTERNAL_SERVER_ERROR: i32 = 500;
pub const CLOSE_OPERATION_ERROR: i32 = 501;
pub const READ_ONLY_SYSTEM_ERROR: i32 = 502;
pub const DISK_SPACE_INSUFFICIENT_ERROR: i32 = 503;
pub const START_UP_ERROR: i32 = 504;
pub const SHUT_DOWN_ERROROR: i32 = 505;
pub const MULTIPLE_ERROR: i32 = 506;

pub const WRONG_LOGIN_PASSWORD_ERROR: i32 = 600;
pub const NOT_LOGIN_ERROR: i32 = 601;
pub const NO_PERMISSION_ERROR: i32 = 602;
pub const UNINITIALIZED_AUTH_ERROR: i32 = 603;

pub const PARTITION_NOT_READY: i32 = 700;
pub const TIME_OUT: i32 = 701;
pub const NO_LEADER: i32 = 702;
pub const UNSUPPORTED_OPERATION: i32 = 703;
pub const NODE_READ_ONLY: i32 = 704;
pub const CONSISTENCY_FAILURE: i32 = 705;
pub const NO_CONNECTION: i32 = 706;
pub const NEED_REDIRECTION: i32 = 707;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TSDataType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Float = 3,
    Double = 4,
    Text = 5,
}

impl Into<i32> for &TSDataType {
    fn into(self) -> i32 {
        match self {
            TSDataType::Boolean => 0,
            TSDataType::Int32 => 1,
            TSDataType::Int64 => 2,
            TSDataType::Float => 3,
            TSDataType::Double => 4,
            TSDataType::Text => 5,
        }
    }
}

impl Into<i32> for TSDataType {
    fn into(self) -> i32 {
        match self {
            TSDataType::Boolean => 0,
            TSDataType::Int32 => 1,
            TSDataType::Int64 => 2,
            TSDataType::Float => 3,
            TSDataType::Double => 4,
            TSDataType::Text => 5,
        }
    }
}

impl From<&String> for TSDataType {
    fn from(t: &String) -> Self {
        match t.as_str() {
            "BOOLEAN" => TSDataType::Boolean,
            "INT32" => TSDataType::Int32,
            "INT64" => TSDataType::Int64,
            "FLOAT" => TSDataType::Float,
            "DOUBLE" => TSDataType::Double,
            "TEXT" => TSDataType::Text,
            _ => panic!("Illegal datatype {}", t),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TSEncoding {
    Plain = 0,
    PlainDictionary = 1,
    RLE = 2,
    Diff = 3,
    Ts2diff = 4,
    Bitmap = 5,
    GorillaV1 = 6,
    Regular = 7,
    Gorilla = 8,
}

impl Into<i32> for TSEncoding {
    fn into(self) -> i32 {
        match self {
            TSEncoding::Plain => 0,
            TSEncoding::PlainDictionary => 1,
            TSEncoding::RLE => 2,
            TSEncoding::Diff => 3,
            TSEncoding::Ts2diff => 4,
            TSEncoding::Bitmap => 5,
            TSEncoding::GorillaV1 => 6,
            TSEncoding::Regular => 7,
            TSEncoding::Gorilla => 8,
        }
    }
}

impl Into<i32> for &TSEncoding {
    fn into(self) -> i32 {
        match self {
            TSEncoding::Plain => 0,
            TSEncoding::PlainDictionary => 1,
            TSEncoding::RLE => 2,
            TSEncoding::Diff => 3,
            TSEncoding::Ts2diff => 4,
            TSEncoding::Bitmap => 5,
            TSEncoding::GorillaV1 => 6,
            TSEncoding::Regular => 7,
            TSEncoding::Gorilla => 8,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TSCompressionType {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    SDT = 4,
    PAA = 5,
    PLA = 6,
    LZ4 = 7,
}

impl Into<i32> for TSCompressionType {
    fn into(self) -> i32 {
        match self {
            TSCompressionType::UNCOMPRESSED => 0,
            TSCompressionType::SNAPPY => 1,
            TSCompressionType::GZIP => 2,
            TSCompressionType::LZO => 3,
            TSCompressionType::SDT => 4,
            TSCompressionType::PAA => 5,
            TSCompressionType::PLA => 6,
            TSCompressionType::LZ4 => 7,
        }
    }
}

impl Into<i32> for &TSCompressionType {
    fn into(self) -> i32 {
        match self {
            TSCompressionType::UNCOMPRESSED => 0,
            TSCompressionType::SNAPPY => 1,
            TSCompressionType::GZIP => 2,
            TSCompressionType::LZO => 3,
            TSCompressionType::SDT => 4,
            TSCompressionType::PAA => 5,
            TSCompressionType::PLA => 6,
            TSCompressionType::LZ4 => 7,
        }
    }
}
