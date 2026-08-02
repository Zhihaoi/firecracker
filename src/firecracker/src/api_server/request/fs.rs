// Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use vmm::logger::{IncMetric, METRICS};
use vmm::rpc_interface::VmmAction;
use vmm::vmm_config::fs::FsDeviceConfig;

use super::super::parsed_request::{ParsedRequest, RequestError, checked_id};
use super::{Body, StatusCode};

pub(crate) fn parse_put_fs(
    body: &Body,
    id_from_path: Option<&str>,
) -> Result<ParsedRequest, RequestError> {
    METRICS.put_api_requests.fs_count.inc();
    let id = if let Some(id) = id_from_path {
        checked_id(id)?
    } else {
        METRICS.put_api_requests.fs_fails.inc();
        return Err(RequestError::EmptyID);
    };

    let device_cfg = serde_json::from_slice::<FsDeviceConfig>(body.raw()).inspect_err(|_| {
        METRICS.put_api_requests.fs_fails.inc();
    })?;

    if id != device_cfg.fs_id {
        METRICS.put_api_requests.fs_fails.inc();
        Err(RequestError::Generic(
            StatusCode::BadRequest,
            "The id from the path does not match the id from the body!".to_string(),
        ))
    } else {
        Ok(ParsedRequest::new_sync(VmmAction::InsertFsDevice(
            device_cfg,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_server::parsed_request::tests::vmm_action_from_request;

    #[test]
    fn test_parse_put_fs_request() {
        parse_put_fs(&Body::new("invalid_payload"), None).unwrap_err();
        parse_put_fs(&Body::new("invalid_payload"), Some("id")).unwrap_err();

        // PUT with invalid fields.
        let body = r#"{
            "fs_id": "bar",
            "invalid_field": false
        }"#;
        parse_put_fs(&Body::new(body), Some("bar")).unwrap_err();

        // PUT with missing socket field.
        let body = r#"{
            "fs_id": "bar"
        }"#;
        parse_put_fs(&Body::new(body), Some("bar")).unwrap_err();

        // PUT with missing all optional fields.
        let body = r#"{
            "fs_id": "1000",
            "socket": "dummy"
        }"#;
        parse_put_fs(&Body::new(body), Some("1000")).unwrap();

        // PUT with invalid types on fields. Adding an fs_id as number instead of string.
        let body = r#"{
            "fs_id": 1000,
            "socket": "dummy"
        }"#;
        parse_put_fs(&Body::new(body), Some("1000")).unwrap_err();

        // PUT with the complete configuration.
        let body = r#"{
            "fs_id": "1000",
            "socket": "dummy",
            "tag": "my_tag",
            "num_request_queues": 4
        }"#;
        parse_put_fs(&Body::new(body), Some("1000")).unwrap();

        // The id from the path must match the id from the body.
        let body = r#"{
            "fs_id": "foo",
            "socket": "dummy"
        }"#;
        parse_put_fs(&Body::new(body), Some("bar")).unwrap_err();

        let expected_config = FsDeviceConfig {
            fs_id: "foo".to_string(),
            socket: "dummy".to_string(),
            tag: None,
            num_request_queues: None,
        };
        assert_eq!(
            vmm_action_from_request(parse_put_fs(&Body::new(body), Some("foo")).unwrap()),
            VmmAction::InsertFsDevice(expected_config)
        );
    }
}
