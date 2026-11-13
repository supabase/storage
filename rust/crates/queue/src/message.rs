use std::collections::HashMap;

use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use storage_queue_transport::{AttributeValue, MessageAttribute, Outgoing, Received};

use crate::{Error, Job};

const SYSTEM_TENANT_REF: &str = "SYSTEM_TENANT";
const HEADERS_ATTRIBUTE: &str = "queue.headers";
pub(crate) const KEY_ATTRIBUTE: &str = "queue.key";

pub(crate) fn payload_tenant(payload: &RawValue) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(payload.get()).ok()?;
    value.get("tenant")?.get("ref")?.as_str().map(str::to_owned)
}

fn string_attribute(value: String) -> MessageAttribute {
    MessageAttribute {
        data_type: "String".into(),
        value: AttributeValue::Text(value),
    }
}

fn group_id(key: &str) -> String {
    if !key.is_empty() && key.len() <= 128 && key.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        key.to_owned()
    } else {
        use std::fmt::Write;
        let mut result = String::from("sha256:");
        for byte in Sha256::digest(key.as_bytes()) {
            write!(&mut result, "{byte:02x}").expect("writing to a string cannot fail");
        }
        result
    }
}

#[derive(Debug, Default)]
pub struct EncodeOptions<'a> {
    /// Override the key derived from the payload's tenant reference.
    pub key: Option<&'a str>,
    /// Used as a fair-queue group when there is no key. Standard SQS does not deduplicate it.
    pub idempotency_key: Option<&'a str>,
    pub headers: HashMap<String, String>,
}

/// Encode the bare JSON body and manager attributes for this job's queue.
pub fn encode<J: Job>(payload: &J::Payload) -> Result<Outgoing, Error> {
    encode_with_options::<J>(payload, EncodeOptions::default())
}

pub fn encode_with_options<J: Job>(
    payload: &J::Payload,
    options: EncodeOptions<'_>,
) -> Result<Outgoing, Error> {
    let payload = serde_json::value::to_raw_value(payload)?;
    let tenant = payload_tenant(&payload).filter(|t| t != SYSTEM_TENANT_REF);
    let key = options
        .key
        .or(tenant.as_deref())
        .filter(|key| !key.is_empty());
    let fair_group = key.or(options.idempotency_key.filter(|key| !key.is_empty()));
    let mut attributes = HashMap::new();
    if let Some(key) = key {
        attributes.insert(KEY_ATTRIBUTE.into(), string_attribute(key.to_owned()));
    }
    let mut headers = options.headers;
    if let Some(kind) = J::TYPE {
        headers
            .entry("message-type".into())
            .or_insert_with(|| kind.to_owned());
    }
    if !headers.is_empty() {
        attributes.insert(
            HEADERS_ATTRIBUTE.into(),
            string_attribute(serde_json::to_string(&headers)?),
        );
    }
    Ok(Outgoing {
        body: payload.get().to_owned(),
        group_id: fair_group.map(group_id),
        attributes,
    })
}

pub(crate) fn text_attribute<'a>(message: &'a Received, name: &str) -> Option<&'a str> {
    match &message.attributes.get(name)?.value {
        AttributeValue::Text(text) => Some(text),
        AttributeValue::Binary(_) => None,
    }
}

pub(crate) fn message_headers(message: &Received) -> HashMap<String, String> {
    if let Some(packed) = text_attribute(message, HEADERS_ATTRIBUTE) {
        return match serde_json::from_str::<serde_json::Value>(packed) {
            Ok(serde_json::Value::Object(headers)) => headers
                .into_iter()
                .filter_map(|(name, value)| value.as_str().map(|text| (name, text.to_owned())))
                .collect(),
            _ => HashMap::new(),
        };
    }
    message
        .attributes
        .iter()
        .filter_map(|(name, attribute)| {
            if name.starts_with("queue.") || attribute.data_type.starts_with("Binary") {
                return None;
            }
            match &attribute.value {
                AttributeValue::Text(value) => Some((name.clone(), value.clone())),
                AttributeValue::Binary(_) => None,
            }
        })
        .collect()
}
