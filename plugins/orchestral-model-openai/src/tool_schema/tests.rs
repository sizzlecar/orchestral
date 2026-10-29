use super::*;

#[test]
fn object_projection_keeps_fields_and_leaves_canonical_constraints_unchanged() {
    let canonical = json!({
        "type":"object", "required":["target"], "additionalProperties":false,
        "properties": {
            "target":{"type":"string","minLength":1},
            "source":{"type":"string"},
            "changes":{"type":"array","items":{"type":"object","properties":{"value":{"type":"string"}}}}
        },
        "oneOf":[{"required":["source"],"not":{"required":["changes"]}},{"required":["changes"]}]
    });
    let original = canonical.clone();
    let wire = parameters(&canonical).unwrap();
    assert_eq!(wire["type"], "object");
    assert_eq!(wire["properties"], canonical["properties"]);
    assert_eq!(wire["required"], canonical["required"]);
    assert_eq!(wire["additionalProperties"], false);
    assert!(ROOT_CONSTRAINTS.iter().all(|key| wire.get(key).is_none()));
    assert_eq!(canonical, original);
}

#[test]
fn composed_object_variants_preserve_all_declared_fields_without_requiring_both_forms() {
    let wire = parameters(&json!({"oneOf":[
        {"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false},
        {"type":"object","properties":{"ids":{"type":"array","items":{"type":"integer"}}},"required":["ids"],"additionalProperties":false}
    ]})).unwrap();
    assert_eq!(wire["properties"]["query"], json!({"type":"string"}));
    assert_eq!(wire["properties"]["ids"]["type"], "array");
    assert!(wire.get("required").is_none());
    assert!(wire.get("oneOf").is_none());
    let open = parameters(&json!({"anyOf":[
        {"properties":{"value":{"type":"string"}}},
        {"properties":{"other":{"type":"integer"}}}
    ]}))
    .unwrap();
    assert_eq!(open["properties"]["value"], json!({}));
}

#[test]
fn root_constraints_and_local_definitions_use_the_same_object_projection() {
    for key in ["allOf", "anyOf", "oneOf"] {
        let mut schema =
            json!({"$defs":{"Input":{"type":"object","properties":{"value":{"type":"string"}}}}});
        schema[key] = json!([{"$ref":"#/$defs/Input"}]);
        let wire = parameters(&schema).unwrap();
        assert_eq!(wire["properties"]["value"]["type"], "string");
        assert_eq!(wire["$defs"], schema["$defs"]);
        assert!(wire.get(key).is_none());
    }
    let literal = parameters(&json!({"const":{"kind":"single"}})).unwrap();
    assert_eq!(literal["properties"]["kind"], json!({"enum":["single"]}));
    assert!(literal.get("const").is_none());
    let enumeration = parameters(&json!({"enum":[{"kind":"first"},{"kind":"second"}]})).unwrap();
    assert_eq!(
        enumeration["properties"]["kind"]["enum"],
        json!(["first", "second"])
    );
    let negated = parameters(&json!({"type":"object","properties":{"value":{"type":"string"}},"not":{"required":["value"]}})).unwrap();
    assert!(negated.get("not").is_none());
}

#[test]
fn unsupported_shapes_and_unavailable_references_fail_before_network_dispatch() {
    for schema in [
        json!({"type":"array"}),
        json!({"$ref":"#"}),
        json!({"$ref":"https://example.invalid/schema"}),
        json!({"type":"object","properties":{"value":{"$ref":"#/oneOf/0"}},"oneOf":[{"type":"object"}]}),
    ] {
        assert_eq!(
            parameters(&schema).unwrap_err().code,
            ModelErrorCode::Unsupported
        );
    }
}
