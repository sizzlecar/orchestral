use std::error::Error;
use std::fs;
use std::path::Path;

use orchestral_agent_protocol_testkit::schema_snapshot::{
    render_wire_schema_bundle, WIRE_SCHEMA_SNAPSHOT_PATH,
};

fn main() -> Result<(), Box<dyn Error>> {
    let snapshot_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(WIRE_SCHEMA_SNAPSHOT_PATH);
    let rendered = render_wire_schema_bundle()?;
    fs::write(&snapshot_path, rendered)?;
    println!("updated {}", snapshot_path.display());
    Ok(())
}
