use super::*;

/// Executor-declared bytes shown by a verified Artifact reader. This is read
/// evidence only, not authority to access an Artifact or mutate its producer.
#[derive(Debug, Clone)]
pub struct ArtifactReadObservation {
    pub artifact: ArtifactRefWithDigest,
    pub media_type: String,
    pub byte_size: u64,
    pub offset: u64,
    pub content: String,
}

pub(crate) fn artifact_model_output(artifact: &ToolArtifact) -> serde_json::Value {
    serde_json::json!({
        "kind": "artifact",
        "artifact": artifact.artifact,
        "media_type": artifact.media_type,
        "byte_size": artifact.byte_size,
        "summary": artifact.summary,
    })
}

pub(super) fn fit_artifact_summary(
    artifact: &mut ToolArtifact,
    maximum: u64,
) -> Result<(), ToolArtifactError> {
    let size = |artifact: &ToolArtifact| {
        serde_jcs::to_vec(&artifact_model_output(artifact))
            .map(|bytes| bytes.len() as u64)
            .map_err(|error| ToolArtifactError::Integrity(error.to_string()))
    };
    if size(artifact)? <= maximum {
        return Ok(());
    }
    let summary = std::mem::replace(&mut artifact.summary, "…".to_owned());
    let minimum = size(artifact)?;
    if minimum > maximum {
        return Err(ToolArtifactError::LimitExceeded {
            observed: minimum,
            maximum,
        });
    }
    let boundaries = std::iter::once(0)
        .chain(
            summary
                .char_indices()
                .map(|(offset, ch)| offset + ch.len_utf8()),
        )
        .collect::<Vec<_>>();
    let mut low = 0;
    let mut high = boundaries.len() - 1;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        artifact.summary = format!("{}…", &summary[..boundaries[middle]]);
        if size(artifact)? <= maximum {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    artifact.summary = format!("{}…", &summary[..boundaries[low]]);
    Ok(())
}

pub(super) fn observed_artifact_output(
    artifact: &ToolArtifact,
    observations: &[ArtifactReadObservation],
) -> Option<serde_json::Value> {
    if artifact.media_type != "application/json" || artifact.validate().is_err() {
        return None;
    }
    let mut pages = observations
        .iter()
        .filter(|page| {
            page.artifact == artifact.artifact
                && page.media_type == artifact.media_type
                && page.byte_size == artifact.byte_size
        })
        .collect::<Vec<_>>();
    pages.sort_by_key(|page| page.offset);
    let mut bytes = Vec::new();
    for page in pages {
        let start = usize::try_from(page.offset).ok()?;
        let end = page.offset.checked_add(page.content.len() as u64)?;
        if start > bytes.len() || end > artifact.byte_size {
            return None;
        }
        let overlap = (bytes.len() - start).min(page.content.len());
        if bytes[start..start + overlap] != page.content.as_bytes()[..overlap] {
            return None;
        }
        bytes.extend_from_slice(&page.content.as_bytes()[overlap..]);
    }
    if bytes.len() as u64 != artifact.byte_size
        || Digest::sha256(&bytes) != artifact.artifact.digest
    {
        return None;
    }
    let output = serde_json::from_slice(&bytes).ok()?;
    // Only the producer's original canonical result, not a differently encoded
    // value or a synthesized summary, can supply complete-read evidence.
    (serde_jcs::to_vec(&output).ok()? == bytes).then_some(output)
}
