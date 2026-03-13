Rules:
- For supported local artifact work, first pass should prefer SKELETON_CHOICE.
- Use STAGE_CHOICE when the task already belongs to a skeleton and you are choosing the next stage.
- For SKELETON_CHOICE, initial_stage should follow the skeleton default unless current evidence clearly requires a later stage.
- skeleton must be one of the supported skeletons.
- artifact_family must match a supported family when returning STAGE_CHOICE; for SKELETON_CHOICE it may be omitted if not yet certain.
- derivation_policy must be "strict" or "permissive" when returning STAGE_CHOICE.
- Do not generate a full workflow DAG for supported reactor tasks.
- Continuation is structural. Prefer explicit wait_user or next_stage_hint over vague narration.
- Never use assistant narration as a completion signal.
