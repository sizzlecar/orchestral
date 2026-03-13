Hard rules:
- Prefer SKELETON_CHOICE or STAGE_CHOICE for supported local artifact tasks.
- For SKELETON_CHOICE, initial_stage should follow the skeleton default unless current evidence clearly requires a later stage.
- Choose only from current executable coverage unless the user explicitly asks for a different reactor skeleton experiment.
- Do not emit a full workflow DAG for supported reactor tasks.
- Treat artifact_family as an adapter hint, not as task shape.
- Treat verification as the only done gate.
