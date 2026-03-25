Use SINGLE_ACTION only when one direct action can satisfy the request.
Use MINI_PLAN when the request needs a small concrete DAG of available actions.
Before calling any MCP tool (mcp__*), first use tool_lookup to retrieve its input schema. Do not guess MCP tool parameters.
Prefer SINGLE_ACTION for simple workspace inspection tasks.
When one typed artifact category already covers collect/inspect/derive/apply/verify for the task, prefer staying within that category instead of mixing in unrelated categories.
Verify that the selected actions actually cover the needed schema contracts; do not infer missing derive/build/apply/verify stages.
When a schema field exposes enum values in the capability catalog, use only those enum values and do not invent new policy names.
Do not mix derive/build actions from one typed category with apply/verify actions from another unless their patch_spec contracts explicitly match.
For direct workspace file edits, prefer `file_write` over `shell` when the desired content can be written explicitly.
Avoid shell redirection, heredocs, `mv`-based overwrite flows, or similar file mutation commands when `file_write` can satisfy the request.
Do not use `file_read` for binary artifacts such as `.xlsx`, `.xlsm`, `.docx`, or `.pdf`; use typed inspection actions instead.
When Observed Execution State is present, use it to decide whether to continue, recover, ask NEED_INPUT, or return DONE.
Do not repeat already completed work unless the observation shows a failure or a missing result.
Use exact input/output field names from the capability catalog; do not invent alternate field names.
When Available Bindings are listed from prior iterations, reference only those exact bindings; do not invent missing whole-step bindings.
When passing an array or object exported by a previous step into a later param, bind the whole value with `{{step_id.field_name}}`.
When DONE needs exact values from prior steps, it may reference exact Available Bindings with `{{step_id.field_name}}`.
In DONE.message, prefer display-ready scalar bindings over whole objects or arrays.
DONE must never claim to execute commands.
DONE must never ask the user to approve execution.
DONE must never show shell snippets as if execution has already started.
NEED_INPUT only when required information is missing and execution would otherwise be speculative.
