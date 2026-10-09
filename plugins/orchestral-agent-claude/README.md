# Claude Code connector

The CLI registers `claude/local` alongside `codex/local`. The PWA discovers its
session group and creation form through the existing Host API.

- Discover local Claude project transcripts and live terminal sessions.
- Read messages, reasoning, tool results and compaction boundaries with stable
  activity identities and explicit observation times. Older history is paginated.
- Observe existing terminal sessions over the Host's shared SSE stream. The
  adapter checks main-session journal metadata and native process/approval state
  every 500 ms, publishing stable activity, turn and request changes. History
  rewrites and metadata changes explicitly require one snapshot reconciliation.
- Send composer messages to an existing terminal through its authenticated local
  inbox, with native journal confirmation and durable at-most-once dispatch claims.
  Claude retains peer origin and appends its cross-session explanation.
- Resolve existing terminal tool permissions from the PWA through the official
  `PermissionRequest` hook (`orchestral serve --claude-approvals`).
- Create a session in a selected local directory, or resume a saved session.
- Stream responses, resolve tool approvals and `AskUserQuestion`, and interrupt
  Host-owned runs through Claude's bidirectional `stream-json` SDK protocol.

Existing interactive processes remain the sole execution owner. The PWA sends
messages to their inbox and follows their transcript; it does not attach a second
SDK execution owner to those processes.
Their local inbox always assigns peer origin and Claude's peer-message context;
socket authentication does not make that origin direct user input. The connector
discloses this behavior while keeping sending available. Initial acceptance emits
no delivery receipt: the adapter confirms the submission ID in a committed native
user record or a peer `queued_command` attachment absorbed during a running turn.
This confirms receipt into the conversation, not completion of the requested work.
After a complete socket write, a delayed confirmation is reported as `submitted`,
not a send failure. A durable pending message stays visible in the session until
the native journal echoes its submission identity; messages received mid-turn
are rendered in that same turn. Partial writes remain explicitly uncertain.
Held and refused messages use authenticated, correlated native receipts. Uncertain
dispatches are never retried; later journal evidence repairs their saved status
on replay or session refresh, including after a Host restart. With `--claude-approvals`, native tool
permission requests appear in the same PWA approval cards. A decision from either
client closes the request, and conflicting or stale decisions are rejected.
Native terminal cancellation remains in that terminal. Messages retain the native peer-message permission
boundary and do not execute slash commands. Host-owned runs support text input; attachments,
steering and reconnection after a Host restart are not advertised.

The connector uses native Claude authentication and project settings. It launches
with manual permissions and never enables bypass mode. Provider-managed effects
are declared explicitly; it does not claim Host sandbox enforcement.

Configuration:

| Setting | Default |
| --- | --- |
| `ORCHESTRAL_CLAUDE_PATH` | `claude` on PATH, then `~/.local/bin/claude` |
| `CLAUDE_CONFIG_DIR` | `~/.claude` |
| Host metadata and Run journal | `$ORCHESTRAL_HOME/agent-connectors/claude-local/` |

The current CLI integration targets Claude Code's manual-permission streaming
interface, verified with 2.1.291. It has no Node or Python runtime dependency.
The protocol follows Anthropic's [SDK transport and control implementation](https://github.com/anthropics/claude-agent-sdk-python/tree/main/src/claude_agent_sdk/_internal).
Live terminal input uses Claude Code's [local cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging)
and retains its inbound controls. The adapter verifies native delivery evidence;
this interface is distinct from Codex's shared app-server protocol.
The official [Remote Control user channel](https://code.claude.com/docs/en/remote-control)
can preserve an existing terminal session, but requires enabling Remote Control
and an eligible claude.ai login. It is not implemented by this adapter.

The approval bridge installs one [PermissionRequest command hook](https://code.claude.com/docs/en/hooks#permissionrequest)
in Claude's user settings, preserves other settings and hooks, and backs up the
previous file under the Host's `claude-local/hook-backups/` directory. Claude
normally reloads settings in an existing session. The hook connects through a
private Unix socket and returns only the explicit, session-bound PWA decision;
it does not change permission modes or add allow rules. If the Host is offline,
the hook returns no decision and Claude keeps its native permission UI. SDK-owned
Runs keep their original control-request path and do not create a second card.

Local validation:

```sh
cargo test -p orchestral-agent-claude
cargo run -p orchestral-agent-claude --example inspect_sessions
```

The tests use local protocol fixtures. The example lists local session metadata
and validates transcripts without making model requests.
