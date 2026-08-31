import test from "node:test";
import assert from "node:assert/strict";

import {
    approvalPresentation,
    approvalScopeLabel,
} from "../modules/view.js";

test("host approval summaries separate the human rationale from operation details", () => {
    assert.deepEqual(
        approvalPresentation(
            "Execute outside the workspace sandbox: git log; Reason: Inspect recent changes",
        ),
        {
            headline: "Inspect recent changes",
            operation: "Execute outside the workspace sandbox: git log",
        },
    );
});

test("approval presentation remains useful for unstructured tool summaries", () => {
    assert.deepEqual(approvalPresentation("Call MCP server 'seekee' Tool 'run'"), {
        headline: "允许 Orchestral 执行此操作？",
        operation: "Call MCP server 'seekee' Tool 'run'",
    });
});

test("approval scopes use concise user-facing labels without hiding exact targets", () => {
    assert.equal(approvalScopeLabel("host_execution"), "主机执行");
    assert.equal(approvalScopeLabel("filesystem_write:unrestricted"), "修改文件");
    assert.equal(approvalScopeLabel("network:api.example.com"), "访问网络 · api.example.com");
});
