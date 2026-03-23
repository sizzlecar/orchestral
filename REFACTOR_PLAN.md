# Orchestral 重构计划与进度

## 目标

将 Orchestral 从当前的 Reactor/Recipe/Skeleton 架构重构为更简洁的 **外层 Agent Loop + 内层 mini-DAG** 模型。

## 5 阶段计划

### Phase 1: 移除非核心模块 (~700 行) ✅ 已完成

**目标**: 删除 ReferenceStore 及相关代码

**已完成的工作**:
- 从 `ExecutorContext` 中移除 `reference_store: Arc<dyn ReferenceStore>` 字段
- 删除 `NoopReferenceStore` 和 `InMemoryReferenceStore`
- 从 `Orchestrator` struct 中移除 `reference_store` 字段
- 清理 `ContextRequest` 中的 `include_references`, `ref_type_filter` 字段
- 清理 `ContextBuilderConfig` 中的 `reference_limit`, `include_summaries` 字段
- 删除 `ContextArtifact`, `ContentSource`, `EmbeddedReference` 等类型
- 更新所有测试文件中的 `ExecutorContext::new()` 调用
- 清理 `bootstrap.rs`, `planning.rs`, `execution.rs` 等文件

**受影响文件** (主要):
- `core/orchestral-core/src/executor/mod.rs`
- `core/orchestral-runtime/src/context/mod.rs`
- `core/orchestral-runtime/src/orchestrator.rs`
- `core/orchestral-runtime/src/orchestrator/execution.rs`
- `core/orchestral-runtime/src/orchestrator/planning.rs`
- `core/orchestral-runtime/src/bootstrap.rs`
- `core/orchestral-runtime/src/action/builtin/tests.rs`
- `core/orchestral-runtime/src/action/mcp.rs`
- `core/orchestral-runtime/src/agent/tests.rs`
- `examples/basic_usage.rs`, `examples/llm_planner.rs`

**验证**: `cargo check` 通过, `cargo test` 167 个测试全部通过

---

### Phase 2: 移除 Reactor/Recipe 系统 (~5,700 行) ✅ 已完成 (待最终验证)

**目标**: 删除整个 Reactor/Recipe/Skeleton/Stage 体系

**已完成的工作**:

#### 删除的文件/目录:
- `core/orchestral-core/src/recipe/` (整个目录: mod.rs, builtins.rs, overrides.rs, rewrite.rs, tests.rs)
- `core/orchestral-core/src/types/skeleton.rs`
- `core/orchestral-core/src/types/stage.rs`
- `core/orchestral-core/src/types/stage_plan.rs`
- `core/orchestral-runtime/src/orchestrator/reactor_loop.rs`
- `core/orchestral-runtime/src/orchestrator/reactor_choice.rs`
- `core/orchestral-runtime/src/orchestrator/reactor_resume.rs`
- `core/orchestral-runtime/src/orchestrator/reactor_lowering/` (整个目录)

#### 核心类型清理:
- `core/orchestral-core/src/types/mod.rs` — 移除 skeleton, stage, stage_plan 模块声明和所有 re-export
- `core/orchestral-core/src/types/task.rs` — 移除 `ReactorTaskState`, `ReactorFailureState`, `set_reactor_state()`
- `core/orchestral-core/src/types/continuation.rs` — 移除 `next_stage_hint: Option<StageKind>`
- `core/orchestral-core/src/lib.rs` — 移除 `pub mod recipe`, 所有 reactor/skeleton/stage re-export
- `core/orchestral-core/src/config/mod.rs` — 移除 `RecipesConfig`, `ReactorConfig`, `DerivationPolicy`
- `core/orchestral-core/src/config/loader.rs` — 移除 `validate_recipes()`, reactor 校验逻辑

#### PlanNormalizer 清理:
- `core/orchestral-core/src/normalizer/mod.rs` — 移除 `recipe_compiler` 字段, `register_recipe_template()`, `RecipeCompile` error variant

#### PlannerOutput 简化:
- `core/orchestral-core/src/planner/mod.rs` — `PlannerOutput` 只保留 `ActionCall`, `DirectResponse`, `Clarification`
- `core/orchestral-runtime/src/planner/llm/parsing.rs` — `PlannerJsonOutput` 对应简化
- `core/orchestral-runtime/src/planner/llm/prompt.rs` — 只保留 non-reactor prompt 函数
- `core/orchestral-runtime/src/planner/llm.rs` — 移除 reactor 相关代码路径

#### Orchestrator 清理:
- `core/orchestral-runtime/src/orchestrator/entry.rs` — 移除 reactor dispatch block
- `core/orchestral-runtime/src/orchestrator/planning.rs` — 只保留 ActionCall, DirectResponse, Clarification match arm
- `core/orchestral-runtime/src/bootstrap.rs` — 移除 recipe registry, reactor config 字段
- `core/orchestral-runtime/src/bootstrap/components.rs` — 清空 recipe registry 函数
- `core/orchestral-runtime/src/bootstrap/runtime_builder.rs` — 移除 reactor 相关配置

#### Action 文件临时处理:
- `action/document/assess.rs`, `action/spreadsheet/assess.rs`, `action/structured/assess.rs` — 添加了本地 `DerivationPolicy` enum (临时方案, Phase 5 进一步清理)
- `action/structured/derive.rs` — 同上
- `examples/llm_planner.rs` — 更新为使用 ACTION_CALL 示例

**当前状态**: `cargo check` 通过 (只有 unused import 警告), `cargo test` 需要最终确认

---

### Phase 3: 简化 Prompt 和 PlannerOutput (~1,500 行简化) 🟡 进行中 (~75%)

**目标**:
1. 简化 `PlannerOutput` 类型 (当前 3 种 → 可能调整为 4 种以支持 agent loop):
   - `SingleAction` — 单步执行 (替代当前 `ActionCall`)
   - `MiniPlan` — 多步 DAG 执行 (新增)
   - `NeedInput` — 需要用户输入 (替代当前 `Clarification`)
   - `Done` — 直接回复 (替代当前 `DirectResponse`)

2. 简化 prompt 系统:
   - 移除 reactor 相关的 prompt 模板文件 (`prompts/` 目录下的 reactor 相关 .md 文件)
   - 合并/精简 non-reactor prompt 构建逻辑
   - 更新 capability catalog 构建

3. 清理 action catalog 描述格式

**已完成的工作**:

#### PlannerOutput / JSON contract:
- `core/orchestral-core/src/planner/mod.rs`
  - `PlannerOutput` 已重命名为 `SingleAction`, `MiniPlan`, `Done`, `NeedInput`
  - 保留 `ActionCall = SingleAction` 兼容别名，降低调用点迁移风险
- `core/orchestral-runtime/src/planner/llm/parsing.rs`
  - 支持新 JSON 类型: `SINGLE_ACTION`, `MINI_PLAN`, `DONE`, `NEED_INPUT`
  - 保留 legacy alias: `ACTION_CALL`, `DIRECT_RESPONSE`, `CLARIFICATION`

#### Prompt 系统精简:
- `core/orchestral-runtime/src/planner/llm/prompt.rs`
  - `build_non_reactor_prompt()` → `build_planner_prompt()`
  - `build_non_reactor_system_prompt()` → `build_planner_system_prompt()`
- `core/orchestral-runtime/src/prompts/`
  - `non_reactor_constitution.md` → `planner_constitution.md`
  - `non_reactor_user_rules.md` → `planner_user_rules.md`
  - `action_call_rules.md` → `planner_execution_rules.md`
  - examples 改为 `single_action_*.json`, `done.json`, `need_input.json`, 新增 `mini_plan.json`
  - 删除未引用的 reactor prompt 模板和 reactor examples

#### Catalog / Orchestrator:
- `core/orchestral-runtime/src/planner/llm/catalog.rs`
  - `Reactor Pipelines` 文案替换为 `Multi-step Families`
  - 直接暴露 family 下的 concrete action names，便于后续 mini-plan/agent loop 使用
- `core/orchestral-runtime/src/orchestrator/planning.rs`
  - `SingleAction` 路径已接入新命名
  - `MiniPlan` 已可执行一个 concrete action DAG（禁止 `mcp__*`, 自动补默认 `on_failure`）
- `core/orchestral-runtime/src/bootstrap/runtime_builder.rs`
  - deterministic planner fallback 改为 `Done`
- `examples/llm_planner.rs`
  - 更新为新 PlannerOutput 命名

**当前剩余**:
- 评估 capability catalog 是否还要补更多 schema/role 信息，帮助 `MiniPlan` 更稳定地产生 family action DAG
- 运行更多 scenario / CLI 级验证，确认 prompt 输出迁移后端到端行为稳定
- README / 顶层文档仍有旧的 reactor-first 描述，后续需要统一

**关键文件**:
- `core/orchestral-core/src/planner/mod.rs`
- `core/orchestral-runtime/src/planner/llm/prompt.rs`
- `core/orchestral-runtime/src/planner/llm/catalog.rs`
- `core/orchestral-runtime/src/planner/llm/parsing.rs`
- `core/orchestral-runtime/src/orchestrator/planning.rs`
- `core/orchestral-runtime/prompts/*.md`

---

### Phase 4: 实现 Agent Loop (新增 ~500-800 行) 🟡 进行中 (~45%)

**目标**: 实现外层 agent loop, LLM 在每一轮决定下一步动作

**设计**:
```
用户意图 → Agent Loop 开始
  ↓
  LLM Planning (选择 SingleAction 或 MiniPlan)
  ↓
  执行 (单步 action 或 mini-DAG 并行执行)
  ↓
  观察结果
  ↓
  LLM 决定: 继续 / 完成 / 需要输入?
  ↓
  循环或退出
```

**已完成的工作**:
- `core/orchestral-core/src/planner/mod.rs`
  - 新增 `PlannerLoopContext`，把外层 loop 的观察结果作为 planner 上下文的一部分
- `core/orchestral-runtime/src/planner/llm/prompt.rs`
  - prompt 已支持 `Observed Execution State` 区块
  - planner 在后续轮次可见 `iteration/max_iterations`、recent observations、completed steps、working set 摘要
- `core/orchestral-runtime/src/orchestrator/agent_loop.rs`
  - 新增外层 loop 状态管理，负责 observation 累积、working set 摘要和迭代控制
- `core/orchestral-runtime/src/orchestrator/planning.rs`
  - `run_planning_pipeline()` 已升级为真正的 outer agent loop
  - planner 每轮可返回 `SingleAction / MiniPlan / Done / NeedInput`
  - `SingleAction` / `MiniPlan` 执行完成后不会立刻对用户返回，而是把观察结果喂回下一轮 planner
  - action 失败后允许在迭代预算内继续规划恢复
  - `NeedInput` 会清空 `task.plan`，确保用户补充信息后重新进入 planning 而不是误 resume 旧 plan
  - 增加 max iteration 限制；若执行完成但 planner 迟迟不给 `Done/NeedInput`，则显式 fail fast
- `core/orchestral-runtime/src/orchestrator/tests.rs`
  - 新增 3 个端到端测试:
    - `Completed -> replanning -> Done`
    - `Failed -> replanning -> NeedInput`
    - `iteration limit` 兜底失败
- `core/orchestral-core/src/config/mod.rs` / `core/orchestral-runtime/src/bootstrap.rs`
  - `max_planner_iterations` 已暴露到 runtime config，并接入 bootstrap
- `apps/orchestral-cli/src/scenario.rs` / `configs/scenarios/*.smoke.yaml`
  - CLI scenario 基线已从 `action_call/direct_response` 迁移到 `single_action/done`
  - 现有 direct-action smoke 现在也可作为最小 agent-loop smoke 使用
- `apps/orchestral-cli/src/runtime/client/submit.rs`
  - CLI persist 输出已按 `PlanningOutput` 语义渲染空计划结果：`done -> Plan: done`, `need_input -> Plan: need_input`
  - recoverable iteration failure 不再把整轮 turn/report 误标为 `Status: failed`
- `core/orchestral-runtime/src/planner/llm/catalog.rs` / `prompt.rs` / `planner_execution_rules.md`
  - `file_write` 已提升为 direct action，并新增 prompt/example 约束
  - 对 workspace 文件改写显式要求优先 `file_write`，避免 shell 重定向/覆盖命令触发 approval

**当前剩余**:
- context window 管理仍比较粗；目前是 history + observation block，后续可以更系统地做 truncation / summarization
- planner 失败恢复目前只做了“失败后可再规划”，还没有更细的 retry taxonomy
- 依赖真实 LLM/provider 的 CLI/scenario 端到端 smoke 已用 `.env.local + OPENROUTER_API_KEY` 跑通 direct-action 基线:
- 依赖真实 LLM/provider 的 CLI/scenario 端到端 smoke 已用 `.env.local + OPENROUTER_API_KEY` 跑通 direct-action 基线:
  - `configs/scenarios/action_call_read_file.smoke.yaml`
  - `configs/scenarios/action_call_list_docs.smoke.yaml`
- 多步 family 路径也已跑通最小 structured smoke:
  - `configs/scenarios/builtin_structured_patch.smoke.yaml`
- 仍需补 spreadsheet / document / skill 类真实 provider smoke，确认更重的 family 路径和外部 skill 接入同样稳定

**关键新文件**:
- `core/orchestral-runtime/src/orchestrator/agent_loop.rs` — 主循环逻辑
- 可能更新 `LlmClient` trait 以支持 tool-use 模式的循环调用

**注意事项**:
- Agent loop 的 max iteration 限制
- 每轮的 context window 管理 (history truncation)
- 错误恢复策略 (action 失败后是否让 LLM 重试)

---

### Phase 5: 清理与验证 🔲 未开始

**目标**: 最终清理和验证

**待办**:
1. 清理 assess.rs 文件中的临时 `DerivationPolicy` enum (Phase 2 遗留)
2. 移除所有 unused import 警告
3. 移除 dead code (如 `parse_working_set_value`, `require_working_set_string` 等)
4. 清理 prompts/ 目录中可能残留的 reactor 相关模板
5. 更新 YAML 配置示例, 移除 reactor/recipe 相关字段
6. 运行完整测试套件并确保全部通过
7. 可选: 运行 `cargo clippy` 清理 lint

---

## 架构对比

### 当前架构 (移除 Reactor 后):
```
Intent → LLM Planner → ActionCall / DirectResponse / Clarification
                          ↓
                     单步 Action 执行
                          ↓
                        结果返回
```

### 目标架构 (Phase 4 完成后):
```
Intent → Agent Loop
           ↓
         LLM Planning → SingleAction / MiniPlan / NeedInput / Done
           ↓                ↓              ↓          ↓        ↓
         执行单步      构建 mini-DAG    等待用户    返回结果
           ↓              ↓
         观察结果     并行执行
           ↓              ↓
         回到 Agent Loop 继续
```

## 当前进度总结

- **Phase 1**: ✅ 100% 完成
- **Phase 2**: ✅ ~98% 完成 (cargo check 通过, cargo test 需最终确认, assess.rs 临时方案待 Phase 5 清理)
- **Phase 3**: 🟡 ~75% 完成
- **Phase 4**: 🟡 ~45% 完成
- **Phase 5**: 🔲 0%

## 重要注意事项

1. `configs/orchestral.cli.runtime.override.yaml` 有本地修改 (git status 显示), 注意不要覆盖
2. 当前分支: `feat/min-config-mcp-skill`
3. assess.rs 文件 (document/spreadsheet/structured) 中有本地 `DerivationPolicy` enum, 这是临时方案 — 这些 action 产生的 `ContinuationState` 原本是 reactor 工作流的一部分, Phase 5 需要决定是保留还是进一步简化
4. `core/orchestral-runtime/src/orchestrator.rs` 中有几个 dead code 函数 (`parse_working_set_value`, `require_working_set_string`, `require_working_set_string_array`, `summarize_working_set`) 可能在 Phase 4 agent loop 中会用到, 也可能不需要, Phase 5 最终决定
5. 顶层 README 仍描述旧的 reactor-first 状态，当前代码实际已经进入 planner output / mini-plan 迁移阶段
