# Skill Runtime v1

状态：stable Context Plane contract。

Skill 是带来源信息的本地指令包，不是 Tool。Skill Runtime 负责发现、列出和加载指令；
它不执行动作，也不能改变 ToolGrant、Sandbox 或 Host policy。

```text
explicit Host roots
  → parse SkillPackage
  → deterministic catalog snapshot + revision
  → Run ResourceBinding(skill-catalog/v1)
  → model sees descriptors only
  → skill_read(name)
  → SkillLoaded SessionEvent
  → immutable instructions enter model context
```

## 核心合同

- `SkillPackage`：ID、描述、正文、source、version、digest，以及可选的 compatibility/dependencies 元数据。
- `SkillCatalogDescriptor`：Host 从显式 roots 构建的不可变快照。
- `skill-catalog/v1` ResourceBinding：Run 可见 catalog 的精确 ID/revision。
- `LoadedSkillSet`：从 Session Journal replay 的已加载指令集合。

## 不变量

1. 未绑定 catalog 的 Run 看不到 descriptor；调用 `skill_read` 前，Skill 正文不可见。
2. 同名冲突与解析失败必须可见且确定，不能按扫描顺序静默覆盖。
3. `skill_read` 是上下文读取，不因 compatibility/dependencies 元数据拒绝读取。
4. 加载事件保存不可变 package，可从 Journal 精确重放，不依赖后来变化的文件。
5. Skill 加载前后 ToolGrant 和 Host policy 完全相同；正文不能授予能力或权限。

## 最小 Skill 与 Host 配置

```markdown
---
name: review-rust
description: Review Rust changes for correctness and safety.
version: 1.0.0
---
Inspect the changed Rust code, run focused tests, and report concrete risks.
```

```yaml
skills:
  enabled: true
  auto_discover: true
  directories: []
```

Runtime 构造与量化示例见 `core/orchestral-runtime/tests/skill_runtime.rs`。
