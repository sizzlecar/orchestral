# Skill Runtime v1

状态：stable Context Plane contract。

Skill 是带来源和信任信息的指令包，不是 Tool。Skill Runtime 只控制哪些文本可以进入模型
Context；它不执行远程调用，也不能改变 ToolGrant、Sandbox 或 Host policy。

```text
explicit Host roots
  → parse SkillPackage
  → deterministic catalog snapshot + revision
  → Run ResourceBinding(skill-catalog/v1)
  → model sees descriptors only
  → orchestral_skill_activate request
  → trust / compatibility / dependency / digest checks
  → SkillActivated SessionEvent
  → full instructions enter this Run context
```

## 核心合同

- `SkillPackage`：ID、描述、正文、source、trust、version、digest、compatibility、dependencies。
- `SkillCatalogDescriptor`：Host 从显式 roots 构建的不可变快照。
- `skill-catalog/v1` ResourceBinding：Run 可见 catalog 的精确 ID/revision。
- `SkillActivationPolicy`：允许的 source、trust 与 compatibility 边界。
- `ActivatedSkillSet`：从 Session Journal replay 的当前 Run 激活状态。

## 不变量

1. 未绑定 catalog 的 Run 看不到 descriptor，未激活 Skill 的全文进入 Context 次数为 0。
2. 同名冲突与解析失败必须可见且确定，不能按扫描顺序静默覆盖。
3. workspace Skill 默认不受信任；digest 变化、依赖或 compatibility 不满足时拒绝激活。
4. 激活事件记录 source、trust、version/digest 与 reason，可从 Journal 重建。
5. Skill 激活前后 ToolGrant 和 Host policy 完全相同。
6. 后续 Run 不因同 Session 的旧激活而自动继承全文。

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
  roots:
    - path: ./skills
      source: workspace
      trusted: false
```

Runtime 构造与量化示例见 `core/orchestral-runtime/tests/skill_runtime.rs`。
