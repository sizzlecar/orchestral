use std::sync::Arc;

use async_trait::async_trait;
use orchestral_runtime::action::{
    Action, ActionBuildError, ActionContext, ActionFactory, ActionInput, ActionInterfaceSpec,
    ActionMeta, ActionResult, ActionSpec,
};

use super::convert::DocConvertAction;
use super::generate::DocGenerateAction;
use super::merge::DocMergeAction;
use super::parse::DocParseAction;
use super::qa::DocQaAction;
use super::summarize::DocSummarizeAction;

pub(super) fn build_document_action(spec: &ActionSpec) -> Option<Box<dyn Action>> {
    match spec.kind.as_str() {
        "doc_parse" => Some(Box::new(DocParseAction::from_spec(spec))),
        "doc_convert" => Some(Box::new(DocConvertAction::from_spec(spec))),
        "doc_summarize" => Some(Box::new(DocSummarizeAction::from_spec(spec))),
        "doc_generate" => Some(Box::new(DocGenerateAction::from_spec(spec))),
        "doc_qa" => Some(Box::new(DocQaAction::from_spec(spec))),
        "doc_merge" => Some(Box::new(DocMergeAction::from_spec(spec))),
        _ => None,
    }
}

pub struct DocsAssistantActionFactory;

impl DocsAssistantActionFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DocsAssistantActionFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ActionFactory for DocsAssistantActionFactory {
    fn build(&self, spec: &ActionSpec) -> Result<Arc<dyn Action>, ActionBuildError> {
        let action = build_document_action(spec)
            .ok_or_else(|| ActionBuildError::UnknownKind(spec.kind.clone()))?;
        let action: Arc<dyn Action> = Arc::from(action);
        Ok(Arc::new(ConfiguredAction::new(action, spec)))
    }
}

struct ConfiguredAction {
    inner: Arc<dyn Action>,
    metadata: ActionMeta,
}

impl ConfiguredAction {
    fn new(inner: Arc<dyn Action>, spec: &ActionSpec) -> Self {
        let metadata = merge_action_metadata(inner.metadata(), spec.interface.as_ref());
        Self { inner, metadata }
    }
}

#[async_trait]
impl Action for ConfiguredAction {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn description(&self) -> &str {
        self.inner.description()
    }

    fn metadata(&self) -> ActionMeta {
        self.metadata.clone()
    }

    async fn run(&self, input: ActionInput, ctx: ActionContext) -> ActionResult {
        self.inner.run(input, ctx).await
    }
}

fn merge_action_metadata(base: ActionMeta, interface: Option<&ActionInterfaceSpec>) -> ActionMeta {
    let Some(interface) = interface else {
        return base;
    };

    let mut merged = base;
    if !interface.input_schema.is_null() {
        merged.input_schema = interface.input_schema.clone();
    }
    if !interface.output_schema.is_null() {
        merged.output_schema = interface.output_schema.clone();
    }
    merged
}
