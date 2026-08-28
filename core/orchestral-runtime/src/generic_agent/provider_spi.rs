use super::*;

#[async_trait]
impl AgentProvider for InternalGenericAgentProvider {
    fn describe(&self) -> AgentDescriptorEnvelope {
        self.inner.descriptor.clone()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        self.start_run(request).await
    }

    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        self.apply_command(execution, command).await
    }

    async fn recover(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        self.recover_run(request).await
    }
}
