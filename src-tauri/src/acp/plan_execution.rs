//! Plan approval changes run policy, not the frozen assistant or sandbox.

use super::*;
use crate::models::assistant::AssistantRunPhase;

pub(super) async fn prepare_plan_decision(
    queue: &mut PermissionQueue,
    state: &Arc<RwLock<SessionState>>,
    injection: Option<&DelegationInjection>,
    request_id: &str,
    option_id: &str,
) -> Result<Option<AssistantRunPhase>, crate::db::error::DbError> {
    let Some(parked) = queue.responders.get(request_id) else {
        return Ok(None);
    };
    if !parked.codex_plan_review
        || parked.responder.is_withdrawn()
        || !parked.options.iter().any(|o| o.option_id == option_id)
    {
        return Ok(None);
    }
    let phase = match option_id {
        "implement_plan" => AssistantRunPhase::Executing,
        "revise_plan" => AssistantRunPhase::Planning,
        _ => return Ok(None),
    };
    let s = state.read().await;
    if s.agent_type != AgentType::Codex {
        return Ok(None);
    }
    let conversation_id = s.conversation_id;
    drop(s);
    if let (Some(injection), Some(id)) = (injection, conversation_id) {
        // Persist before releasing the responder: even an immediate end_turn
        // or disconnect must not allow a reconnect to replay startup plan.
        injection.broker.set_run_phase(id, phase).await?;
    }
    state
        .write()
        .await
        .plan_revisions
        .review_decided(phase == AssistantRunPhase::Planning);
    Ok(Some(phase))
}

#[derive(Default)]
pub(super) struct PlanExecutionFallback {
    deadline: Option<tokio::time::Instant>,
}

impl PlanExecutionFallback {
    pub(super) fn decided(&mut self, phase: Option<AssistantRunPhase>) {
        if let Some(phase) = phase {
            self.deadline = (phase == AssistantRunPhase::Executing)
                .then(|| tokio::time::Instant::now() + std::time::Duration::from_millis(250));
        }
    }

    pub(super) fn cancel(&mut self) {
        self.deadline = None;
    }

    pub(super) fn observe(&mut self, update: &SessionUpdate) {
        let switched = match update {
            SessionUpdate::CurrentModeUpdate(update) => {
                update.current_mode_id.to_string() != "plan"
            }
            SessionUpdate::ConfigOptionUpdate(update) => update.config_options.iter().any(|o| {
                o.id.to_string() == "collaboration_mode" && !config_option_already_holds(o, "plan")
            }),
            _ => false,
        };
        if switched {
            self.cancel();
        }
    }

    pub(super) async fn ready(&self) {
        match self.deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    }

    pub(super) async fn apply(
        &mut self,
        cx: &ConnectionTo<Agent>,
        session_id: &SessionId,
        state: &Arc<RwLock<SessionState>>,
        emitter: &EventEmitter,
    ) {
        self.cancel();
        let s = state.read().await;
        // The config channel is authoritative when offered by Codex. Older
        // bridges expose only current_mode. Unknown state never forces a set.
        let collaboration_mode = s.config_options.as_ref().and_then(|options| {
            options
                .iter()
                .find(|o| o.id == "collaboration_mode")
                .and_then(|o| match &o.kind {
                    SessionConfigKindInfo::Select(select) => Some(select.current_value.as_str()),
                    _ => None,
                })
        });
        let needs_fallback = s.agent_type == AgentType::Codex
            && collaboration_mode.or(s.current_mode.as_deref()) == Some("plan");
        drop(s);
        if needs_fallback {
            if let Err(e) = set_config_option_for_agent(
                AgentType::Codex,
                cx,
                session_id,
                state,
                emitter,
                "collaboration_mode".into(),
                "default".into(),
            )
            .await
            {
                tracing::warn!("[ACP] Codex plan execution fallback failed: {e}");
            }
        }
    }
}
