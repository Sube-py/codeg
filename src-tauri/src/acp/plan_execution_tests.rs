use super::*;
use crate::acp::delegation::broker::{
    ConversationDepthLookup, DbChildStatusLookup, DelegationBroker, DelegationConfig,
};
use crate::acp::delegation::spawner::{
    mock::MockSpawner, AssistantLink, DelegationLink, ResumedSpawn,
};
use crate::acp::delegation::types::{
    ContinueDelegationRequest, DelegationAssistant, DelegationError, ResumeDelegationRequest,
    TaskStatus,
};
use crate::acp::manager::{
    ConnectionManager, ConnectionManagerPlanApprovalLookup, ConnectionManagerQuestionLookup,
};
use crate::db::service::{
    conversation_service,
    delegation_task_service::{self, DbDelegationTaskLedger, NewDelegationTask, TerminalUpdate},
};
use crate::db::{test_helpers, AppDatabase};
use crate::models::assistant::{selection, AssistantRunPhase};
use serde_json::{json, Value};

pub(super) fn assistant_with_policy(enabled: bool) -> DelegationAssistant {
    let _ = enabled;
    DelegationAssistant {
        id: 3,
        name: "Planner".into(),
        agent_options: selection(
            None,
            &[("collaboration_mode", "plan"), ("sandbox", "read-only")],
        ),
        after_plan_options: None,
        instructions: "Keep the scope small".into(),
    }
}

pub(super) fn injection(
    broker: Arc<DelegationBroker>,
    db: Arc<AppDatabase>,
    manager: Arc<ConnectionManager>,
) -> DelegationInjection {
    DelegationInjection {
        broker,
        tokens: Arc::new(crate::acp::delegation::listener::TokenRegistry::default()),
        socket_path: PathBuf::from("/tmp/plan-policy-unused.sock"),
        agent_availability: Arc::new(DbAgentAvailabilityLookup { db }),
        feedback: crate::acp::feedback::FeedbackRuntimeConfig::new(),
        ask: crate::acp::question::QuestionRuntimeConfig::new(),
        sessions: crate::acp::session_info::SessionInfoRuntimeConfig::new(),
        authoring: crate::acp::chat_authoring::ChatAuthoringRuntimeConfig::new(),
        browser: crate::acp::browser_tools::BrowserToolsRuntimeConfig::new(),
        computer: crate::acp::computer_tools::ComputerToolsRuntimeConfig::new(),
        questions: Arc::new(ConnectionManagerQuestionLookup {
            manager: manager.clone(),
        }),
        plan_approvals: Arc::new(ConnectionManagerPlanApprovalLookup { manager }),
    }
}

struct FlatDepth;

#[async_trait::async_trait]
impl ConversationDepthLookup for FlatDepth {
    async fn parent_of(&self, _id: i32) -> Result<Option<i32>, DelegationError> {
        Ok(None)
    }
}

struct ReplayHarness {
    db: Arc<AppDatabase>,
    broker: DelegationBroker,
    spawner: Arc<MockSpawner>,
    child_id: i32,
    snapshot: String,
}

impl ReplayHarness {
    async fn new(policy: bool, phase: Option<AssistantRunPhase>) -> Self {
        let db = Arc::new(test_helpers::fresh_in_memory_db().await);
        let folder = test_helpers::seed_folder(&db, "/tmp/run-policy").await;
        let parent = test_helpers::seed_conversation(&db, folder, AgentType::Codex).await;
        let assistant = assistant_with_policy(policy);
        let child = conversation_service::create_with_delegation(
            &db.conn,
            folder,
            AgentType::Codex,
            Some("Original plan".into()),
            None,
            Some(DelegationLink {
                parent_conversation_id: parent,
                parent_tool_use_id: "tool-1".into(),
                delegation_call_id: "task-1".into(),
                assistant: Some(AssistantLink {
                    id: assistant.id,
                    snapshot: assistant.run_snapshot(),
                }),
            }),
        )
        .await
        .unwrap();
        assert_eq!(child.assistant_run_phase.as_deref(), Some("planning"));
        conversation_service::bind_external_id(&db.conn, child.id, "session-1", &[])
            .await
            .unwrap();
        if let Some(phase) = phase {
            conversation_service::set_assistant_run_phase(&db.conn, child.id, phase)
                .await
                .unwrap();
        } else {
            use crate::db::entities::conversation;
            use sea_orm::sea_query::Expr;
            use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
            conversation::Entity::update_many()
                .col_expr(
                    conversation::Column::AssistantRunPhase,
                    Expr::value(Option::<String>::None),
                )
                .filter(conversation::Column::Id.eq(child.id))
                .exec(&db.conn)
                .await
                .unwrap();
        }
        delegation_task_service::insert(
            &db.conn,
            NewDelegationTask {
                task_id: "task-1".into(),
                source_task_id: None,
                request_hash: None,
                parent_conversation_id: parent,
                parent_tool_use_id: Some("tool-1".into()),
                child_conversation_id: child.id,
                agent_type: AgentType::Codex,
                external_session_id: Some("session-1".into()),
                working_dir: Some("/tmp/run-policy".into()),
                assistant: Some(assistant),
                task_preview: Some("Original plan".into()),
            },
        )
        .await
        .unwrap();
        delegation_task_service::finish(
            &db.conn,
            "task-1",
            TerminalUpdate {
                status: TaskStatus::Canceled,
                error_code: None,
                error_message: None,
                duration_ms: Some(1),
                result_preview: None,
                external_session_id: None,
            },
        )
        .await
        .unwrap();
        conversation_service::update_status(
            &db.conn,
            child.id,
            crate::db::entities::conversation::ConversationStatus::Cancelled,
        )
        .await
        .unwrap();
        let spawner = Arc::new(MockSpawner::new());
        let broker = DelegationBroker::new(spawner.clone(), Arc::new(FlatDepth))
            .with_ledger(Arc::new(DbDelegationTaskLedger { db: db.clone() }))
            .with_status_lookup(Arc::new(DbChildStatusLookup { db: db.clone() }));
        broker
            .set_config(DelegationConfig {
                enabled: true,
                ..Default::default()
            })
            .await;
        Self {
            db,
            broker,
            spawner,
            child_id: child.id,
            snapshot: child.assistant_snapshot.unwrap(),
        }
    }

    async fn assert_continue(&self, expected: &str) {
        self.spawner
            .queue_continue_spawn(Ok(ResumedSpawn::fresh("continued")))
            .await;
        self.spawner.queue_continue_send(Ok(())).await;
        let result = self
            .broker
            .continue_delegation(ContinueDelegationRequest {
                parent_connection_id: "parent".into(),
                parent_conversation_id: 1,
                parent_tool_use_id: "tool-next".into(),
                task_id: "task-1".into(),
                task: "Review implementation".into(),
                external_handle: None,
            })
            .await;
        assert_eq!(result.status, TaskStatus::Running, "{result:?}");
        let spawn = self.spawner.continue_spawn_args.lock().await[0].clone();
        assert_eq!(
            spawn.preferred_config_values["collaboration_mode"],
            expected
        );
        assert_eq!(spawn.preferred_config_values["sandbox"], "read-only");
        let send = self.spawner.continue_send_args.lock().await[0].clone();
        assert_eq!(send.child_conversation_id, self.child_id);
        assert_eq!(send.task, "Review implementation");
        let original = delegation_task_service::resolve(&self.db.conn, "task-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.status, TaskStatus::Canceled);
        assert_eq!(original.successor_task_id, result.task_id);
        use sea_orm::{EntityTrait, PaginatorTrait};
        assert_eq!(
            crate::db::entities::conversation::Entity::find()
                .count(&self.db.conn)
                .await
                .unwrap(),
            2
        );
        let (_, snapshot) = conversation_service::get_assistant_run(&self.db.conn, self.child_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_string(&snapshot.unwrap()).unwrap(),
            self.snapshot
        );
    }

    async fn assert_resume(&self, expected: &str) {
        self.spawner
            .queue_resume_spawn(Ok(ResumedSpawn::fresh("resumed")))
            .await;
        self.spawner.queue_resume_send(Ok(())).await;
        let result = self
            .broker
            .resume_delegation(ResumeDelegationRequest {
                parent_connection_id: "parent".into(),
                parent_conversation_id: 1,
                task_id: "task-1".into(),
                reason: None,
                external_handle: None,
            })
            .await;
        assert_eq!(result.status, TaskStatus::Running, "{result:?}");
        let spawn = self.spawner.resume_spawn_args.lock().await[0].clone();
        assert_eq!(
            spawn.preferred_config_values["collaboration_mode"],
            expected
        );
        assert_eq!(spawn.preferred_config_values["sandbox"], "read-only");
    }
}

#[tokio::test]
async fn continue_and_resume_select_frozen_options_by_run_phase() {
    for (phase, expected) in [
        (None, "plan"),
        (Some(AssistantRunPhase::Planning), "plan"),
        (Some(AssistantRunPhase::Executing), "default"),
    ] {
        ReplayHarness::new(false, phase)
            .await
            .assert_continue(expected)
            .await;
        ReplayHarness::new(false, phase)
            .await
            .assert_resume(expected)
            .await;
    }
}

#[tokio::test]
async fn execution_fallback_is_codex_only_and_preserves_sandbox() {
    use agent_client_protocol::schema::v1::SetSessionConfigOptionResponse;
    for (agent_type, mode, expected_calls) in [
        (AgentType::Codex, "plan", 1),
        (AgentType::Codex, "default", 0),
        (AgentType::ClaudeCode, "plan", 0),
    ] {
        let requests = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
        let log = requests.clone();
        let (client_end, agent_end) = agent_client_protocol::Channel::duplex();
        let agent = tokio::spawn(async move {
            Agent.builder().on_receive_request(async move |req: SetSessionConfigOptionRequest,
                responder: Responder<SetSessionConfigOptionResponse>, _cx: ConnectionTo<Client>| {
                log.lock().unwrap().push(serde_json::to_value(&req).unwrap());
                responder.respond(serde_json::from_value(json!({ "configOptions": [
                    { "id": "collaboration_mode", "name": "Mode", "type": "select", "currentValue": "default", "options": [] },
                    { "id": "sandbox", "name": "Sandbox", "type": "select", "currentValue": "read-only", "options": [] }
                ] })).unwrap())
            }, on_receive_request!()).connect_with(agent_end, async |_cx: ConnectionTo<Client>| {
                std::future::pending::<Result<(), agent_client_protocol::Error>>().await
            }).await
        });
        let state = Arc::new(RwLock::new(SessionState::new(
            "fallback".into(),
            agent_type,
            None,
            "test".into(),
            None,
        )));
        state.write().await.current_mode = Some(mode.into());
        Client
            .builder()
            .connect_with(client_end, async |cx: ConnectionTo<Agent>| {
                let mut fallback = PlanExecutionFallback::default();
                fallback.decided(Some(AssistantRunPhase::Executing));
                fallback.ready().await;
                fallback
                    .apply(&cx, &SessionId::new("session"), &state, &EventEmitter::Noop)
                    .await;
                Ok::<_, agent_client_protocol::Error>(())
            })
            .await
            .unwrap();
        agent.abort();
        let calls = requests.lock().unwrap().clone();
        assert_eq!(calls.len(), expected_calls);
        if let Some(request) = calls.first() {
            assert_eq!(request["configId"], "collaboration_mode");
            assert_eq!(request["value"], "default");
            let s = state.read().await;
            let sandbox = s
                .config_options
                .as_ref()
                .unwrap()
                .iter()
                .find(|o| o.id == "sandbox")
                .unwrap();
            assert!(
                matches!(&sandbox.kind, SessionConfigKindInfo::Select(value) if value.current_value == "read-only")
            );
        }
    }
}

#[tokio::test]
async fn codex_automatic_mode_update_disarms_the_fallback() {
    let notification: SessionNotification = serde_json::from_value(json!({
        "sessionId": "session", "update": { "sessionUpdate": "current_mode_update", "currentModeId": "default" }
    })).unwrap();
    let mut fallback = PlanExecutionFallback::default();
    fallback.decided(Some(AssistantRunPhase::Executing));
    fallback.observe(&notification.update);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), fallback.ready())
            .await
            .is_err()
    );
}

#[test]
fn executing_replaces_plan_with_default_and_leaves_other_phases() {
    for raw in [
        json!({"id": 1, "name": "Old", "agent_options": {"mode_id": "plan"}}),
        json!({"id": 1, "name": "Old", "agent_options": {"mode_id": "plan"}, "after_plan_options": null}),
        json!({"id": 1, "name": "Old", "agent_options": {"mode_id": "plan"}, "after_plan_options": {}}),
    ] {
        let assistant: DelegationAssistant = serde_json::from_value(raw).unwrap();
        for phase in [None, Some(AssistantRunPhase::Planning)] {
            assert_eq!(assistant.options_for_phase(phase), assistant.agent_options);
        }
        let executing = assistant.options_for_phase(Some(AssistantRunPhase::Executing));
        assert_eq!(executing.mode_id.as_deref(), Some("default"));
        let snapshot = serde_json::to_value(assistant.run_snapshot()).unwrap();
        assert!(snapshot.get("phase").is_none());
        assert!(snapshot.get("assistant_run_phase").is_none());
    }
}

#[tokio::test]
async fn a_manual_mode_change_cancels_the_pending_fallback() {
    let mut fallback = PlanExecutionFallback::default();
    fallback.decided(Some(AssistantRunPhase::Executing));
    fallback.cancel();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), fallback.ready())
            .await
            .is_err()
    );
}
