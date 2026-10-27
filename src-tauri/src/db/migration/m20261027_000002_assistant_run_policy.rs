use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Assistant::Table)
                    .add_column(ColumnDef::new(Assistant::AfterPlanOptions).text().null())
                    .to_owned(),
            )
            .await?;
        // NULL deliberately preserves pre-policy replay on existing runs.
        manager
            .alter_table(
                Table::alter()
                    .table(Conversation::Table)
                    .add_column(
                        ColumnDef::new(Conversation::AssistantRunPhase)
                            .text()
                            .null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Conversation::Table)
                    .drop_column(Conversation::AssistantRunPhase)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Assistant::Table)
                    .drop_column(Assistant::AfterPlanOptions)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Assistant {
    Table,
    AfterPlanOptions,
}

#[derive(DeriveIden)]
enum Conversation {
    Table,
    AssistantRunPhase,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        entities::{assistant, conversation},
        migration::Migrator,
    };
    use sea_orm::{ConnectionTrait, Database, DbBackend, EntityTrait, Statement};

    #[tokio::test]
    async fn legacy_rows_keep_null_policy_and_phase_after_upgrade_and_rollback() {
        let conn = Database::connect("sqlite::memory:").await.unwrap();
        let migrations = Migrator::migrations();
        let index = migrations
            .iter()
            .position(|m| m.name() == Migration.name())
            .unwrap();
        Migrator::up(&conn, Some(index as u32)).await.unwrap();
        for sql in [
            "INSERT INTO assistant (id, uid, name, avatar, agent_type, agent_options, label_snapshot, created_at, updated_at) VALUES (1, 'old', 'Old', '{}', 'codex', '{\"mode_id\":\"plan\"}', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO folder (id, name, path, last_opened_at, created_at, updated_at) VALUES (1, 'repo', '/tmp/repo', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO conversation (id, folder_id, agent_type, status, kind, created_at, updated_at) VALUES (1, 1, 'codex', 'cancelled', 'delegate', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        ] {
            conn.execute(Statement::from_string(DbBackend::Sqlite, sql)).await.unwrap();
        }
        for _ in 0..2 {
            Migrator::up(&conn, None).await.unwrap();
            let assistant = assistant::Entity::find_by_id(1)
                .one(&conn)
                .await
                .unwrap()
                .unwrap();
            assert!(assistant.after_plan_options.is_none());
            assert_eq!(assistant.agent_options, r#"{"mode_id":"plan"}"#);
            let child = conversation::Entity::find_by_id(1)
                .one(&conn)
                .await
                .unwrap()
                .unwrap();
            assert!(child.assistant_run_phase.is_none());
            Migrator::down(&conn, Some(1)).await.unwrap();
        }
    }
}
