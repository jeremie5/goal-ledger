use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

const APPLICATION_ID: i64 = 0x474c_4447; // "GLDG"
const SCHEMA_VERSION: i64 = 2;
const MAX_LIST_LIMIT: u32 = 500;

const REQUEST_COLUMNS_V1: &[&str] = &[
    "session_id",
    "request_id",
    "turn_id",
    "revision",
    "status",
    "summary",
    "requirements_json",
    "next_action",
    "evidence_json",
    "created_at_ms",
    "updated_at_ms",
];
const REQUEST_COLUMNS_V2: &[&str] = &[
    "session_id",
    "request_id",
    "turn_id",
    "revision",
    "status",
    "summary",
    "requirements_json",
    "next_action",
    "evidence_json",
    "created_at_ms",
    "updated_at_ms",
    "admission_offset",
];
const TRANSCRIPT_COLUMNS: &[&str] = &["session_id", "path", "updated_at_ms"];

const SELECT_REQUEST: &str = "SELECT session_id, request_id, turn_id, revision, status, \
    summary, requirements_json, next_action, evidence_json FROM requests \
    WHERE session_id = ?1 AND request_id = ?2";

const SELECT_LATEST_ADMITTED: &str = "SELECT session_id, request_id, turn_id, revision, status, \
    summary, requirements_json, next_action, evidence_json FROM requests \
    WHERE session_id = ?1 \
    ORDER BY (admission_offset IS NOT NULL) DESC, COALESCE(admission_offset, 0) DESC, \
        created_at_ms DESC, request_id DESC LIMIT 1";

const SELECT_ADMITTED_REQUESTS: &str = "SELECT session_id, request_id, turn_id, revision, status, \
    summary, requirements_json, next_action, evidence_json FROM requests \
    WHERE session_id = ?1 \
        AND (?2 IS NULL OR status = ?2) \
        AND (?3 IS NULL OR ( \
            (admission_offset IS NOT NULL), COALESCE(admission_offset, 0), \
            created_at_ms, request_id \
        ) < (?4, ?5, ?6, ?7)) \
    ORDER BY (admission_offset IS NOT NULL) DESC, COALESCE(admission_offset, 0) DESC, \
        created_at_ms DESC, request_id DESC LIMIT ?8";

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS requests (
    session_id TEXT NOT NULL CHECK (length(trim(session_id)) > 0),
    request_id TEXT NOT NULL CHECK (length(trim(request_id)) > 0),
    turn_id TEXT CHECK (turn_id IS NULL OR length(trim(turn_id)) > 0),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    status TEXT NOT NULL DEFAULT 'observed' CHECK (
        status IN ('observed', 'open', 'in_progress', 'waiting', 'completed', 'superseded')
    ),
    summary TEXT,
    requirements_json TEXT NOT NULL DEFAULT '[]',
    next_action TEXT,
    evidence_json TEXT NOT NULL DEFAULT '[]',
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    admission_offset INTEGER CHECK (admission_offset IS NULL OR admission_offset >= 0),
    PRIMARY KEY (session_id, request_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_requests_session_updated
    ON requests (session_id, updated_at_ms DESC, request_id DESC);
CREATE INDEX IF NOT EXISTS idx_requests_session_status_updated
    ON requests (session_id, status, updated_at_ms DESC, request_id DESC);
CREATE INDEX IF NOT EXISTS idx_requests_session_admission
    ON requests (
        session_id,
        (admission_offset IS NOT NULL) DESC,
        COALESCE(admission_offset, 0) DESC,
        created_at_ms DESC,
        request_id DESC
    );
CREATE INDEX IF NOT EXISTS idx_requests_session_status_admission
    ON requests (
        session_id,
        status,
        (admission_offset IS NOT NULL) DESC,
        COALESCE(admission_offset, 0) DESC,
        created_at_ms DESC,
        request_id DESC
    );

CREATE TABLE IF NOT EXISTS session_transcripts (
    session_id TEXT PRIMARY KEY CHECK (length(trim(session_id)) > 0),
    path TEXT NOT NULL CHECK (length(path) > 0),
    updated_at_ms INTEGER NOT NULL
) WITHOUT ROWID;
"#;

const MIGRATE_V1_TO_V2_SQL: &str = r#"
ALTER TABLE requests ADD COLUMN admission_offset INTEGER
    CHECK (admission_offset IS NULL OR admission_offset >= 0);

CREATE INDEX IF NOT EXISTS idx_requests_session_admission
    ON requests (
        session_id,
        (admission_offset IS NOT NULL) DESC,
        COALESCE(admission_offset, 0) DESC,
        created_at_ms DESC,
        request_id DESC
    );
CREATE INDEX IF NOT EXISTS idx_requests_session_status_admission
    ON requests (
        session_id,
        status,
        (admission_offset IS NOT NULL) DESC,
        COALESCE(admission_offset, 0) DESC,
        created_at_ms DESC,
        request_id DESC
    );
"#;

/// Lifecycle state for one durable request record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Observed,
    Open,
    InProgress,
    Waiting,
    Completed,
    Superseded,
}

impl Status {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Open => "open",
            Self::InProgress => "in_progress",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
            Self::Superseded => "superseded",
        }
    }

    fn from_db_str(value: &str) -> Result<Self> {
        match value {
            "observed" => Ok(Self::Observed),
            "open" => Ok(Self::Open),
            "in_progress" => Ok(Self::InProgress),
            "waiting" => Ok(Self::Waiting),
            "completed" => Ok(Self::Completed),
            "superseded" => Ok(Self::Superseded),
            _ => bail!("unknown request status in ledger database"),
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Superseded)
    }
}

/// Identity data used to create a request record. No prompt text or working
/// directory is retained by default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRequest {
    pub session_id: String,
    pub request_id: String,
    pub turn_id: Option<String>,
}

/// Durable request state returned by the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub session_id: String,
    pub request_id: String,
    pub turn_id: Option<String>,
    pub revision: u64,
    pub status: Status,
    pub summary: Option<String>,
    pub requirements: Vec<String>,
    pub next_action: Option<String>,
    pub evidence: Vec<String>,
}

/// Patch applied to a request. `None` leaves a field unchanged; an empty text
/// value clears an optional text field, and an empty vector clears a list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct RequestUpdate {
    pub expected_revision: Option<u64>,
    pub status: Option<Status>,
    pub summary: Option<String>,
    pub requirements: Option<Vec<String>>,
    pub next_action: Option<String>,
    pub evidence: Option<Vec<String>>,
}

/// SQLite-backed request ledger. The database stores request metadata and a
/// transcript path only; it does not store raw prompts or the current working
/// directory.
pub struct Ledger {
    connection: Connection,
    path: PathBuf,
}

impl Ledger {
    /// Open or create a ledger database at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        ensure_private_parent_directory(path)?;
        let created = create_private_database_file(path)?;
        if !created {
            validate_existing_database(path)?;
        }

        let mut connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .with_context(|| format!("open ledger database at {}", path.display()))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .context("set ledger busy timeout")?;

        if !created {
            validate_database_identity(&connection)?;
        }

        initialize_schema(&mut connection)?;
        configure_wal(&connection)?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .context("enable ledger foreign keys")?;
        secure_sqlite_files(path)?;

        Ok(Self {
            connection,
            path: path.to_path_buf(),
        })
    }

    /// Record a request once. Repeated calls preserve existing state. A later
    /// non-empty `turn_id` can fill a missing owner; a conflicting owner errors.
    pub fn record(&mut self, new: &NewRequest) -> Result<Request> {
        validate_new_request(new)?;

        let now = now_ms()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("begin request record transaction")?;
        transaction
            .execute(
                "INSERT INTO requests (session_id, request_id, turn_id, revision, status, \
                     summary, requirements_json, next_action, evidence_json, created_at_ms, updated_at_ms) \
                 VALUES (?1, ?2, ?3, 1, 'observed', NULL, '[]', NULL, '[]', ?4, ?4) \
                 ON CONFLICT (session_id, request_id) DO NOTHING",
                params![new.session_id, new.request_id, new.turn_id, now],
            )
            .context("insert request record")?;

        let mut request = fetch_request(&transaction, &new.session_id, &new.request_id)?
            .ok_or_else(|| anyhow!("request record was not available after insert"))?;

        if let (Some(existing), Some(incoming)) = (&request.turn_id, &new.turn_id) {
            ensure!(
                existing == incoming,
                "request is already owned by a different turn"
            );
        }

        if request.turn_id.is_none() {
            if let Some(turn_id) = new.turn_id.as_deref() {
                let next_revision = increment_revision(request.revision)?;
                let changed = transaction
                    .execute(
                        "UPDATE requests SET turn_id = ?1, revision = ?2, updated_at_ms = ?3 \
                         WHERE session_id = ?4 AND request_id = ?5 AND revision = ?6 AND turn_id IS NULL",
                        params![
                            turn_id,
                            sql_revision(next_revision)?,
                            now,
                            new.session_id,
                            new.request_id,
                            sql_revision(request.revision)?
                        ],
                    )
                    .context("set request owning turn")?;
                ensure!(changed == 1, "request changed while recording its owner");
                request.turn_id = Some(turn_id.to_owned());
                request.revision = next_revision;
            }
        }

        transaction.commit().context("commit request record")?;
        self.secure_files()?;
        Ok(request)
    }

    /// Record the original transcript byte position for an existing request.
    /// Replayed observations keep the earliest position and do not change the
    /// request revision or update timestamp.
    pub fn record_admission(&mut self, request: &NewRequest, byte_offset: u64) -> Result<()> {
        validate_new_request(request)?;
        let byte_offset = i64::try_from(byte_offset)
            .context("request admission byte offset exceeds SQLite integer range")?;

        let (existing_turn, existing_offset): (Option<String>, Option<i64>) = self
            .connection
            .query_row(
                "SELECT turn_id, admission_offset FROM requests \
                 WHERE session_id = ?1 AND request_id = ?2",
                params![request.session_id, request.request_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .context("read existing request admission")?;
        ensure!(
            request.turn_id.is_none() || request.turn_id == existing_turn,
            "request has not been recorded with the requested owning turn"
        );
        if existing_offset.is_some_and(|existing_offset| existing_offset <= byte_offset) {
            return Ok(());
        }

        let changed = self
            .connection
            .execute(
                "UPDATE requests SET admission_offset = ?1 \
                 WHERE session_id = ?2 AND request_id = ?3 \
                 AND (admission_offset IS NULL OR admission_offset > ?1)",
                params![byte_offset, request.session_id, request.request_id],
            )
            .context("update request admission offset")?;
        if changed > 0 {
            self.secure_files()?;
        }
        Ok(())
    }

    /// Fetch one request by its owning session and request identifiers.
    pub fn get(&self, session_id: &str, request_id: &str) -> Result<Option<Request>> {
        validate_identifier("session_id", session_id)?;
        validate_identifier("request_id", request_id)?;
        fetch_request(&self.connection, session_id, request_id)
    }

    /// Return the most recently admitted request in its original transcript
    /// order. Requests without a known byte position use creation time.
    pub fn latest_admitted(&self, session_id: &str) -> Result<Option<Request>> {
        validate_identifier("session_id", session_id)?;
        let stored = self
            .connection
            .query_row(
                SELECT_LATEST_ADMITTED,
                params![session_id],
                StoredRequest::from_row,
            )
            .optional()
            .context("query latest admitted request")?;
        stored.map(StoredRequest::into_request).transpose()
    }

    /// List requests in reverse transcript admission order, capped at 500
    /// rows. `before` is the request_id of the last row from the previous page.
    pub fn list_admitted(
        &self,
        session_id: &str,
        status: Option<Status>,
        limit: u32,
        before: Option<&str>,
    ) -> Result<Vec<Request>> {
        validate_identifier("session_id", session_id)?;
        let limit = i64::from(limit.min(MAX_LIST_LIMIT));
        if limit == 0 {
            return Ok(Vec::new());
        }

        let cursor = match before {
            Some(request_id) => {
                validate_identifier("before", request_id)?;
                Some(
                    self.connection
                        .query_row(
                            "SELECT (admission_offset IS NOT NULL), \
                                COALESCE(admission_offset, 0), created_at_ms, request_id \
                             FROM requests \
                             WHERE session_id = ?1 AND request_id = ?2",
                            params![session_id, request_id],
                            |row| {
                                Ok(AdmissionCursor {
                                    has_offset: row.get(0)?,
                                    offset: row.get(1)?,
                                    created_at_ms: row.get(2)?,
                                    request_id: row.get(3)?,
                                })
                            },
                        )
                        .optional()
                        .context("read request admission cursor")?
                        .ok_or_else(|| {
                            anyhow!("pagination cursor request was not found in this session")
                        })?,
                )
            }
            None => None,
        };
        let cursor_values = cursor.unwrap_or(AdmissionCursor {
            has_offset: 0,
            offset: 0,
            created_at_ms: 0,
            request_id: String::new(),
        });

        let mut statement = self
            .connection
            .prepare(SELECT_ADMITTED_REQUESTS)
            .context("prepare admitted request list")?;
        let rows = statement
            .query_map(
                params![
                    session_id,
                    status.map(Status::as_db_str),
                    before,
                    cursor_values.has_offset,
                    cursor_values.offset,
                    cursor_values.created_at_ms,
                    cursor_values.request_id,
                    limit
                ],
                StoredRequest::from_row,
            )
            .context("query admitted request list")?;
        let mut requests = Vec::new();
        for row in rows {
            requests.push(row.context("read admitted request row")?.into_request()?);
        }
        Ok(requests)
    }

    /// Apply a transactional update. If `expected_revision` is supplied, a
    /// stale update fails unless the requested patch has already been applied,
    /// in which case the retry returns the current record unchanged.
    pub fn update(
        &mut self,
        session_id: &str,
        request_id: &str,
        change: &RequestUpdate,
    ) -> Result<Request> {
        validate_identifier("session_id", session_id)?;
        validate_identifier("request_id", request_id)?;

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("begin request update transaction")?;
        let current = fetch_request(&transaction, session_id, request_id)?
            .ok_or_else(|| anyhow!("request record not found"))?;

        if let Some(expected_revision) = change.expected_revision {
            if expected_revision != current.revision {
                if patch_has_fields(change) && patch_matches(&current, change) {
                    transaction
                        .commit()
                        .context("finish idempotent request update retry")?;
                    return Ok(current);
                }
                bail!(
                    "stale request revision: expected {expected_revision}, current {}",
                    current.revision
                );
            }
        }

        let mut updated = current.clone();
        if let Some(status) = change.status {
            updated.status = status;
        }
        if let Some(summary) = change.summary.as_deref() {
            updated.summary = normalize_optional_text(summary);
        }
        if let Some(requirements) = change.requirements.as_ref() {
            updated.requirements.clone_from(requirements);
        }
        if let Some(next_action) = change.next_action.as_deref() {
            updated.next_action = normalize_optional_text(next_action);
        }
        if let Some(evidence) = change.evidence.as_ref() {
            updated.evidence.clone_from(evidence);
        }

        ensure!(
            !current.status.is_terminal() || updated.status == current.status,
            "terminal request status cannot be changed"
        );
        validate_terminal_state(&updated)?;

        if updated == current {
            transaction
                .commit()
                .context("finish unchanged request update")?;
            return Ok(current);
        }

        updated.revision = increment_revision(current.revision)?;
        let now = now_ms()?;
        let requirements_json =
            serde_json::to_string(&updated.requirements).context("encode request requirements")?;
        let evidence_json =
            serde_json::to_string(&updated.evidence).context("encode request evidence")?;
        let changed = transaction
            .execute(
                "UPDATE requests SET revision = ?1, status = ?2, summary = ?3, \
                     requirements_json = ?4, next_action = ?5, evidence_json = ?6, updated_at_ms = ?7 \
                 WHERE session_id = ?8 AND request_id = ?9 AND revision = ?10",
                params![
                    sql_revision(updated.revision)?,
                    updated.status.as_db_str(),
                    updated.summary,
                    requirements_json,
                    updated.next_action,
                    evidence_json,
                    now,
                    session_id,
                    request_id,
                    sql_revision(current.revision)?
                ],
            )
            .context("update request record")?;
        ensure!(
            changed == 1,
            "request changed during revision compare-and-swap"
        );

        transaction.commit().context("commit request update")?;
        self.secure_files()?;
        Ok(updated)
    }

    /// Store or replace the transcript file path for a session. Transcript
    /// contents are not copied into the database.
    pub fn save_transcript(&self, session_id: &str, path: &Path) -> Result<()> {
        validate_identifier("session_id", session_id)?;
        let path = path
            .to_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("transcript path must be non-empty UTF-8"))?;
        self.connection
            .execute(
                "INSERT INTO session_transcripts (session_id, path, updated_at_ms) \
                 VALUES (?1, ?2, ?3) \
                 ON CONFLICT (session_id) DO UPDATE SET path = excluded.path, updated_at_ms = excluded.updated_at_ms",
                params![session_id, path, now_ms()?],
            )
            .context("save transcript path")?;
        self.secure_files()
    }

    /// Return the saved transcript path for a session, if one exists.
    pub fn transcript(&self, session_id: &str) -> Result<Option<PathBuf>> {
        validate_identifier("session_id", session_id)?;
        let path: Option<String> = self
            .connection
            .query_row(
                "SELECT path FROM session_transcripts WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()
            .context("read transcript path")?;
        Ok(path.map(PathBuf::from))
    }

    fn secure_files(&self) -> Result<()> {
        secure_sqlite_files(&self.path)
    }
}

struct StoredRequest {
    session_id: String,
    request_id: String,
    turn_id: Option<String>,
    revision: i64,
    status: String,
    summary: Option<String>,
    requirements_json: String,
    next_action: Option<String>,
    evidence_json: String,
}

struct AdmissionCursor {
    has_offset: i64,
    offset: i64,
    created_at_ms: i64,
    request_id: String,
}

impl StoredRequest {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            session_id: row.get(0)?,
            request_id: row.get(1)?,
            turn_id: row.get(2)?,
            revision: row.get(3)?,
            status: row.get(4)?,
            summary: row.get(5)?,
            requirements_json: row.get(6)?,
            next_action: row.get(7)?,
            evidence_json: row.get(8)?,
        })
    }

    fn into_request(self) -> Result<Request> {
        Ok(Request {
            session_id: self.session_id,
            request_id: self.request_id,
            turn_id: self.turn_id,
            revision: u64::try_from(self.revision)
                .context("invalid request revision in database")?,
            status: Status::from_db_str(&self.status)?,
            summary: self.summary,
            requirements: serde_json::from_str(&self.requirements_json)
                .context("decode request requirements from database")?,
            next_action: self.next_action,
            evidence: serde_json::from_str(&self.evidence_json)
                .context("decode request evidence from database")?,
        })
    }
}

fn fetch_request(
    connection: &Connection,
    session_id: &str,
    request_id: &str,
) -> Result<Option<Request>> {
    let stored = connection
        .query_row(
            SELECT_REQUEST,
            params![session_id, request_id],
            StoredRequest::from_row,
        )
        .optional()
        .context("fetch request record")?;
    stored.map(StoredRequest::into_request).transpose()
}

fn validate_new_request(new: &NewRequest) -> Result<()> {
    validate_identifier("session_id", &new.session_id)?;
    validate_identifier("request_id", &new.request_id)?;
    if let Some(turn_id) = new.turn_id.as_deref() {
        validate_identifier("turn_id", turn_id)?;
    }
    Ok(())
}

fn validate_existing_database(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect existing ledger path at {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "existing ledger path is not a regular file"
    );

    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
    )
    .with_context(|| format!("validate existing database read-only at {}", path.display()))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .context("set validation read timeout")?;
    validate_database_identity(&connection)
}

fn validate_database_identity(connection: &Connection) -> Result<()> {
    let application_id: i64 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .context("read ledger application id")?;
    let schema_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .context("read ledger schema version")?;
    ensure!(
        application_id == APPLICATION_ID && (1..=SCHEMA_VERSION).contains(&schema_version),
        "existing SQLite database is not a supported goal-ledger database"
    );
    let request_columns = if schema_version == 1 {
        REQUEST_COLUMNS_V1
    } else {
        REQUEST_COLUMNS_V2
    };
    validate_table_columns(connection, "requests", request_columns)?;
    validate_table_columns(connection, "session_transcripts", TRANSCRIPT_COLUMNS)?;
    Ok(())
}

fn validate_table_columns(connection: &Connection, table: &str, expected: &[&str]) -> Result<()> {
    let pragma = match table {
        "requests" => "PRAGMA table_info(requests)",
        "session_transcripts" => "PRAGMA table_info(session_transcripts)",
        _ => bail!("unsupported ledger table name"),
    };
    let mut statement = connection
        .prepare(pragma)
        .with_context(|| format!("inspect {table} table columns"))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .with_context(|| format!("read {table} table columns"))?;
    let mut actual = Vec::new();
    for row in rows {
        actual.push(row.with_context(|| format!("read {table} column name"))?);
    }
    ensure!(
        actual
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied()),
        "existing SQLite database is not a supported goal-ledger database"
    );
    Ok(())
}

fn configure_wal(connection: &Connection) -> Result<()> {
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .context("enable ledger WAL mode")?;
    ensure!(
        journal_mode.eq_ignore_ascii_case("wal"),
        "SQLite refused WAL mode for the ledger"
    );
    connection
        .pragma_update(None, "synchronous", 2_i64)
        .context("set full ledger durability")?;
    Ok(())
}

fn initialize_schema(connection: &mut Connection) -> Result<()> {
    let application_id: i64 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .context("read ledger application id")?;
    let schema_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .context("read ledger schema version")?;

    ensure!(
        application_id == 0 || application_id == APPLICATION_ID,
        "database belongs to a different application"
    );
    ensure!(
        schema_version <= SCHEMA_VERSION,
        "ledger database schema is newer than this binary"
    );
    ensure!(
        schema_version >= 0,
        "invalid ledger database schema version"
    );

    if schema_version == SCHEMA_VERSION {
        ensure!(
            application_id == APPLICATION_ID,
            "ledger database application id is missing"
        );
        validate_database_identity(connection)?;
        return Ok(());
    }

    if schema_version == 1 {
        ensure!(
            application_id == APPLICATION_ID,
            "ledger database application id is missing"
        );
        validate_database_identity(connection)?;
        return migrate_v1_to_v2(connection);
    }

    if application_id == 0 {
        let existing_tables: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )
            .context("check existing database tables")?;
        ensure!(
            existing_tables == 0,
            "refusing to initialize over an unrelated SQLite database"
        );
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("begin ledger schema transaction")?;
    transaction
        .execute_batch(SCHEMA_SQL)
        .context("create ledger schema")?;
    transaction
        .pragma_update(None, "application_id", APPLICATION_ID)
        .context("write ledger application id")?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .context("write ledger schema version")?;
    transaction.commit().context("commit ledger schema")?;
    Ok(())
}

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("begin ledger schema migration")?;
    let application_id: i64 = transaction
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .context("read ledger application id during migration")?;
    let schema_version: i64 = transaction
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .context("read ledger schema version during migration")?;
    ensure!(
        application_id == APPLICATION_ID && schema_version == 1,
        "ledger database ownership changed before migration"
    );
    validate_table_columns(&transaction, "requests", REQUEST_COLUMNS_V1)?;
    validate_table_columns(&transaction, "session_transcripts", TRANSCRIPT_COLUMNS)?;

    transaction
        .execute_batch(MIGRATE_V1_TO_V2_SQL)
        .context("add request admission offset")?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .context("write migrated ledger schema version")?;
    transaction
        .commit()
        .context("commit ledger schema migration")?;
    Ok(())
}

fn validate_identifier(name: &str, value: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{name} must be non-empty");
    Ok(())
}

fn normalize_optional_text(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn patch_has_fields(change: &RequestUpdate) -> bool {
    change.status.is_some()
        || change.summary.is_some()
        || change.requirements.is_some()
        || change.next_action.is_some()
        || change.evidence.is_some()
}

fn patch_matches(request: &Request, change: &RequestUpdate) -> bool {
    change.status.is_none_or(|status| request.status == status)
        && change
            .summary
            .as_deref()
            .is_none_or(|summary| request.summary == normalize_optional_text(summary))
        && change
            .requirements
            .as_ref()
            .is_none_or(|requirements| request.requirements == *requirements)
        && change
            .next_action
            .as_deref()
            .is_none_or(|next_action| request.next_action == normalize_optional_text(next_action))
        && change
            .evidence
            .as_ref()
            .is_none_or(|evidence| request.evidence == *evidence)
}

fn validate_terminal_state(request: &Request) -> Result<()> {
    match request.status {
        Status::Completed => {
            ensure!(
                request.evidence.iter().any(|item| !item.trim().is_empty()),
                "completed request requires non-empty evidence"
            );
            Ok(())
        }
        Status::Superseded => {
            ensure!(
                request
                    .summary
                    .as_deref()
                    .is_some_and(|summary| !summary.trim().is_empty())
                    || request.evidence.iter().any(|item| !item.trim().is_empty()),
                "superseded request requires a summary or evidence reason"
            );
            Ok(())
        }
        _ => Ok(()),
    }
}

fn increment_revision(revision: u64) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("request revision overflow"))
}

fn sql_revision(revision: u64) -> Result<i64> {
    i64::try_from(revision).context("request revision exceeds SQLite integer range")
}

fn now_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_millis()).context("system timestamp exceeds SQLite integer range")
}

fn ensure_private_parent_directory(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    create_private_directory_tree(parent)
}

fn create_private_directory_tree(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let is_directory = if metadata.file_type().is_symlink() {
                fs::metadata(path)
                    .map(|target| target.is_dir())
                    .unwrap_or(false)
            } else {
                metadata.is_dir()
            };
            ensure!(is_directory, "ledger parent path is not a directory");
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect ledger parent directory at {}", path.display()));
        }
    }

    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_private_directory_tree(parent)?;
    }

    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => set_private_directory_permissions(path),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::metadata(path)
                .with_context(|| format!("inspect ledger parent at {}", path.display()))?;
            ensure!(metadata.is_dir(), "ledger parent path is not a directory");
            Ok(())
        }
        Err(error) => {
            Err(error).with_context(|| format!("create ledger parent at {}", path.display()))
        }
    }
}

fn create_private_database_file(path: &Path) -> Result<bool> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(file) => {
            set_private_handle_permissions(&file)?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("create ledger file at {}", path.display()))
        }
    }
}

fn secure_sqlite_files(path: &Path) -> Result<()> {
    set_private_permissions(path)?;
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        set_private_permissions(Path::new(&sidecar))?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("restrict ledger directory at {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_handle_permissions(file: &fs::File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(fs::Permissions::from_mode(0o600))
        .context("restrict newly created ledger file")
}

#[cfg(not(unix))]
fn set_private_handle_permissions(_file: &fs::File) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    match fs::set_permissions(path, fs::Permissions::from_mode(0o600)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("restrict permissions on {}", path.display()))
        }
    }
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        directory: PathBuf,
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let unique = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "goal-ledger-test-{}-{unique}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock is after Unix epoch")
                    .as_nanos()
            ));
            fs::create_dir(&directory).expect("create temporary test directory");
            let path = directory.join("ledger.sqlite");
            Self { directory, path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    fn new_request(turn_id: Option<&str>) -> NewRequest {
        NewRequest {
            session_id: "session-a".to_owned(),
            request_id: "request-a".to_owned(),
            turn_id: turn_id.map(str::to_owned),
        }
    }

    fn named_request(request_id: &str, turn_id: &str) -> NewRequest {
        NewRequest {
            session_id: "session-a".to_owned(),
            request_id: request_id.to_owned(),
            turn_id: Some(turn_id.to_owned()),
        }
    }

    #[test]
    fn record_retries_preserve_state_and_reject_owner_conflicts() {
        let db = TempDb::new();
        let mut ledger = Ledger::open(&db.path).unwrap();
        let initial = ledger.record(&new_request(None)).unwrap();
        assert_eq!(initial.revision, 1);
        assert_eq!(initial.status, Status::Observed);

        let owned = ledger.record(&new_request(Some("turn-a"))).unwrap();
        assert_eq!(owned.turn_id.as_deref(), Some("turn-a"));
        assert_eq!(owned.revision, 2);
        let preserved = ledger
            .update(
                "session-a",
                "request-a",
                &RequestUpdate {
                    expected_revision: Some(owned.revision),
                    status: Some(Status::Open),
                    summary: Some("keep this update".to_owned()),
                    ..RequestUpdate::default()
                },
            )
            .unwrap();
        let retry = ledger.record(&new_request(Some("turn-a"))).unwrap();
        assert_eq!(retry, preserved);

        assert!(ledger.record(&new_request(Some("turn-b"))).is_err());
        assert_eq!(
            ledger.get("session-a", "request-a").unwrap(),
            Some(preserved)
        );
    }

    #[test]
    fn update_uses_revision_cas_and_accepts_an_applied_retry() {
        let db = TempDb::new();
        let mut ledger = Ledger::open(&db.path).unwrap();
        ledger.record(&new_request(Some("turn-a"))).unwrap();
        let patch = RequestUpdate {
            expected_revision: Some(1),
            status: Some(Status::InProgress),
            summary: Some("Inspect the current request".to_owned()),
            ..RequestUpdate::default()
        };

        let updated = ledger.update("session-a", "request-a", &patch).unwrap();
        assert_eq!(updated.revision, 2);
        assert_eq!(
            ledger.update("session-a", "request-a", &patch).unwrap(),
            updated
        );

        let stale = RequestUpdate {
            expected_revision: Some(1),
            next_action: Some("different action".to_owned()),
            ..RequestUpdate::default()
        };
        assert!(ledger.update("session-a", "request-a", &stale).is_err());
        assert_eq!(ledger.get("session-a", "request-a").unwrap(), Some(updated));
    }

    #[test]
    fn terminal_states_require_reason_and_cannot_reopen() {
        let db = TempDb::new();
        let mut ledger = Ledger::open(&db.path).unwrap();
        ledger.record(&new_request(Some("turn-a"))).unwrap();

        let no_evidence = RequestUpdate {
            expected_revision: Some(1),
            status: Some(Status::Completed),
            ..RequestUpdate::default()
        };
        assert!(
            ledger
                .update("session-a", "request-a", &no_evidence)
                .is_err()
        );
        assert_eq!(
            ledger
                .get("session-a", "request-a")
                .unwrap()
                .unwrap()
                .status,
            Status::Observed
        );

        let completed = ledger
            .update(
                "session-a",
                "request-a",
                &RequestUpdate {
                    expected_revision: Some(1),
                    status: Some(Status::Completed),
                    evidence: Some(vec!["receipt: check-1".to_owned()]),
                    ..RequestUpdate::default()
                },
            )
            .unwrap();
        assert_eq!(completed.status, Status::Completed);

        let reopen = RequestUpdate {
            expected_revision: Some(completed.revision),
            status: Some(Status::InProgress),
            ..RequestUpdate::default()
        };
        assert!(ledger.update("session-a", "request-a", &reopen).is_err());
        assert_eq!(
            ledger.get("session-a", "request-a").unwrap(),
            Some(completed)
        );
    }

    #[test]
    fn superseded_requires_a_reason_and_lists_are_filtered_and_bounded() {
        let db = TempDb::new();
        let mut ledger = Ledger::open(&db.path).unwrap();
        ledger.record(&new_request(Some("turn-a"))).unwrap();
        assert!(
            ledger
                .update(
                    "session-a",
                    "request-a",
                    &RequestUpdate {
                        status: Some(Status::Superseded),
                        ..RequestUpdate::default()
                    }
                )
                .is_err()
        );
        ledger
            .update(
                "session-a",
                "request-a",
                &RequestUpdate {
                    summary: Some("Replaced by request-b".to_owned()),
                    status: Some(Status::Superseded),
                    ..RequestUpdate::default()
                },
            )
            .unwrap();

        assert_eq!(
            ledger
                .list_admitted("session-a", Some(Status::Superseded), 10, None)
                .unwrap()
                .len(),
            1
        );
        assert!(
            ledger
                .list_admitted("session-a", Some(Status::Open), 10, None)
                .unwrap()
                .is_empty()
        );
        assert!(
            ledger
                .list_admitted("session-a", None, 0, None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn admission_order_uses_transcript_offsets_and_keyset_pages() {
        let db = TempDb::new();
        let mut ledger = Ledger::open(&db.path).unwrap();
        let earlier = named_request("earlier", "turn-earlier");
        let middle = named_request("middle", "turn-middle");
        let latest = named_request("latest", "turn-latest");
        ledger.record(&earlier).unwrap();
        ledger.record(&middle).unwrap();
        ledger.record(&latest).unwrap();
        ledger.record_admission(&earlier, 10).unwrap();
        ledger.record_admission(&middle, 20).unwrap();
        ledger.record_admission(&latest, 30).unwrap();

        let revision_before_replay = ledger
            .get("session-a", "earlier")
            .unwrap()
            .unwrap()
            .revision;
        ledger.record_admission(&earlier, 10).unwrap();
        ledger.record_admission(&earlier, 15).unwrap();
        let offset_after_replay: i64 = ledger
            .connection
            .query_row(
                "SELECT admission_offset FROM requests WHERE session_id = ?1 AND request_id = ?2",
                params!["session-a", "earlier"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(offset_after_replay, 10);
        assert_eq!(
            ledger
                .get("session-a", "earlier")
                .unwrap()
                .unwrap()
                .revision,
            revision_before_replay
        );

        ledger
            .update(
                "session-a",
                "earlier",
                &RequestUpdate {
                    status: Some(Status::Open),
                    summary: Some("Updated after the later admission".to_owned()),
                    ..RequestUpdate::default()
                },
            )
            .unwrap();

        let first_page = ledger.list_admitted("session-a", None, 2, None).unwrap();
        assert_eq!(
            first_page
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            ["latest", "middle"]
        );
        let second_page = ledger
            .list_admitted("session-a", None, 2, Some("middle"))
            .unwrap();
        assert_eq!(
            second_page
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            ["earlier"]
        );
        assert_eq!(
            ledger
                .latest_admitted("session-a")
                .unwrap()
                .unwrap()
                .request_id,
            "latest"
        );
        assert_eq!(
            ledger
                .list_admitted("session-a", Some(Status::Open), 10, None)
                .unwrap()
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            ["earlier"]
        );
        assert!(
            ledger
                .list_admitted("session-a", None, 2, Some("missing"))
                .is_err()
        );
    }

    #[test]
    fn v1_migration_preserves_request_state_and_transcript() {
        let db = TempDb::new();
        {
            let connection = Connection::open(&db.path).unwrap();
            connection
                .execute_batch("PRAGMA journal_mode = DELETE;")
                .unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE requests (
                        session_id TEXT NOT NULL CHECK (length(trim(session_id)) > 0),
                        request_id TEXT NOT NULL CHECK (length(trim(request_id)) > 0),
                        turn_id TEXT CHECK (turn_id IS NULL OR length(trim(turn_id)) > 0),
                        revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
                        status TEXT NOT NULL DEFAULT 'observed' CHECK (
                            status IN ('observed', 'open', 'in_progress', 'waiting', 'completed', 'superseded')
                        ),
                        summary TEXT,
                        requirements_json TEXT NOT NULL DEFAULT '[]',
                        next_action TEXT,
                        evidence_json TEXT NOT NULL DEFAULT '[]',
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        PRIMARY KEY (session_id, request_id)
                    ) WITHOUT ROWID;
                    CREATE TABLE session_transcripts (
                        session_id TEXT PRIMARY KEY CHECK (length(trim(session_id)) > 0),
                        path TEXT NOT NULL CHECK (length(path) > 0),
                        updated_at_ms INTEGER NOT NULL
                    ) WITHOUT ROWID;
                    INSERT INTO requests VALUES (
                        'session-old', 'request-old', 'turn-old', 7, 'completed',
                        'Preserve this completed request', '[\"requirement\"]', NULL,
                        '[\"receipt: old\"]', 1000, 2000
                    );
                    INSERT INTO session_transcripts VALUES ('session-old', '/tmp/old.jsonl', 3000);",
                )
                .unwrap();
            connection
                .pragma_update(None, "application_id", APPLICATION_ID)
                .unwrap();
            connection
                .pragma_update(None, "user_version", 1_i64)
                .unwrap();
        }

        let ledger = Ledger::open(&db.path).unwrap();
        let preserved = ledger.get("session-old", "request-old").unwrap().unwrap();
        assert_eq!(preserved.revision, 7);
        assert_eq!(preserved.status, Status::Completed);
        assert_eq!(
            preserved.summary.as_deref(),
            Some("Preserve this completed request")
        );
        assert_eq!(preserved.requirements, ["requirement"]);
        assert_eq!(preserved.evidence, ["receipt: old"]);
        assert_eq!(
            ledger.transcript("session-old").unwrap(),
            Some(PathBuf::from("/tmp/old.jsonl"))
        );

        let schema_version: i64 = ledger
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        let migrated_offset: Option<i64> = ledger
            .connection
            .query_row(
                "SELECT admission_offset FROM requests \
                 WHERE session_id = 'session-old' AND request_id = 'request-old'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(schema_version, SCHEMA_VERSION);
        assert_eq!(migrated_offset, None);
    }

    #[test]
    fn malformed_v1_schema_is_rejected_without_mutation() {
        let db = TempDb::new();
        {
            let connection = Connection::open(&db.path).unwrap();
            connection
                .execute_batch("PRAGMA journal_mode = DELETE;")
                .unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE requests (session_id TEXT NOT NULL, request_id TEXT NOT NULL);
                    CREATE TABLE session_transcripts (
                        session_id TEXT PRIMARY KEY, path TEXT NOT NULL, updated_at_ms INTEGER NOT NULL
                    );",
                )
                .unwrap();
            connection
                .pragma_update(None, "application_id", APPLICATION_ID)
                .unwrap();
            connection
                .pragma_update(None, "user_version", 1_i64)
                .unwrap();
        }

        let before = fs::read(&db.path).unwrap();
        assert!(Ledger::open(&db.path).is_err());
        assert_eq!(fs::read(&db.path).unwrap(), before);
    }

    #[test]
    fn open_creates_missing_parent_directories_privately() {
        let db = TempDb::new();
        let path = db
            .directory
            .join("private-state")
            .join("ledger")
            .join("ledger.sqlite");
        #[cfg(unix)]
        let existing_parent_mode = unix_mode(&db.directory);

        let ledger = Ledger::open(&path).unwrap();
        drop(ledger);

        assert!(path.is_file());
        #[cfg(unix)]
        {
            assert_eq!(unix_mode(&db.directory), existing_parent_mode);
            assert_eq!(unix_mode(&db.directory.join("private-state")) & 0o077, 0);
            assert_eq!(
                unix_mode(&db.directory.join("private-state/ledger")) & 0o077,
                0
            );
            assert_eq!(unix_mode(&path) & 0o077, 0);
        }
    }

    #[test]
    fn unrelated_database_is_rejected_without_mutation() {
        let db = TempDb::new();
        {
            let connection = Connection::open(&db.path).unwrap();
            connection
                .execute_batch(
                    "PRAGMA journal_mode = DELETE; \
                     CREATE TABLE unrelated (value TEXT NOT NULL); \
                     INSERT INTO unrelated VALUES ('preserve me');",
                )
                .unwrap();
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&db.path, fs::Permissions::from_mode(0o640)).unwrap();
        }

        let before_pages = fs::read(&db.path).unwrap();
        #[cfg(unix)]
        let before_mode = unix_mode(&db.path);

        assert!(Ledger::open(&db.path).is_err());

        assert_eq!(fs::read(&db.path).unwrap(), before_pages);
        #[cfg(unix)]
        assert_eq!(unix_mode(&db.path), before_mode);

        let read_only = Connection::open_with_flags(
            &db.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .unwrap();
        let journal_mode: String = read_only
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode.to_ascii_lowercase(), "delete");
    }

    #[cfg(unix)]
    fn unix_mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }
}
