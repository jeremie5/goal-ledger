mod codex;
mod ledger;
mod mcp;
mod rollout;

use anyhow::{Result, bail};
use std::path::PathBuf;

fn db_path() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("GOAL_LEDGER_DB").filter(|path| !path.is_empty()) {
        return Ok(path.into());
    }
    let state_home = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| std::env::home_dir().map(|home| home.join(".local").join("state")))
        .ok_or_else(|| anyhow::anyhow!("set GOAL_LEDGER_DB to a local SQLite path"))?;
    Ok(state_home.join("goal-ledger").join("tasks.sqlite3"))
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let command = args.next();
    if args.next().is_some() {
        bail!("usage: goal-ledger <hook|mcp>");
    }
    match command.as_deref() {
        Some("hook") => {
            // A bookkeeping failure must never interrupt a Codex turn.
            if let Err(err) = db_path().and_then(|path| codex::run_hook(&path)) {
                eprintln!("goal-ledger hook skipped: {err:#}");
            }
            Ok(())
        }
        Some("mcp") => mcp::run(&db_path()?),
        Some("--version" | "-V") => {
            println!("goal-ledger {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("--help" | "-h") | None => {
            println!(
                "goal-ledger <hook|mcp>\n\n  hook  Handle a Codex lifecycle event on stdin\n  mcp   Serve task ledger tools over MCP stdio"
            );
            Ok(())
        }
        Some(_) => bail!("usage: goal-ledger <hook|mcp>"),
    }
}
