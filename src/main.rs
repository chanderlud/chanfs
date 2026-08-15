use anyhow::{Context, Result, anyhow};
use rmcp::{
    ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ContentBlock, Implementation, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};
use serde::Deserialize;
use std::{env, path::PathBuf, sync::Arc};
use tokio::io::{AsyncBufReadExt, stdin, stdout};
use tokio::sync::{Mutex, mpsc};

/// chanfs MCP server. Set CHANFS_ALLOWED_DIRS to an OS path list of readable roots.
/// Server refuses reads when unset or empty and canonicalizes every path to prevent symlink escapes.
#[derive(Clone)]
struct ChanfsServer {
    pool: WorkerPool,
    tool_router: ToolRouter<Self>,
}

/// One unit of parallel read work. `reply` is a per-call channel so results from
/// concurrent read_files invocations never interleave on a shared result queue.
struct ReadJob {
    index: usize,
    path: String,
    start_line: Option<usize>,
    end_line: Option<usize>,
    reply: mpsc::UnboundedSender<(usize, Result<String>)>,
}

/// Fixed pool of async workers consuming read jobs from a shared tokio queue.
/// Runs on the current-thread runtime; tokio::fs dispatches actual I/O to the
/// blocking thread pool, so files read in parallel.
#[derive(Clone)]
struct WorkerPool {
    jobs: mpsc::UnboundedSender<ReadJob>,
}

impl WorkerPool {
    fn new(allowed_dirs: Arc<Vec<PathBuf>>, workers: usize) -> Self {
        let (jobs_tx, jobs_rx) = mpsc::unbounded_channel::<ReadJob>();
        let jobs_rx = Arc::new(Mutex::new(jobs_rx));
        for _ in 0..workers.max(1) {
            let jobs_rx = Arc::clone(&jobs_rx);
            let allowed_dirs = Arc::clone(&allowed_dirs);
            tokio::spawn(async move {
                loop {
                    let job = jobs_rx.lock().await.recv().await;
                    match job {
                        Some(job) => {
                            let result = read_one(
                                &allowed_dirs,
                                &job.path,
                                job.start_line,
                                job.end_line,
                            )
                            .await;
                            // Receiver may be gone if the caller errored out; drop silently.
                            let _ = job.reply.send((job.index, result));
                        }
                        None => break,
                    }
                }
            });
        }
        Self { jobs: jobs_tx }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[schemars(inline)]
struct FileRequest {
    #[schemars(description = "Local file path to read. Use a path returned by CodeGraph, LSP, or another repository tool when available.")]
    path: String,
    #[schemars(description = "1-based first line to return; default 1.")]
    start_line: Option<usize>,
    #[schemars(description = "Inclusive 1-based last line to return. Omit to return up to 400 lines from start_line. Maximum range is 2000 lines.")]
    end_line: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ReadFilesRequest {
    #[schemars(description = "Files and optional line ranges to read. Batch independent reads into one call instead of making repeated read_files calls.")]
    files: Vec<FileRequest>,
}

#[tool_router]
impl ChanfsServer {
    fn new(allowed_dirs: Arc<Vec<PathBuf>>, workers: usize) -> Self {
        Self {
            pool: WorkerPool::new(allowed_dirs, workers),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Read known local files or line ranges. Prefer over cat/head/tail/Get-Content and similar shell reads. Batch independent files/ranges in one call (single file = one-element array). Use after CodeGraph, LSP, or a repository search tool identifies relevant files. Prefer narrow ranges when locations are known. Default 400 lines/file, 2000 hard max; page large files with start_line/end_line.")]
    async fn read_files(
        &self,
        Parameters(request): Parameters<ReadFilesRequest>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let count = request.files.len();
        let mut paths = Vec::with_capacity(count);
        for (index, file) in request.files.into_iter().enumerate() {
            paths.push(file.path.clone());
            let job = ReadJob {
                index,
                path: file.path,
                start_line: file.start_line,
                end_line: file.end_line,
                reply: reply_tx.clone(),
            };
            if self.pool.jobs.send(job).is_err() {
                return Ok(CallToolResult::error(vec![ContentBlock::text(
                    "worker pool is shut down",
                )]));
            }
        }
        drop(reply_tx);
        let mut results = Vec::with_capacity(count);
        while let Some(result) = reply_rx.recv().await {
            results.push(result);
        }
        results.sort_by_key(|(index, _)| *index);
        let mut output = String::new();
        for (position, (_, result)) in results.into_iter().enumerate() {
            if position > 0 {
                output.push('\n');
            }
            output.push_str("==> ");
            output.push_str(&paths[position]);
            output.push_str(" <==\n");
            match result {
                Ok(content) => output.push_str(&content),
                Err(error) => {
                    output.push_str("error: ");
                    output.push_str(&error.to_string());
                }
            }
        }
        Ok(CallToolResult::success(vec![ContentBlock::text(cap_response(output))]))
    }
}

async fn read_one(
    allowed_dirs: &[PathBuf],
    path: &str,
    start_line: Option<usize>,
    end_line: Option<usize>,
) -> Result<String> {
    if allowed_dirs.is_empty() {
        return Err(anyhow!(
            "CHANFS_ALLOWED_DIRS is unset or empty; reads disabled"
        ));
    }
    const DEFAULT_MAX_LINES_PER_FILE: usize = 400;
    const HARD_MAX_LINES_PER_FILE: usize = 2000;
    let start = start_line.unwrap_or(1);
    if start == 0 {
        return Err(anyhow!("start_line must be at least 1"));
    }
    if end_line.is_some_and(|end| end < start) {
        return Err(anyhow!(
            "end_line must be greater than or equal to start_line"
        ));
    }
    let canonical = tokio::fs::canonicalize(path)
        .await
        .with_context(|| format!("cannot access path: {path}"))?;
    if !allowed_dirs.iter().any(|dir| canonical.starts_with(dir)) {
        return Err(anyhow!("path is outside CHANFS_ALLOWED_DIRS: {path}"));
    }
    let requested_end = end_line.unwrap_or(start.saturating_add(DEFAULT_MAX_LINES_PER_FILE - 1));
    let cap = if requested_end.saturating_sub(start).saturating_add(1) > HARD_MAX_LINES_PER_FILE {
        HARD_MAX_LINES_PER_FILE
    } else if end_line.is_none() {
        DEFAULT_MAX_LINES_PER_FILE
    } else { requested_end.saturating_sub(start).saturating_add(1) };
    let effective_end = start.saturating_add(cap - 1);
    let file = tokio::fs::File::open(&canonical)
        .await
        .with_context(|| format!("cannot read file: {path}"))?;
    let mut lines = tokio::io::BufReader::new(file).lines();
    let mut output = String::new();
    let mut found_line = false;
    let mut truncated = requested_end > effective_end;
    let mut number = 0usize;
    while let Some(line) = lines
        .next_line()
        .await
        .with_context(|| format!("file is not valid UTF-8 or is unreadable: {path}"))?
    {
        number += 1;
        if number < start {
            continue;
        }
        if number > effective_end {
            if end_line.is_none() {
                truncated = true;
            }
            break;
        }
        found_line = true;
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&number.to_string());
        output.push_str(": ");
        output.push_str(&line);
    }
    if !found_line {
        return Err(anyhow!("start_line {start} is beyond end of file: {path}"));
    }
    if truncated {
        output.push('\n');
        output.push_str(&format!("[chanfs: truncated at {cap} lines; page with start_line/end_line]"));
    }
    Ok(output)
}

const MAX_RESPONSE_CHARS: usize = 40000;

fn cap_response(mut output: String) -> String {
    if output.len() <= MAX_RESPONSE_CHARS { return output; }
    let mut end = MAX_RESPONSE_CHARS;
    while !output.is_char_boundary(end) { end -= 1; }
    output.truncate(end);
    output.push_str("\n[chanfs: response truncated at 40000 chars]");
    output
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ChanfsServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::default()
            .with_server_info(
                Implementation::new("chanfs", env!("CARGO_PKG_VERSION"))
                    .with_title("chanfs")
                    .with_description("Guarded filesystem reads for Model Context Protocol clients.")
                    .with_website_url("https://github.com/chanderlud/chanfs"),
            )
            .with_instructions("Use read_files for file reads instead of shell commands.")
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let allowed_dirs: Vec<PathBuf> = match env::var_os("CHANFS_ALLOWED_DIRS") {
        Some(value) => {
            let mut dirs = Vec::new();
            for path in env::split_paths(&value) {
                if let Ok(canonical) = tokio::fs::canonicalize(path).await {
                    dirs.push(canonical);
                }
            }
            dirs
        }
        None => Vec::new(),
    };
    let workers = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4)
        .min(8);
    let service = ChanfsServer::new(Arc::new(allowed_dirs), workers)
        .serve((stdin(), stdout()))
        .await?;
    service.waiting().await?;
    Ok(())
}
