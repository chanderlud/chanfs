use anyhow::{Context, Result, anyhow};
use globset::{Glob, GlobSet, GlobSetBuilder};
use regex::RegexBuilder;
use rmcp::{
    ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ContentBlock, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};
use serde::Deserialize;
use std::{env, path::{Path, PathBuf}, sync::Arc};
use tokio::io::{AsyncBufReadExt, stdin, stdout};
use tokio::sync::{Mutex, mpsc};

/// chanfs MCP server. Set CHANFS_ALLOWED_DIRS to an OS path list of readable roots.
/// Server refuses reads when unset or empty and canonicalizes every path to prevent symlink escapes.
#[derive(Clone)]
struct ChanfsServer {
    allowed_dirs: Arc<Vec<PathBuf>>,
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
    #[schemars(description = "Local file path to read. Use a path returned by find_files, search_text, CodeGraph, or another repository tool when available.")]
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

#[derive(Debug, Default, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
#[schemars(inline)]
enum PathKind {
    #[serde(alias = "file")]
    Files,
    #[serde(alias = "directory")]
    Directories,
    #[default]
    Both,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct FindFilesRequest {
    #[schemars(description = "Directory to search within; default workspace root.")]
    path: Option<String>,
    #[schemars(description = "Glob or name pattern matched against paths under the search root, e.g. **/*.rs, src/**, or Cargo.toml. Omit to list all matching entries subject to kind/max_depth.")]
    pattern: Option<String>,
    #[schemars(description = "Return files, directories, or both; default both.")]
    #[serde(default)]
    kind: PathKind,
    #[schemars(description = "Maximum traversal depth below path. Use 1 for a direct directory listing; omit for recursive search.")]
    max_depth: Option<usize>,
    #[schemars(description = "Maximum number of returned paths; default 1000. Use a smaller limit for broad recursive searches.")]
    limit: Option<usize>,
    #[schemars(description = "Include hidden files and directories such as .github or .config; default false.")]
    include_hidden: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SearchTextRequest {
    #[schemars(description = "Text or regex to find in file contents. Interpreted literally unless regex=true.")]
    pattern: String,
    #[schemars(description = "Files or directories to search within; default workspace root. Narrow this when relevant paths are already known.")]
    paths: Option<Vec<String>>,
    #[schemars(description = "Interpret pattern as a regular expression; default false (literal text search).")]
    regex: Option<bool>,
    #[schemars(description = "Optional file glob restricting which files are searched, e.g. **/*.rs or **/Cargo.toml.")]
    glob: Option<String>,
    #[schemars(description = "Whether matching is case-sensitive; default true.")]
    case_sensitive: Option<bool>,
    #[schemars(description = "Number of surrounding lines to return before and after each match. Keep small for broad searches; use read_files for larger surrounding regions.")]
    context_lines: Option<usize>,
    #[schemars(description = "Maximum number of matches returned; default 100, hard max 1000. Narrow paths/glob/pattern rather than repeatedly requesting very large result sets.")]
    max_results: Option<usize>,
}

#[tool_router]
impl ChanfsServer {
    fn new(allowed_dirs: Arc<Vec<PathBuf>>, workers: usize) -> Self {
        Self {
            allowed_dirs: Arc::clone(&allowed_dirs),
            pool: WorkerPool::new(allowed_dirs, workers),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Read known local files or line ranges. Prefer over cat/head/tail/Get-Content and similar shell reads. Batch independent files/ranges in one call (single file = one-element array). Use after find_files, search_text, CodeGraph, or LSP identifies relevant files. Prefer narrow ranges when locations are known. Default 400 lines/file, 2000 hard max; page large files with start_line/end_line.")]
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

    #[tool(description = "Locate local files and directories by path, name, or glob. Prefer over ls/find/fd/Get-ChildItem/rg --files for filesystem discovery. Use for filenames, directory structure, extension/glob queries, and directory listings. Does not inspect file contents; use search_text for content search, CodeGraph for architecture/relationships, and LSP for symbols.")]
    async fn find_files(&self, Parameters(request): Parameters<FindFilesRequest>) -> Result<CallToolResult, rmcp::ErrorData> {
        match find_files(&self.allowed_dirs, request).await {
            Ok(output) => Ok(CallToolResult::success(vec![ContentBlock::text(output)])),
            Err(error) => Ok(CallToolResult::error(vec![ContentBlock::text(error.to_string())])),
        }
    }

    #[tool(description = "Search local file contents for literal text or regex. Prefer over grep/rg/Select-String for repository content search. Use for strings, comments, configuration values, log text, and identifiers when text matching is sufficient. Use find_files for filenames/paths, ast-grep for syntax-shaped structural queries, LSP for definitions/references, CodeGraph for architecture/flow, and grep_app for external GitHub code.")]
    async fn search_text(&self, Parameters(request): Parameters<SearchTextRequest>) -> Result<CallToolResult, rmcp::ErrorData> {
        match search_text(&self.allowed_dirs, request).await {
            Ok(output) => Ok(CallToolResult::success(vec![ContentBlock::text(output)])),
            Err(error) => Ok(CallToolResult::error(vec![ContentBlock::text(error.to_string())])),
        }
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

fn make_globset(pattern: Option<&str>) -> Result<Option<GlobSet>> {
    let Some(pattern) = pattern else { return Ok(None); };
    let mut builder = GlobSetBuilder::new();
    builder.add(Glob::new(pattern).context("invalid glob pattern")?);
    builder.add(Glob::new(&format!("**/{pattern}")).context("invalid glob pattern")?);
    Ok(Some(builder.build().context("cannot build glob set")?))
}

fn is_hidden_name(path: &Path, metadata: &std::fs::Metadata) -> bool {
    if path.file_name().is_some_and(|name| name.to_string_lossy().starts_with('.')) { return true; }
    #[cfg(windows)]
    { use std::os::windows::fs::MetadataExt; metadata.file_attributes() & 0x2 != 0 }
    #[cfg(not(windows))]
    { let _ = metadata; false }
}

fn relative_path(root: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let display = if relative.as_os_str().is_empty() {
        path.to_string_lossy().replace('\\', "/")
    } else {
        relative.to_string_lossy().replace('\\', "/")
    };
    display.strip_prefix("//?/").map(str::to_string).unwrap_or(display)
}

async fn canonical_root(allowed_dirs: &[PathBuf], path: Option<&str>) -> Result<PathBuf> {
    let value = path.map(PathBuf::from).unwrap_or(env::current_dir().context("cannot get workspace root")?);
    let canonical = tokio::fs::canonicalize(&value).await.with_context(|| format!("cannot access path: {}", value.display()))?;
    if !allowed_dirs.iter().any(|dir| canonical.starts_with(dir)) { return Err(anyhow!("path is outside CHANFS_ALLOWED_DIRS: {}", value.display())); }
    Ok(canonical)
}

async fn walk_paths(root: &Path, max_depth: Option<usize>, include_hidden: bool, glob: Option<&GlobSet>) -> Result<Vec<(PathBuf, bool)>> {
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    let mut output = Vec::new();
    while let Some((directory, depth)) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&directory).await.with_context(|| format!("cannot read directory: {}", directory.display()))?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;
            let is_dir = metadata.is_dir();
            if !include_hidden && is_hidden_name(&path, &metadata) {
                if is_dir { continue; }
                continue;
            }
            let relative = relative_path(root, &path);
            if glob.is_none_or(|set| set.is_match(&relative)) { output.push((path.clone(), is_dir)); }
            if is_dir && max_depth.is_none_or(|limit| depth + 1 < limit) { stack.push((path.clone(), depth + 1)); }
        }
    }
    Ok(output)
}

async fn find_files(allowed_dirs: &[PathBuf], request: FindFilesRequest) -> Result<String> {
    let root = canonical_root(allowed_dirs, request.path.as_deref()).await?;
    let glob = make_globset(request.pattern.as_deref())?;
    let kind = request.kind;
    let limit = request.limit.unwrap_or(1000);
    let mut entries = walk_paths(&root, request.max_depth, request.include_hidden.unwrap_or(false), glob.as_ref()).await?;
    entries.retain(|(_, is_dir)| match kind { PathKind::Files => !*is_dir, PathKind::Directories => *is_dir, PathKind::Both => true });
    let truncated = entries.len() > limit;
    entries.sort_by_key(|(a, _)| relative_path(&root, a));
    entries.truncate(limit);
    let mut output = entries.into_iter().map(|(path, is_dir)| { let mut value = relative_path(&root, &path); if is_dir { value.push('/'); } value }).collect::<Vec<_>>().join("\n");
    if truncated { if !output.is_empty() { output.push('\n'); } output.push_str(&format!("[chanfs: limit of {limit} reached]")); }
    Ok(cap_response(output))
}

async fn search_text(allowed_dirs: &[PathBuf], request: SearchTextRequest) -> Result<String> {
    let case_sensitive = request.case_sensitive.unwrap_or(true);
    let expression = if request.regex.unwrap_or(false) { request.pattern } else { regex::escape(&request.pattern) };
    let matcher = RegexBuilder::new(&expression).case_insensitive(!case_sensitive).build().context("invalid regex pattern")?;
    let glob = make_globset(request.glob.as_deref())?;
    let context = request.context_lines.unwrap_or(0);
    let max_results = request.max_results.unwrap_or(100).min(1000);
    let roots = match request.paths {
        Some(paths) => { let mut roots = Vec::new(); for path in paths { roots.push(canonical_root(allowed_dirs, Some(&path)).await?); } roots }
        None => vec![canonical_root(allowed_dirs, None).await?],
    };
    let mut output = String::new();
    let mut matches_count = 0usize;
    let mut reached = false;
    for root in roots {
        let metadata = tokio::fs::metadata(&root).await?;
        let files = if metadata.is_dir() { walk_paths(&root, None, false, glob.as_ref()).await?.into_iter().filter(|(_, is_dir)| !*is_dir).map(|(path, _)| path).collect() } else { vec![root.clone()] };
        for path in files {
            if reached { break; }
            let display = relative_path(&root, &path);
            let file = match tokio::fs::File::open(&path).await { Ok(file) => file, Err(_) => continue };
            let mut lines = tokio::io::BufReader::new(file).lines();
            let mut all_lines = Vec::new();
            while let Some(line) = lines.next_line().await.ok().flatten() { all_lines.push(line); }
            let matching: Vec<usize> = all_lines.iter().enumerate().filter_map(|(index, line)| matcher.is_match(line).then_some(index)).collect();
            if matching.is_empty() { continue; }
            let mut groups = Vec::<(usize, usize)>::new();
            for index in matching.iter().copied() {
                let group = (index.saturating_sub(context), (index + context).min(all_lines.len() - 1));
                if let Some(last) = groups.last_mut() && group.0 <= last.1 + 1 { last.1 = last.1.max(group.1); continue; }
                groups.push(group);
            }
            let mut last_end = None;
            for (group_start, group_end) in groups {
                let group_output_start = output.len();
                if last_end.is_some() { output.push_str("--\n"); }
                for (index, line) in all_lines.iter().enumerate().take(group_end + 1).skip(group_start) {
                    let is_match = matching.binary_search(&index).is_ok();
                    if is_match { matches_count += 1; if matches_count > max_results { reached = true; break; } }
                    if is_match { output.push_str(&format!("{display}:{}: {line}\n", index + 1)); }
                    else { output.push_str(&format!("{display}-{}- {line}\n", index + 1)); }
                }
                if reached && output.len() == group_output_start + if last_end.is_some() { 3 } else { 0 } { output.truncate(group_output_start); }
                last_end = Some(group_end);
                if reached { break; }
            }
        }
        if reached { break; }
    }
    if reached { output.push_str(&format!("[chanfs: max_results of {max_results} reached]")); }
    Ok(cap_response(output.trim_end_matches('\n').to_string()))
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ChanfsServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::default()
            .with_instructions("Use read_files, find_files, and search_text instead of shell commands.")
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
