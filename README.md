# chanfs-mcp

Rust MCP stdio server exposing guarded file reads, file discovery, and text search.

Set `CHANFS_ALLOWED_DIRS` to an OS path-list of allowed root directories. Paths are canonicalized at startup and per read; symlink escapes are rejected. Unset or empty disables reads. Invalid UTF-8 returns an error.

Tools:

- `read_files`: `{ "files": [{ "path": string, "start_line"?: number, "end_line"?: number }] }`
- `find_files`: find files and directories by path/name/glob.
- `search_text`: search file contents by literal text or regex.

Line numbers are 1-based; `end_line` is inclusive. Reads page at most 400 lines by default.

Example client config:

```json
{"mcpServers":{"chanfs":{"command":"C:\\path\\to\\chanfs-mcp.exe","env":{"CHANFS_ALLOWED_DIRS":"C:\\Users\\chand\\RustroverProjects"}}}}
```

Codex (`~/.codex/config.toml`):

```toml
[mcp_servers.chanfs]
command = "C:\\path\\to\\chanfs-mcp.exe"
env = { CHANFS_ALLOWED_DIRS = "C:\\Users\\chand\\RustroverProjects" }
```
