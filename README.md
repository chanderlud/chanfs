# chanfs-mcp

Rust MCP stdio server exposing guarded file reads.

Set `CHANFS_ALLOWED_DIRS` to an OS path-list of allowed root directories. Paths are canonicalized at startup and per read; symlink escapes are rejected. Unset or empty disables reads. Invalid UTF-8 returns an error.

Tool:

- `read_files`: `{ "files": [{ "path": string, "start_line"?: number, "end_line"?: number }] }`

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
