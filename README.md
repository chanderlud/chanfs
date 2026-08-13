# chanfs-mcp

Rust MCP stdio server exposing guarded file reads.

Set `CHANFS_ALLOWED_DIRS` to an OS path-list of allowed root directories. Paths are canonicalized at startup and per read; symlink escapes are rejected. Unset or empty disables reads. Invalid UTF-8 returns an error.

Tool:

- `read_files`: `{ "files": [{ "path": string, "start_line"?: number, "end_line"?: number }] }`

Line numbers are 1-based; `end_line` is inclusive. Reads page at most 400 lines by default.

## Download and validate

Download release assets from the [GitHub Releases page](https://github.com/chanderlud/chanfs/releases).

- Linux x86_64: `chanfs-mcp-x86_64-unknown-linux-gnu.tar.gz`
- Linux ARM64: `chanfs-mcp-aarch64-unknown-linux-gnu.tar.gz`
- Windows x86_64: `chanfs-mcp-x86_64-pc-windows-msvc.zip`
- Windows ARM64: `chanfs-mcp-aarch64-pc-windows-msvc.zip`

Each archive has matching `.sha256` checksum file. On Linux, run from directory containing both files:

```sh
sha256sum -c chanfs-mcp-x86_64-unknown-linux-gnu.tar.gz.sha256
```

On Windows PowerShell, compare `Get-FileHash` output with hash in matching `.sha256` file:

```powershell
$actual = (Get-FileHash .\chanfs-mcp-x86_64-pc-windows-msvc.zip -Algorithm SHA256).Hash.ToLower()
$expected = (Get-Content .\chanfs-mcp-x86_64-pc-windows-msvc.zip.sha256).Split()[0]
if ($actual -ne $expected) { throw "Checksum mismatch" }
"Checksum OK"
```

Verify build provenance with GitHub CLI:

```sh
gh attestation verify chanfs-mcp-x86_64-unknown-linux-gnu.tar.gz --repo chanderlud/chanfs
```

This requires `gh` CLI installed and verifies artifact was built by this repository's GitHub Actions workflow. It works independently of GitHub Releases page.

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
