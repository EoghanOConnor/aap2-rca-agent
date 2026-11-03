# provision-cost-estimate

Estimates what a failing provision is costing, given its provision UUID. See
[SKILL.md](SKILL.md) for the method and output format.

## Requirements

This skill does not ship or configure any MCP server. It needs the
**`reporting-db-prod`** MCP server to be connected in the environment that runs
it. That server provides read-only access to the RHDP reporting PostgreSQL
database.

The skill uses these tools, both listed in `allowed-tools` in `SKILL.md`:

- `mcp__reporting-db-prod__query`
- `mcp__reporting-db-prod__describe_table`

If `reporting-db-prod` is not connected, these tools do not exist and the skill
cannot run. It will not produce an estimate.

### Connecting the server

Add `reporting-db-prod` as an HTTP MCP server in whichever scope fits (user
config, a project `.mcp.json`, or the agent's own MCP configuration). For
example, with the Claude Code CLI:

```bash
claude mcp add --transport http reporting-db-prod <reporting-mcp-url> \
  --header "Authorization: <token>"
```

Get the server URL and an access token from the RHDP reporting platform team.
Keep the token out of version control: do not commit it, or a `.mcp.json`
containing it, to this repository.

### Optional: the bundled script

`scripts/catalog_metrics.py` runs the whole method in one command and is
what the skill tries first:

```bash
export REPORTING_MCP_TOKEN=<token>   # same token as the MCP server above
python3 -I scripts/catalog_metrics.py <provision-uuid>
```

It needs only the Python standard library. `env.example` lists the variables
(`REPORTING_MCP_TOKEN`, and optionally `REPORTING_MCP_URL` to override the
default endpoint). If no comparison provisions exist within 30 days it falls
back to a catalog-level median (`--baseline` shows it as a cross-check). The script calls the MCP server over HTTP itself, so it works
without a connected MCP client, but it needs the token in the environment.
Without `REPORTING_MCP_TOKEN` the skill falls back to the manual SQL steps in
`SKILL.md`.

### Checking the connection

Run `/mcp` in Claude Code and confirm `reporting-db-prod` is listed as
connected. For the Agent SDK, confirm the server is included in the agent's
MCP server configuration.
