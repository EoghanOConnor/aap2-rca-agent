Analyze AAP job $job_id using the existing artifacts in `$analysis_dir`.

Investigate the failure and return the final structured summary grounded in
these artifacts. Include the root cause, confidence, correlation identifiers,
evidence, and actionable recommendations. Include the exact job ID `"$job_id"`.

Every evidence entry requires `source`, `timestamp`, and `message`, including
GitHub evidence. For configuration or code with no event timestamp in the
artifacts, use `"timestamp": ""` to indicate that the time is unavailable;
never invent a timestamp. Include `github_path` for GitHub evidence.

Use the configured `StructuredOutput` tool to return the summary fields
directly at the root of its input. The Agent SDK validates the structured output
against the configured schema and handles any retries. Python writes the
successful structured result to `$summary_path`. Do not write that file yourself
or upload the analysis.

Follow this summary schema:

```json
$summary_schema
```
