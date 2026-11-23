Analyze AAP job $job_id using the existing artifacts in `$analysis_dir`.

Investigate the failure and return the final structured summary grounded in
these artifacts. Include the root cause, confidence, correlation identifiers,
evidence, and actionable recommendations. Include the exact job ID `"$job_id"`.

If `$analysis_dir/step4c_cost_estimate.json` exists, read it and report the
estimated cost exactly as given, following the cost-estimate rules in your
instructions.

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
