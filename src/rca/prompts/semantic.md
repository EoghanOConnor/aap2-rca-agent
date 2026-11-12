Compare these newly analyzed jobs against recent known issues and
against one another. Return only the requested structured JSON.

Historical matching rules:
- Only consider new jobs with high or medium confidence.
- Match only when the error type, failing component, and failure mode are
  genuinely the same. A shared category or catalog item alone is insufficient.
- Use only matched_result_id values present in the supplied known-issue list.
- Give one concise sentence of evidence-based similarity reasoning per match.
- Include a confidence value and concise evidence for each match. Evidence
  should cite concrete signatures, components, resources, logs, or artifacts.
- Distinguish observed facts from inferences in the evidence.

Cross-job pattern rules:
- Report only clear overlaps across at least two new jobs: same failing
  component and substantially the same error/failure mode.
- Do not infer a pattern from a shared category alone. Return an empty array
  when no clear pattern exists.
- Include only job IDs from the supplied newly analyzed jobs.
- Include a confidence value and concise evidence for each pattern. Evidence
  should explain why the jobs belong to the same failure pattern.

Newly analyzed jobs:
$job_summaries

Known recent issues:
$known_issues
