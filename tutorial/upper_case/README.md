# Upper Case Tutorial

## What It Does

- Runs `uppercase` (`python_fn`) over `inputs/*.txt`.
- Runs `summary` (`shell_cmd`) to aggregate output stats.

## Run

```bash
./sequor.sh validate tutorial/upper_case/pipeline.yaml
./sequor.sh list-tasks tutorial/upper_case/pipeline.yaml
./sequor.sh run tutorial/upper_case/pipeline.yaml
./sequor.sh run tutorial/upper_case/pipeline.yaml
```

Expected behavior:

- Second run cache-hits `uppercase`.
- `summary` runs every time (`cache: false`).

## Inputs

- `tutorial/upper_case/inputs/*.txt`

## Outputs

- Uppercase files: `tutorial/upper_case/output/upper/*.txt`
- Summary CSV: `tutorial/upper_case/output/summary.csv`
- Uppercase manifests: `tutorial/upper_case/.sequor/tasks/uppercase/manifests/<item_id>.json`
- Uppercase task manifest: `tutorial/upper_case/.sequor/tasks/uppercase/task_manifest.json`
- Task logs: `tutorial/upper_case/.sequor/logs/*.log`

## Notes

- Item timeout defaults to 600s unless overridden in task config.
- Every planned item uses explicit `item_id`.
