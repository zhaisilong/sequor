# Upper Case Tutorial

This tutorial creates text files, uppercases each file in parallel, and writes a summary report.

## Commands

```bash
./sequor.sh validate tutorial/upper_case/pipeline.yaml
./sequor.sh list-tasks tutorial/upper_case/pipeline.yaml
./sequor.sh run tutorial/upper_case/pipeline.yaml
```

Run it a second time to observe cache hits for `uppercase` and `summarize`.

## Expected outputs

- Run directories under `tutorial/upper_case/.sequor/runs/<run_id>/`
- Runtime outputs under `tutorial/upper_case/.sequor/runtime/output/`
- Uppercased files under `.../runtime/output/upper/`
- Summary report at `.../runtime/output/summary/report.txt`
- Item manifests under `.../tasks/<task_name>/items/<item_id>/manifest.json`
- Cache index at `tutorial/upper_case/.sequor/cache/index.jsonl`
