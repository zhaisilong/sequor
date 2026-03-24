# Rosetta Project Tutorial (Mock)

## What It Does

- Loads case rows from `cases.csv` via `planner: csv_manifest`.
- Runs one `shell_cmd` task (`run_rosetta_mock`) per case.
- Produces per-case mock Rosetta outputs under `output/rosetta/`.

## Run

```bash
./sequor.sh validate tutorial/rosetta_project/pipeline.yaml
./sequor.sh list-tasks tutorial/rosetta_project/pipeline.yaml
./sequor.sh run tutorial/rosetta_project/pipeline.yaml
python tutorial/rosetta_project/summarize.py
```

Optional cache check:

```bash
./sequor.sh run tutorial/rosetta_project/pipeline.yaml
```

Second run should cache-hit `run_rosetta_mock` when inputs/config remain stable.

## Inputs

- Manifest: `tutorial/rosetta_project/cases.csv`
- Per-row fields used by command template: `case_id`, `pdb`, `xml`, `nstruct`

## Outputs

- Case outputs: `tutorial/rosetta_project/output/rosetta/<case_id>/`
- Summary reports (from standalone script):
  - `tutorial/rosetta_project/output/reports/rosetta_summary.json`
  - `tutorial/rosetta_project/output/reports/rosetta_summary.tsv`
- Task manifests: `tutorial/rosetta_project/.sequor/tasks/run_rosetta_mock/`

## Notes

- Sequor runs the iter-heavy task; summary export is author-managed script logic.
- Item timeout defaults to 600s unless overridden.
