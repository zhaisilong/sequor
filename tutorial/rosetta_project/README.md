# Rosetta Project Starter (Mock)

This tutorial starts a Rosetta-style workflow in Sequor without requiring Rosetta binaries.

## Run

```bash
./sequor.sh validate tutorial/rosetta_project/pipeline.yaml
./sequor.sh list-tasks tutorial/rosetta_project/pipeline.yaml
./sequor.sh run tutorial/rosetta_project/pipeline.yaml
./sequor.sh run tutorial/rosetta_project/pipeline.yaml
```

The second run should hit cache for both tasks.

## What it simulates

- Each case consumes `pdb + xml`.
- Per-case outputs include decoys, score files, and logs.
- A downstream Python task aggregates best scores into JSON/TSV.

## Key output paths

- Runtime output: `tutorial/rosetta_project/.sequor/runtime/output/`
- Case outputs: `.../runtime/output/rosetta/<case_id>/`
- Final report: `.../runtime/output/reports/rosetta_summary.tsv`
