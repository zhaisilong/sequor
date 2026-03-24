# Sequor

Sequor is a local-first, lightweight workflow engine with DAG orchestration.
It is designed as a practical `prefect-lite` core for file-based scientific and data pipelines.
This project can be paired with Claude/Codex to define task-level pipelines and run them directly.

## Features

- Declarative `Flow` + `Task` execution model
- DAG validation and dependency scheduling
- Task `builder / processor / executor` separation
- `python` and `bash` task support
- `serial` and `parallel` executors
- Item-level results and task/flow states
- Local cache for reruns + traceability manifests
- YAML pipeline definitions and CLI runner

## Quick Start

### 1. Run from source (no install required)

```bash
./sequor.sh validate tutorial/upper_case/pipeline.yaml
./sequor.sh list-tasks tutorial/upper_case/pipeline.yaml
./sequor.sh run tutorial/upper_case/pipeline.yaml
```

### 2. Optional editable install

```bash
python -m pip install -e .
sequor validate tutorial/upper_case/pipeline.yaml
```

## Tutorials

Two tutorials are included.

### 1) `tutorial/upper_case`

A minimal DAG pipeline:
- Generate ~12 txt files
- Uppercase each file in parallel
- Summarize outputs

Run:

```bash
./sequor.sh run tutorial/upper_case/pipeline.yaml
./sequor.sh run tutorial/upper_case/pipeline.yaml
```

Second run should show cache hits.

### 2) `tutorial/rosetta_project`

A Rosetta-style starter workflow (mocked binary):
- Per-case input: `pdb + xml`
- Per-case outputs: logs, decoys, scores
- Downstream aggregation report

Run:

```bash
./sequor.sh run tutorial/rosetta_project/pipeline.yaml
./sequor.sh run tutorial/rosetta_project/pipeline.yaml
```

## Repository Layout

```text
src/sequor/
  core/      # flow, scheduler, runner, states, cache, context
  tasks/     # python and bash task implementations
  io/        # yaml parser and config loader
  cli/       # command line entry
  utils/     # hashing/import/path helpers

tutorial/
  upper_case/
  rosetta_project/

projects/    # local workspace placeholder (tracked only with .gitkeep)
```

## CLI

```bash
./sequor.sh validate <pipeline.yaml>
./sequor.sh list-tasks <pipeline.yaml>
./sequor.sh run <pipeline.yaml>
```

Optional:

```bash
./sequor.sh run <pipeline.yaml> --work-dir /abs/path/to/workdir
```

## Notes

- Paths in YAML are resolved relative to the pipeline file directory.
- Runtime outputs are written under each tutorial's `.sequor/` directory.
- `.gitignore` excludes generated runtime/cache/bytecode artifacts.

## Next Step: Real Rosetta Integration

Use `tutorial/rosetta_project` as the template and replace `mock_rosetta.py` with real Rosetta command invocation while keeping the same Sequor task boundaries (`run_rosetta` -> `summarize`).
