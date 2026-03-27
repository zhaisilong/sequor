# Sequor

Sequor is a local-first DAG workflow engine with a minimal two-stage runtime.

## Quick Start

```bash
./sequor.sh validate tutorial/upper_case/pipeline.yaml
./sequor.sh list-tasks tutorial/upper_case/pipeline.yaml
./sequor.sh run tutorial/upper_case/pipeline.yaml
```

## Runtime Model

- `TaskPlanner.plan`: produce a list of planned items.
- `TaskRunner.run`: execute one planned item.
- Every item must provide `item_id`.

## Core Behavior

- Default parallelism is `flow.parallelism: 4`.
- `task.parallelism` overrides flow-level parallelism.
- Default item timeout is `600` seconds unless task overrides `timeout`.
- Cache fingerprint excludes `config_hash` and `run_dir`.
- Sequor does not auto-generate final summary reports; authors provide summary tasks/scripts.
- `script` tasks can access context, but do not write task manifests/logs under `.sequor`.
- Legacy `builder/processor/executor` schema is removed.

## YAML (Current Schema)

```yaml
flow:
  name: demo
  fail_fast: true
  parallelism: 4
  work_dir: .sequor
  output_dir: output

tasks:
  - name: step_a
    task_type: python_fn
    depends_on: []
    cache: true
    config:
      function: some.module.fn
      items:
        - item_id: sample_1
          input: a.txt

  - name: step_b
    task_type: shell_cmd
    depends_on: [step_a]
    timeout: 120
    config:
      default_item:
        item_id: summarize
        cmd: "python summarize.py"
```

## Planner Inputs

Default planner (`python_fn` / `shell_cmd` / `script`) supports:

- `config.items`
- `config.pattern` + `config.item_id_template`
- `config.from_task` (expects upstream dict outputs carrying `item_id`)
- `config.default_item`

`planner: csv_manifest` supports:

- `config.manifest`
- `config.item_id_field`
- `config.defaults`
- optional `config.cmd_template`

## CLI

```bash
./sequor.sh validate <pipeline.yaml>
./sequor.sh list-tasks <pipeline.yaml>
./sequor.sh run <pipeline.yaml>
```

## Doc Map

- [Upper Case Tutorial](tutorial/upper_case/README.md)
