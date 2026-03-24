from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdb", required=True)
    parser.add_argument("--xml", required=True)
    parser.add_argument("--out_dir", required=True)
    parser.add_argument("--nstruct", type=int, default=3)
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    logs_dir = out_dir / "logs"
    score_dir = out_dir / "scores"
    decoy_dir = out_dir / "decoys"
    for d in [out_dir, logs_dir, score_dir, decoy_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pdb_name = Path(args.pdb).stem
    random.seed(args.seed + len(pdb_name))

    (logs_dir / "command.log").write_text(
        f"mock_rosetta --pdb {args.pdb} --xml {args.xml} --nstruct {args.nstruct}\n",
        encoding="utf-8",
    )
    (logs_dir / "protocol.log").write_text(Path(args.xml).read_text(encoding="utf-8"), encoding="utf-8")

    scores = []
    for i in range(args.nstruct):
        tag = f"{pdb_name}_{i:04d}"
        score = round(-120.0 + random.random() * 20.0, 3)
        rmsd = round(0.5 + random.random() * 2.0, 3)
        scores.append({"tag": tag, "score": score, "rmsd": rmsd})
        (decoy_dir / f"{tag}.pdb").write_text(
            f"REMARK MOCK DECOY {tag}\nREMARK SCORE {score}\n",
            encoding="utf-8",
        )
        time.sleep(0.01)

    best = min(scores, key=lambda x: x["score"])
    (score_dir / "score.json").write_text(json.dumps(scores, indent=2), encoding="utf-8")
    (out_dir / "summary.json").write_text(
        json.dumps(
            {
                "input_pdb": args.pdb,
                "xml": args.xml,
                "nstruct": args.nstruct,
                "best": best,
                "count": len(scores),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"MOCK_ROSETTA_DONE {pdb_name} best={best['score']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
