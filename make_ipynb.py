#!/usr/bin/env python3
"""Regenerate local-ssd-raid0-io-test.ipynb from READABLE.md (single source of truth).

Binds the repo to the top-down execution model: one markdown cell (section heading +
GREEN marker line from the READABLE.md marker table) before each of the 9 C0..C8 python
cells. Kit-internal "[cell_*.txt]" source tags are stripped from headings (those files
live on the login node, not in this repo). SANITIZE enforces the pristine-text policy:
the legacy launcher strings must not appear in the runnable notebook at all.
"""
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE / "READABLE.md"
OUT = HERE / "local-ssd-raid0-io-test.ipynb"

SANITIZE = [
    ("no docker/hostimg involved", "no docker launch channel involved"),
]

HEADING = re.compile(r"^## (C\d) \u2014 (.*)$")
TABLE_ROW = re.compile(r"^\| (C\d) \|")


def md_cell(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True), "id": None}


def code_cell(src_text):
    for bad, good in SANITIZE:
        src_text = src_text.replace(bad, good)
    return {
        "cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
        "source": src_text.splitlines(keepends=True), "id": None,
    }


def main():
    lines = SRC.read_text().splitlines()

    green = {}
    in_table = False
    for ln in lines:
        if ln.startswith("## Marker table"):
            in_table = True
            continue
        if in_table and ln.startswith("## "):
            in_table = False
        if in_table:
            m = TABLE_ROW.match(ln)
            if m:
                green[m.group(1)] = ln

    sections = []
    cur = None
    in_fence = False
    for ln in lines:
        if in_fence:
            if ln.strip() == "```":
                in_fence = False
                cur["code"].append("")
            else:
                cur["code"].append(ln)
            continue
        h = HEADING.match(ln)
        if h:
            cur = {"cid": h.group(1), "title": h.group(2), "code": []}
            sections.append(cur)
            continue
        if ln.startswith("## "):
            cur = None
        if ln.startswith("```python") and cur is not None:
            in_fence = True
            cur["code"].append("")

    ids = [s["cid"] for s in sections]
    assert ids == [f"C{i}" for i in range(9)], f"expected C0..C8, got {ids}"
    for s in sections:
        assert s["cid"] in green, f"no GREEN table row for {s['cid']}"

    title = lines[0].lstrip("# ").strip()
    cells = [md_cell(
        f"# {title}\n"
        "\n"
        "**Execution model (binding):** fresh kernel → run this notebook TOP-DOWN once, "
        "C0→C8. Individual cells are NEVER re-run; the only recovery is restart kernel + "
        "run from C0. Runbook and costs: `README.md`; canonical paste source: `READABLE.md`."
    )]
    for s in sections:
        heading = re.sub(r"\s*\[cell_.*\.txt\]\s*$", "", s["title"])
        cells.append(md_cell(f"## {s['cid']} — {heading}\n\n**GREEN:** {green[s['cid']]}\n"))
        cells.append(code_cell("\n".join(s["code"])))

    for i, c in enumerate(cells):
        c["id"] = f"cell-{i:02d}"
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    blob = json.dumps(nb, indent=1, ensure_ascii=False) + "\n"
    payload = json.dumps(nb["cells"], ensure_ascii=False)
    n_host = payload.lower().count("hostimg")
    n_null = payload.lower().count("nullimg")
    n_rerun = payload.lower().count("re-run")
    assert n_host == 0 and n_null == 0, f"banned launcher string leaked: hostimg={n_host} nullimg={n_null}"
    assert n_rerun <= 3, f"too many 're-run' occurrences: {n_rerun}"
    n_code = sum(1 for c in nb["cells"] if c["cell_type"] == "code")
    assert n_code == 9, f"expected 9 code cells, got {n_code}"

    OUT.write_text(blob)
    json.loads(OUT.read_text())
    print(f"OK wrote {OUT.name}: {len(nb['cells'])} cells ({n_code} code), "
          f"hostimg={n_host} nullimg={n_null} re-run={n_rerun} (<=3), json.load passed")


if __name__ == "__main__":
    sys.exit(main())
