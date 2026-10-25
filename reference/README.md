# Reference implementation (released after Part 1)

* `replay/`  the reference replayer. `make attack` (from the repository root) builds it as image `ieraft-ref` and checks `scripts/p*.json`.
* `node/`    the reference node (scripted mode and live mode). Use it instead of your own node with
  `NODE_DIR=./reference/node docker compose up -d --wait node0 node1 node2 node3 node4`
  (add `MODE=live` for Part 3). Copy this directory into your repository next to `node/`
  (the Makefile looks for `reference/replay`, so keep the name `reference`).

Pre-Vote is **not** included in this release: a script with `prevote: true` is skipped, and a node started with Pre-Vote refuses to run. Part 3 asks you to implement Pre-Vote yourself; the complete reference is released after Part 3.
