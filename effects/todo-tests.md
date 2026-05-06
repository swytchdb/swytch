# Test Refactoring TODO

Tests removed during the DAG walker rewrite that tested valid behaviors.
These should be re-implemented against the new walker (`dag.walk` + `reconstruct`).

## Ordered list merge behaviors (from ordered_merge_test.go)

- **Linear chain ordering**: ORDERED RPUSH chain [a→b→c] produces elements in causal order [a, b, c]
- **Fork preserves causal order**: Adding a concurrent branch must not reorder elements from the original causal chain (Jepsen bug: [a,b,c,d] became [d,e,a,b,c])
- **Stable under growth**: Ordering from a smaller DAG must be a subsequence of the ordering from the same DAG with additional concurrent effects
- **Deterministic across views**: Different index tip sets over the same effects must produce identical element ordering
- **Branch contiguity (§2.3)**: Concurrent branches' elements must form contiguous blocks — no interleaving. Applies with 2 branches, 3 branches, and with merge tip nodes
- **Merge tip doesn't change ordering**: A subscription/noop that merges two branch tips should not alter the element ordering vs the multi-tip case

## Sequential composition (from ordered_merge_test.go)

- **New elements after base**: `composeSequential(base, delta)` must place delta's elements after base's, regardless of fork-choice hash values

## Merge commutativity (from ordered_merge_test.go)

- **Ordered merge is commutative**: `Merge2(A, B)` and `Merge2(B, A)` produce the same element ordering for concurrent ORDERED branches

## Transaction filtering (from ordered_merge_test.go)

- **Cross-txn deps must not confirm**: When txn B's bind is confirmed and B depends on txn A's tentative write, A's effects must NOT become visible (root cause of Jepsen G0)

## LCA detection (from snapshot_test.go)

- **Single tip LCA**: LCA of a single-tip DAG is the tip itself
- **Two tips same fork**: LCA of two tips forking from the same node is that fork point
- **Three tips**: LCA of three tips from the same fork point is that fork point

## Benchmarks (from resolve_bench_test.go)

- **Real production DAGs**: Benchmarks using actual DAG encodings captured from redis-benchmark (N162 and N418 node DAGs). These should be re-implemented as benchmarks for the new walker.
