# cohort-stream-processor — deferred issues

Tracked residuals and follow-ups, deliberately not closed in the change that recorded them. Each
entry notes the milestone by which it must be addressed.

## Rebalance / partition lifecycle

- **Re-acquire between the post-`join` `owned` re-check and `delete_partition`** (`revoke_partition_drain`,
  `consumers/events.rs`). Microscopic window: a reassign that lands *after* the post-join re-check but
  *before* `delete_partition` still wipes a now-re-owned slice. Degrades to "the new tenure replays from
  the committed offset" — the same residual class as the documented cold-rebuild-on-move, acceptable for
  M1 (shadow-only). **Close before M5** (the un-gated read path): needs the PR-3.5 offset-aligned restore
  (pause → restore → seek → resume), not an M1-era lock-across-I/O.

- **Sequential drain on a many-partition revoke** (`run_rebalance_worker`, `partitions/rebalance.rs`).
  A scale-down or node loss revokes many partitions at once; the drain joins each worker serially, which
  can exceed the SIGTERM grace window and leave slices un-deleted. Recovered on the next clean start by
  `wipe_on_start`, so not a correctness bug, but it widens the cold-rebuild window. Consider a
  bounded-concurrency drain plus a per-callback total-drain-duration metric to make the budget visible.

- **`shutdown` vs concurrent `dispatch` — RESOLVED (Slice C2, D11).** The merge-protocol follower
  consumers make multiple dispatching loops real, so the documented race (a dispatch racing
  `shutdown` registers a worker sender after `router.clear()` → the join hangs forever) got the
  predicted fix: a `draining: AtomicBool` on `EventDispatcher`, stored **first** in `shutdown()` and
  consulted by `dispatch`/`dispatch_merges`/`dispatch_transfers`/`route_sweep` *and* `ensure_worker`
  (the spawn seam). The gates alone are racy — a `draining` load that passed before the flip can
  still reach the spawn — so the hang-proof guarantee is structural: `clear()` closes the
  `PartitionRouter` **terminally**, and `add_partition` refuses while holding the same shard guard
  the clear's removal pass needs, so no sender can be registered after (and survive) the clear. A
  post-shutdown dispatch is dropped unmarked (and counted on the per-topic
  `*_skipped_not_owned_total`), so Kafka replays it on the next owner. Pinned by
  `draining_gate_rejects_all_dispatch_after_shutdown_without_hanging` +
  `post_shutdown_registration_attempt_is_refused_by_the_closed_router` (`consumers/events.rs`) and
  `clear_closes_the_router_so_add_partition_refuses` (`partitions/router.rs`).

## Tests

- **`cooperative_sticky_migration_preserves_offsets_and_partition_colocation`** (broker-backed,
  `#[ignore]`d, `tests/events_consumer.rs`) deterministically fails its *state-presence* assertion
  (`entered_a + entered_b == PERSONS`, ~`a=2, b=0`) on a fast local broker (Redpanda). Pre-existing and
  reproduced identically on the base commit (`3b735f8`) — **not** caused by the ownership-gate change.
  Root cause: Batch 2 is produced the instant consumer B is spawned, so on a fast broker pod A consumes
  *and commits* those events before the cooperative-sticky rebalance migrates the partition; A then
  deletes the just-built slice on revoke, and B has nothing left to replay → the moved person's state is
  absent in both pods. The Kafka-no-loss (line ~912) and partition-co-location (lines ~919-925)
  assertions — the invariants the ownership gate protects — pass every run. Fix the test by
  synchronizing Batch 2 to *after* B's assignment settles (e.g. wait for B to own partitions before
  producing), not by changing production code. The merge keystone
  (`cooperative_migration_mid_merge_stream_loses_no_merge`, `tests/merge_consumer.rs`) applies that
  fix: its post-migration batch is produced only after the split settles (disjoint full coverage,
  stable across a re-check), and its batch-1 assertions are interleaving-independent
  (count-exact-wherever-present, never presence).

- **Local broker partition budget: leaked 64-partition test topics wedge later runs.** The
  `tests/merge_consumer.rs` suite creates its topics at the production partition count (the merge
  protocol's partition arithmetic is the thing under test), and a single-node Redpanda caps
  partitions per shard — after a handful of leaked runs every later `create_topics` fails with
  `InvalidPartitions` (hit live 2026-06-11). The tests now delete their topics on the way out; a
  *panicking* test still leaks, and `tests/events_consumer.rs` topics (4-partition, 16× cheaper)
  always leak. The `merge_consumer.rs` module doc carries the uuid-suffix `rpk topic delete` sweep
  that clears both families.

## Sweep / time-driven eviction (PR 2.3)

- **`BehavioralSingle` calendar-day eviction (D9) — RESOLVED 2026-06-10 (slice 5, commit 1).**
  `EvictionWindow` split by interval kind: a whole-day window (`day`/`week`/`month`/`year`) is
  `RelativeDays { days }`, evicting at tz-local midnight of `day(newest_event) + days + 1` — the same
  boundary `daily_eviction_deadline` uses, so a `performed_event` single and a
  `performed_event_multiple` bucket over the same window now evict together. A sub-day window
  (`hour`/`minute`) stays `RelativeSeconds` (instant-granular; the old pipeline does not day-floor
  `-Nh`). `earliest_eviction_at_ms` gained a `tz` param fed from `filters.timezone` at
  `event_path.rs`. No migration: stored deadlines are advisory and the new deadline is strictly later,
  so no spurious early `Left`s during transition. Pinned by `calendar_identical_events_share_one_eviction_midnight`
  (in-module) + `single_eviction_deadline_is_team_local_midnight_for_both_edge_persons` (`tests/stage1_worker.rs`,
  Kolkata) + `single_does_not_evict_before_its_calendar_midnight_and_does_after` (`tests/sweep_worker.rs`).
  Unblocks the M2 ±5 min `left`-transition parity gate for `performed_event` shapes (incl. compositions).

- **`route_sweep` logs a WARN per idle (owned-but-workerless) partition every sweep cycle**
  (`reason=no_worker`, `partitions/router.rs`). Benign (no events ⇒ no worker ⇒ nothing to evict — the
  tested-benign path) but steady WARN noise on a sparse/idle deploy (1/partition/interval). Consider
  demoting the sweep-route drop to debug, or skipping the route when no live worker exists.
  `route_redrive` (Slice C2) routes through the same drop path, adding a second WARN per workerless
  partition at the redrive cadence (`merge_redrive_interval_ms`, default 60 s) — a fix here should
  cover both ticks.

- **Sweep `Left`/`Entered` is at-most-once across a crash** (`handle_sweep`, `workers/worker.rs`). The
  sweep produces its membership changes (a daily `eq`/`lte`/`lt` slide can `Entered`, not only `Left`)
  and awaits acks *before* applying the state mutation (produce-before-write), so a clean produce
  failure replays against the still-un-evicted state. A crash strictly between the produce-ack and the
  `store.write_batch` cannot re-emit: the state is un-advanced but the change is gone. Same posture as
  the event-path shadow output below. **Close before the M4 cut-over**; PR 3.5 durability
  (persist/restore the per-worker `EvictionQueue`) is the real fix.

- **No test for the sweep `write_batch`-failure branch** (`handle_sweep`, `workers/worker.rs`). The
  produce-failure retry is covered (`tests/sweep_worker.rs`), but "produce succeeded, state write
  failed → reschedule, at-least-once duplicate next tick" is not — needs a store-write fault seam.

- **The per-worker `EvictionQueue` is in-memory only** (`run_worker`, `workers/worker.rs`). It is
  rebuilt from events per tenure (a revoke drops the worker and its queue; `wipe_on_start` clears any
  on-disk state). On a crash-restart the queue is empty until new events reschedule each key, so a
  state whose newest event predates the restart is not re-evaluated for eviction until either a new
  event for that key arrives or a rebalance reclaims the slice. Bounded in practice by the topic's
  24 h retention vs the ≥1-day windows; **PR 3.5** rebuilds the queue on recovery.

- **Sweep `Delete` leaves a stale `cf_person_index` LSK — RESOLVED (M3 PR 3.1, Slice C1, commit 0).**
  The `handle_sweep` `EvictionAction::Delete` arm now also `merge_person_index(Remove)` in the same
  `WriteBatch`, so a full-expiry delete leaves no stale LSK for the merge drain to enumerate (the
  `Stage1Key` carries the person-index coordinates, so no extra read). The drain still tolerates a
  residual hole (a `multi_get_stage1` miss is a counted skip). Pinned by
  `sweep_full_expiry_delete_retracts_the_person_index_entry` (`tests/sweep_worker.rs`).

## Compressed history (>180-day windows, M2)

- **L8 — no per-person bitmap for dense-event >180-day users** (`stage1/compressed_history.rs`). The
  compressed variant stores sparse `(day_idx, count)` entries, bounded by `window_days + 1` (≤ the dense
  daily array it replaces), so it only bloats for a user active on very many *distinct* days within the
  window. The TDD's per-person-bitmap / `state_size_bytes` alarm optimization is **Phase 7** — not
  needed for correctness and strictly smaller than dense for the common case.

- **Compressed-history merge concat — SHIPPED (M3 PR 3.1, Slice C1, commit 3).** The RLE
  union/concat lives in `merge/compressed_concat.rs` (`union_by_day`: sum same-day counts, sorted,
  zero-free, `window_start_day = max`) and is wired into `merge/rules.rs::merge_records`. No index
  alignment (RLE stores absolute days, per TDD §4.5); the predicate sums all entries and the next
  sweep slide prunes any now-out-of-window day.

## Output / durability

- **At-most-once shadow produce on a crash between produce and commit** (already known, M4). The worker
  produces membership changes, awaits acks, then marks the offset; a crash strictly between the ack and
  the offset commit re-produces on replay (idempotent for the parity diff), but a crash strictly between
  the produce attempt and a *failed* ack cannot re-emit. Must be resolved before the M4 cut-over from the
  shadow topic to the real `cohort_membership_changed` topic.

- **The sweep's Stage 2 compose pass is write-before-produce → at-most-once, with no self-heal for a
  dormant person** (`handle_sweep`, `workers/worker.rs`). `compose_stage2` commits `cf_stage2` before
  the second produce, so a failed produce — or a crash between the `cf_stage2` write and the ack —
  drops the composed flip with nothing to retry (the recompute diffs against the already-advanced bit
  and emits nothing). An **active** person self-heals on their next event; a **dormant** person's flip
  is lost for good. Same at-most-once class as the existing event/sweep shadow output, acceptable
  pre-M4 (shadow-only); commit-after-ack durability (**PR 3.5**) is the real fix. Pinned by
  `sweep_compose_produce_failure_does_not_corrupt` (`tests/sweep_worker.rs`).

## Stage 2 / eligibility (M3)

- **Cohort dependency graph + Tarjan cycle exclusion + dependency-aware eligibility shipped (slice 5,
  commit 2).** `filters::cohort_graph::analyze` (petgraph `tarjan_scc`) builds the per-team
  referrer→referenced graph at freeze and surfaces (a) cohorts in a reference cycle — an SCC of size
  > 1 or a self-loop — and (b) a referenced-before-referrer `refinement_order`.
  `stage2::eligibility::refine_ref_bearing` then narrows each pass-1 `Excluded(HasCohortRef)` cohort to
  `CycleDetected` (in a cycle), `UnresolvedRef` (a transitive target is missing or non-transport-excluded),
  or keeps it `HasCohortRef` (all targets resolvable, no cycle — the exact set that flips to composable
  once cascade transport lands, so `excluded_has_cohort_ref` is now that slice's sizing metric).
  `reverse_index::freeze` is now three passes (classify → ref refinement → emit-map build from the final
  class). New label-free `cohort_in_cycle_total` counter (ids in the freeze `warn!`, not a label). Refs
  still **do not compose or emit** in this slice — the per-hop `cascade_chain` runtime check,
  `stage2::cascade::should_emit`, and the `cohort_cascade_events` topic (TDD PR 3.4) remain deferred to
  the transport slice.
  - **Sizing caveat (negated ref to a missing target):** at transport time, absence-under-negation reads
    `true` (TDD §2.7), so a cohort whose *only* unresolvable target is a **negated** ref to a missing id
    is actually composable. This slice conservatively counts it as `UnresolvedRef`, slightly undercounting
    the `HasCohortRef` sizing metric. Revisit when transport lands (the graph ignores negation by design,
    so the fix is in `refine_ref_bearing`'s resolvability check, gated on the leaf's negation bit).

- **Per-leaf negation in Stage 2 composition shipped (slice 4).** Cohorts with negated person/behavioral
  leaves are now `Stage2Composable` (no longer wholesale-excluded). Each leaf's membership is XOR'd with its
  `negated()` bit at composition time, per the `hogql_cohort_query.py` oracle's per-person set algebra.
  **TDD D12 superseded for composition**: the oracle honors negation (`prop.negation`), so the new pipeline
  does too — D12's "silent-ignore" would diverge on every negated cohort. Root-negated trees are excluded
  (`TopLevelNegation`), mirroring the oracle's `ValidationError`; empty groups in the original JSON are
  excluded (`EmptyGroup`), closing the always-true empty-AND divergence. Metric label rename:
  `excluded_has_negation` → `excluded_top_level_negation` (shadow-only, no dashboards pinned). Deploy note:
  cohorts moving Excluded→Composable have no `cf_stage2` bits; first post-deploy leaf flips emit initial
  `Entered`s and the parity diff may transiently spike.

- **Event-path Stage 2 composition shipped** (slice 2): `stage2::evaluator` (`leaf_membership` +
  `evaluate_tree`), `stage2::state` (`Stage2State` → `cf_stage2`), `store::multi_get_stage1`,
  `filters::reverse_index::by_lsk_to_composable_cohorts`, and `workers::stage2_path::compose_stage2`
  wired into `handle_event`. A `Stage2Composable` cohort now re-evaluates and emits `Entered`/`Left`
  when a leaf flips on the **event** path, diffing the new bit against `cf_stage2`.

- **Sweep-path Stage 2 composition shipped** (slice 3): `handle_sweep` now routes the tick's
  `LeafTransition`s through the same `by_lsk_to_composable_cohorts → compose_stage2` fan-out as the
  event path, after the Stage 1 eviction `write_batch` commits (so compose reads post-eviction
  `cf_stage1`; a fully-drained `Delete` reads as a non-member), and produces the composed changes in
  a second, independent produce (disjoint cohort ids from the single-leaf produce). This closes the
  **staleness invariant** slice 2 opened: a sibling-window-expiry `Left` (or a daily/compressed slide
  `Entered`) between events — including for a churned/dormant person who never returns — now emits
  from the sweep alone. Covered end-to-end in `tests/sweep_worker.rs` (dormant-person `Left`,
  bidirectional slide `Entered`, single-leaf + composable shared-leaf fan-out, same-tick two-leaf
  dedup, compose-after-delete, compose-produce-failure). Unblocks the M2 ±5 min `left`-transition
  parity gate for multi-leaf cohorts (the `BehavioralSingle` instant-vs-calendar eviction bug above
  still gates `performed_event` shapes).

## Cross-partition merge protocol (M3 PR 3.1, Slices C1 + C2)

Slice C1 shipped the **Kafka-free protocol core** — four merge CFs, per-leaf merge rules, the
murmur2 partitioner, tombstone resolution, and the sink-free drain/apply handlers. Slice C2 wired
the **Kafka plumbing** end-to-end: the two assignment-mirrored follower consumers
(`cohort-stream-merges`, `cohort-stream-merge-apply`), the transfer + straggler-re-key producers,
the pending-transfer redrive, and per-topic offset gating. The protocol is live in-process but
**production traffic stays zero until C3** ships the Node merge producer and the Terraform topics;
until then it is exercised by the test suites and the local harness. Residuals below are C3 work
or deferred follow-ups, tracked against later slices.

- **`CrossPartition` tombstone redirect is dropped, not re-produced — RESOLVED (Slice C2).**
  `Redirected::DropCrossPartition` is replaced by `Redirected::ReKey`: the straggler is rewritten to
  the target (`person_id` rewritten — tombstones are slice-prefixed, so an un-rewritten event would
  miss the target's tombstone and rebuild orphan P_old state in the wrong slice; FIRST-origin
  `redirected_from` kept; `redirect_hops` incremented per produced hop, capped at 8 (D13) with an
  inline-at-best-known-target degrade + `merge_redirect_hop_capped_total`). The batch epilogue
  produces re-keys on `stream_event_sink` keyed to the target (D1) and ack-gates the events offset
  alongside the membership produce — this hold IS self-healing (the ReKey path writes no state, so
  redelivery re-resolves and re-produces; a duplicate carries its original source coords and is
  absorbed by the target's `redirect_dedup[origin]`).
  `merge_tombstone_redirects_total{path="re_keyed"}` counts only post-ack (label renamed from
  `cross_partition`); `inline` semantics unchanged. Pinned by
  `cross_partition_redirect_re_keys_the_straggler_to_the_target`,
  `re_key_produce_failure_holds_the_events_offset_until_redelivery_succeeds`,
  `re_keyed_event_folds_into_p_new_exactly_once_via_redirect_dedup`, and
  `hop_capped_redirect_processes_inline_at_the_best_known_target` (`workers/worker.rs`). Residual:
  the re-key hold rides the events-path offset gate, so it inherits the known **sub-batch leapfrog**
  envelope — a later sub-batch marking past a held offset drops the un-produced re-key for good
  (whole-event loss, not just a flip emission); closed by the same at-least-once cutover that fixes
  the shadow-output gate.

- **Transfer-produce failure posture — IMPLEMENTED worker-side (Slice C2, D3/D4).**
  `workers/merge_path.rs` retries the transfer produce inline with bounded backoff while holding the
  partition worker (default 5 retries, 0.5→8 s capped, ≈15.5 s total — deliberately under the TDD's
  10×→60 s so a blocked worker stays inside the liveness deadline and the 30 s graceful-shutdown
  window), falling back to leave-in-pending + skip-mark + `merge_transfer_produce_failure_total`. An
  `AlreadyDrained` redelivery re-produces the still-staged outbox entry (the C1 doc contract). The
  **periodic redrive tick is live**: `RedriveSweeper` (`merge/redrive.rs`) rides a second
  `run_sweep_loop` (`merge_redrive_interval_ms`, default 60 s) and routes `RedrivePendingTransfers`
  to every owned partition via `route_redrive` — no-spawn like the sweep, and the workerless
  RouteError is benign because an outbox entry can only be staged by a live worker this tenure. The
  worker (`handle_redrive`) scans `cf_pending_transfers`, sets `merge_pending_transfers{partition}`,
  re-produces each entry with a **single attempt per tick** (never the inline budget — backing off
  would hold the worker ≈15 s per dead entry), and on ack clears the slot and marks the stored
  merge-message coords on the merge tracker (why `PendingTransfer` carries them). Pinned by
  `redrive_re_produces_a_staged_entry_clears_it_and_marks_the_stored_coords`,
  `redrive_produce_failure_leaves_the_entry_and_the_offset_for_the_next_tick`,
  `redrive_makes_a_single_produce_attempt_per_tick_with_no_backoff`,
  `redrive_leaves_an_undecodable_entry_in_place_and_still_recovers_the_rest`
  (`workers/merge_path.rs`), plus
  `route_redrive_recovers_an_inline_exhausted_transfer_within_the_tenure` and
  `route_redrive_duplicate_of_an_already_acked_transfer_applies_once` (`consumers/events.rs`).
  **Cross-tenure the gap remains**: the outbox is wiped on revoke (`delete_partition`) and on
  restart (`wipe_store_on_start`), so recovery across tenures activates with Track D durability /
  the offset-aligned restore (PR 3.5).

- **The inline transfer-retry true worst case is ~135 s against a black-hole broker, not ≈15.5 s.**
  D4's inline budget counts only the backoff sleeps (5 × 0.5/1/2/4/8 s ≈ 15.5 s); each of the 6
  produce attempts can itself block until the producer's `message.timeout.ms` = 20 s when the broker
  accepts connections but never acks. True worst case ≈ 6 × 20 s + 15.5 s ≈ 135 s of worker hold —
  past the 60 s liveness deadline twice over, consuming 135 s of the 180 s (60 s × stall 3) trip
  budget, and far past the 30 s graceful-shutdown window the ≈15.5 s figure was sized against (the
  fast-fail case). Consider a lower per-attempt produce timeout for the transfer sink (a dedicated
  producer config rather than the shared 20 s) before C3 makes merge traffic real.

- **Redrive ticks share the sweep-loop metrics.** The second `run_sweep_loop` counts its ticks on
  the same label-free `sweep_cycles_total` / `sweep_cycle_duration_seconds` as the eviction sweep,
  so the two cadences are indistinguishable in metrics (the redrive's own signal is the
  `merge_pending_transfers` gauge). Add a loop label if the conflation ever matters operationally.

- **Held merge/transfer offsets are leapfroggable within a batch (D8 residual).** A store-error hold
  (drain or apply) skips the mark, but `mark_processed` is monotonic-max, so a *later* merge/transfer
  for the same partition in the same tenure marks past it and the held message is never redelivered.
  For a produce-exhaustion hold the outbox + redrive recover it (that's D3's design); for a
  store-error hold there is **no outbox entry** — the merge is lost if anything later marks before
  redelivery. Bounded by the merge rate (~12/s global ⇒ two merges for one partition in one tenure
  window is rare) and surfaced as drain/apply `warn!`s; revisit if store errors stop being
  effectively-fatal-rare.

- **(F3) Tombstone permanence: `delete_partition` after a committed merge offset wipes a tombstone
  that never self-heals.** A revoke (or `wipe_store_on_start`) deletes the slice's four merge CFs
  along with the stage CFs (`Cf::ALL` fan-out, `store/rocks.rs::delete_partition`). For Stage 1
  state that is the known cold-rebuild class — the next tenure replays `cohort_stream_events` from
  the committed offset and re-derives it. `cf_merge_tombstones` is **not** in that class: its only
  source is the merge message on `person_merge_events`, and once the drain committed that offset
  (transfer produced + acked) the message never redelivers, so the wiped tombstone is gone for
  good. A post-wipe straggler for P_old then resolves no tombstone and is processed as P_old —
  orphan/resurrected P_old state in the new tenure's slice, split-brain against P_new's merged
  state. Bounded by the straggler tail (only events for already-merged-away persons that arrive
  after the wipe), comparator-scoped like the drain's no-`Left` decision. **PR 3.5's offset-aligned
  restore must cover the 4 merge CFs**, not just `cf_stage1`/`cf_person_index`/`cf_stage2` —
  restoring the stage CFs without the tombstones reintroduces exactly this hole.

- **Corrupt `cf_pending_transfers` value posture (Slice C2).** An `AlreadyDrained` redelivery whose
  staged outbox entry fails to decode marks the offset anyway (holding would wedge the partition on
  it forever) and leaves the entry in place, so the redrive scan surfaces the same decode failure
  every tick. No counter; the `warn!` carries the ids. Should the redrive (or PR 3.5 recovery) want
  to GC such entries, that is where the delete belongs.

- **Follower-group offsets expire after `offsets.retention.minutes` — HANDLED (Slice C2).** The two
  never-`subscribe()` consumers (`cohort-stream-merges`, `cohort-stream-merge-apply`) are `Empty`
  groups, so their committed offsets are pruned after the broker's retention (default 7 d). The
  mirror re-establishes them on every assignment (`stored_tpl`: `Offset::Stored`, falling back to the
  hard-coded `auto.offset.reset=earliest` in `follower_client_config` once pruned). The residual
  cost is a post-pruning replay of up to the merge-topic retention, absorbed by the drain/apply
  source-coords markers.

- **A failed follower `incremental_assign` on a fresh acquire leaves that partition unmirrored until
  the next rebalance event that includes it — potentially indefinitely on a stable group.** Merges
  for it stall, with lag on the follower's consumer group as the only signal (warn-and-continue
  posture, D5).

- **Revoke-time follower commit skipped (redelivery + dedup posture).** Neither the rebalance
  worker's revoke arm nor the followers' shutdown path commits offsets the drain is still marking:
  the revoke unassigns the followers *before* `revoke_partition_drain`, and the followers' final
  sync commit covers only what was marked when their loops stopped. The uncommitted tail simply
  redelivers to the partition's next owner, whose `cf_merge_drains_applied`/`cf_merge_applied`
  source-coords markers absorb the replay — cheaper and simpler than synchronizing a commit against
  an in-flight drain, at the cost of bounded duplicate fetches (visible as brief merge-group lag).

- **Merge-CF GC / retention deferred.** `cf_merge_drains_applied` / `cf_merge_applied` (idempotence
  markers) and `cf_merge_tombstones` accumulate; the TDD specifies eviction once the merge-topic
  (7 d) and `clickhouse_events_json` (7 d) + `cohort_stream_events` (24 h) retentions lapse, via the
  sweep with a separate `tombstone_eviction_deadline_ms`. **Not built in C1 or C2** — the sweep does
  not yet evict any merge CF. Defer to the C3 retention pass.

- **`redirect_dedup` is never GC'd** (`StatefulRecord`). It grows one entry per merge-chain ancestor of
  a person; bounded by that person's merge-chain ancestry (tiny in practice — chains are short), and
  `skip_serializing_if` keeps it off the wire when empty. No time-based eviction (it would re-open the
  replay window the entry closes), mirroring `AppliedOffsets`.

- **(F12) Straggler folded pre-apply double-counts — protocol-inherent ordering hole.** The guard
  that absorbs a re-keyed straggler at the target — `redirect_dedup[P_old]` — is composed at
  **apply** time (`merge/rules.rs::compose_ancestor_dedup` folds P_old's `applied_offsets` into the
  merged record). Kafka guarantees no relative order between `cohort_stream_events` (carrying the
  re-keyed straggler) and `cohort_merge_state_transfer` at the target partition, so a straggler
  already counted in P_old's drained state (folded pre-merge, redelivered post-tombstone, re-keyed)
  can land **before** the transfer applies: it finds no `redirect_dedup[P_old]` to consult and
  folds into P_new; the apply then sums the drained state that already contains it. No local write
  ordering closes this — it would take cross-topic coordination the protocol deliberately avoids.
  Bounded to one event per leaf, and the resulting count skew sits within the same ±5 min parity
  envelope as the bucket-alignment loss (TDD §4.5.1 S6b).

- **Absent-team drain leaves prior-epoch `cf_stage2` rows (C1-inherited).** The drain derives
  `old_stage2_keys` from the catalog's composable-cohort list, so when P_old's team is absent from
  the catalog the list is empty and any `cf_stage2` rows written under a prior catalog epoch survive
  the drain — orphaned but unread (Stage 2 only reads keys derived from the live catalog), bounded
  by `delete_partition` on revoke and store retention. GC belongs with the merge-CF retention pass.

- **Drain emits no `Left` for P_old (Decision 1).** The drain silently deletes P_old's
  `cf_stage1` / `cf_person_index` / `cf_stage2` rows; the parity comparator scopes merged-away persons
  out (the benchmark harness generates the merges, so it knows them). Drain needs no `cf_stage2` reads —
  P_old's `Stage2Key`s are built from the catalog's composable-cohort list, and deleting an absent key
  is a no-op. Verify the comparator scoping holds when the harness merge scenario lands (the named
  follow-up after C2).

- **Apply dedup keys by source merge-message coordinates — a re-drain with rebuilt state is dropped
  (accepted).** `cf_merge_applied` is keyed by `MergeStateTransfer::source_partition`/`source_offset`
  (identical across every copy of one merge's transfer; the designed duplication paths — crash between
  produce ack and outbox clear, `AlreadyDrained` re-produce, redrive racing the inline retry — each
  re-produce at fresh transfer-topic coordinates, which would double-count daily/compressed buckets if
  keyed instead). Residual: a re-drain after a drain-marker wipe that captured straggler-rebuilt P_old
  state produces a same-source-coords transfer with a different payload, and the dedup drops it. Rare
  (marker wipe + stragglers in the same window), bounded by the straggler tail, comparator-scoped;
  pinned by `redrained_transfer_with_rebuilt_state_is_dropped_by_source_coords_dedup`.

## Deploy

- **`cf_stage1` format break on the L11 deploy** (`AppliedOffsets` replaced the scalar
  `last_applied_partition`/`last_applied_offset` pair). Old-shaped records fail-decode by design — no
  `#[serde(default)]` on `applied_offsets`, since a silent empty default would drop the per-source-partition
  high-water marks and re-open the double-count L11 closes (pinned by
  `old_scalar_format_fails_to_decode_rather_than_silently_defaulting`). On the rolling deploy, **wipe the
  shadow `STORE_PATH`** to avoid a one-time `stage1_state_decode_error_total` bump as the worker re-derives
  state on the next event per key. Pre-rollout, shadow-only, so the break is acceptable.
