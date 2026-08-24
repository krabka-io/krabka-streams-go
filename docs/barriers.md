# Barrier cuts

A krabka broker puts an epoch-stamped marker into every partition of a named
barrier group. The offset of the marker of epoch N in a partition is that
partition's point of cut N. A cut is an exact point in every input at once,
so two replays of cut N read the same records.

The marker itself is a Kafka control record. No consumer sees one, and this
library never looks for one. The broker publishes each cut as a manifest
record on the internal topic `__barrier_state`, and this library reads that
topic with a plain assign, seek, and poll loop.

## The cut reader

```go
reader, err := columnar.NewCutReader(barrierConsumer, partitionCount)

latest, err := reader.LatestCompleteCut(ctx, "audit")
newer, err := reader.CompleteCutsAfter(ctx, "audit", latest.Epoch)
```

`NewCutReader` takes the partition count of `__barrier_state`. The reader
assigns those partitions, seeks each one to the start, and polls to the end.
It keeps every cut it decoded, so a later call polls only what arrived since
the previous one.

Give the reader a consumer of its own. `Assign` replaces the assignment of
the consumer it drives, which would revoke the partitions of a subscribed
group runner.

A partial cut is never alignable, and the reader never returns one. The
partitions in `BarrierCut.Missing` receive no marker for that epoch, so a
task that waits for one waits forever. The broker publishes a partial cut for
exactly this reason: it lets every reader skip the epoch.

`DecodeBarrierCut` decodes one key and value of the topic on its own, for a
tool that reads `__barrier_state` outside a runner. It returns a nil cut for
a group record, an injection-start record, and a tombstone. Malformed bytes
return a `*columnar.BarrierFormatError`.

## Alignment

```go
runner, err := columnar.NewGroupRunner(topology, consumer, producer,
    columnar.WithStateStore(columnar.NewFileStateStore(dir)),
    columnar.WithBarrierGroup("audit", reader),
    columnar.WithBarrierListener(func(barrier columnar.Barrier) {
        log.Printf("epoch %d at %v", barrier.Cut.Epoch, barrier.Offsets)
    }))
```

With a barrier group the runner takes the oldest complete cut above the
epoch it last committed. It then truncates each partition's fetched records
at that partition's cut offset. Records below the offset run in the round;
the rest wait in memory for the next round. The marker takes the cut offset
itself and reaches no consumer, so "everything before the cut" is exactly
"offset below the cut offset".

The barrier fires once every aligned partition reaches its cut offset. The
aligned partitions are the ones the runner owns, or every partition of the
cut when the runner owns none yet. A rebalance that takes a partition away
drops it from the pending cut, so a lost partition never holds a barrier
open.

## The snapshot

At the barrier the runner snapshots the state of each partition with
`BuiltTopology.SnapshotPartition`, saves it under the epoch of the cut, and
then commits. The committed position of each aligned partition is the cut, so
the saved state and the committed offsets name the same point.

`StateStore` keys every snapshot by the partition and the epoch:

```go
type StateStore interface {
    Load(partition int, epoch int64) (map[string][]byte, error)
    Save(partition int, epoch int64, snapshot map[string][]byte) error
}
```

A rebalance saves and loads under `columnar.NoEpoch`, which keeps the
snapshot of a revoked partition apart from the barrier snapshots.

The bytes inside a snapshot are the container `FileStateStore` always wrote:
a big-endian version of 1, a count, and then length-prefixed name and value
pairs in ascending byte order of the name. The broker, `krabka-streams-rs`,
and `krabka-streams-java` share that container. Only the storage key carries
the epoch.

## Restore

```go
cut, err := runner.RestoreToEpoch(ctx, 42)
cut, err := runner.RestoreToLatestCut(ctx)
```

Both entry points load the snapshot of that epoch for every partition the
runner acts on, restore the processors from it, and seek every input
partition of the cut to its marker offset. The runner then continues from an
exact, reproducible point.

## Transactions

`RunOnceTransactional` sends the produced records, the consumed offsets, and
the barrier snapshot in one producer transaction. The snapshot is written
before the offsets go to the transaction, so the cut and the transaction
boundary are the same point. A failed round aborts the transaction and rolls
the partition state back, and the epoch stays open for the next round.

## Limits

- A barrier holds a partition until the runner knows where that partition is.
  The `Consumer` seam has no position query, so the runner takes the position
  from the records it read and from the cuts it restored to. A partition that
  delivered nothing since the runner started holds the barrier open.
- The records after a cut wait in memory until the barrier fires. A slow
  partition in a barrier group costs memory in every fast one.
- A cut whose offsets are behind the runner's committed position does not
  rewind the commit. The runner commits the higher of the two.
- While no cut is pending, the runner reads the barrier state topic once per
  round, which costs one poll of the reader's consumer.
  `WithCutPollTimeout` sets what that poll costs. A cut the reader misses
  arrives at the next round.
