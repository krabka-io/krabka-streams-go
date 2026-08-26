# Coordination primitives

Leader election, leases, and fencing tokens for an active or standby process.
One member of a role does the work, the cluster fences every other member, and
a third party checks who is authoritative with one request.

## The epoch is the safety mechanism and the lease is not

The leadership epoch is the producer epoch that Kafka's transaction
coordinator mints for `transactional.id = <role>`. That one decision supplies
every property the package promises.

- The coordinator writes the epoch to `__transaction_state`, which is
  replicated, so the quorum mints the epoch.
- The coordinator advances the epoch on every `InitProducerId` call and reuses
  no value, so the epoch grows.
- The broker rejects a write that carries a superseded epoch, with
  `INVALID_PRODUCER_EPOCH` or `PRODUCER_FENCED`. The cluster fences a deposed
  leader, and the leader does not fence itself.
- `DescribeTransactions` reports the current producer id and producer epoch of
  a transactional id. Any process calls it.

The lease adds no safety of its own. It decides when a standby stops waiting
for a quiet holder. A wrong lease makes a failover early or late. A wrong
lease never makes two writers authoritative. A leader does not prove that its
lease is live before each write. The write carries the proof, and the broker
checks it.

## The record format

Per-role state lives in the compacted internal topic `__coordination_state`.
Every record of one role goes to one partition, so the records of a role carry
a total order. The topic holds two record kinds, and the key discriminates
them.

```text
key:
  version  i16 = 0
  kind     i16          0 registration, 1 lease
  role     string
  member   string       the member id for kind 0, the empty string for kind 1

registration value:
  version        i16 = 0
  member         string
  registeredAt   i64    epoch milliseconds

lease value:
  version         i16 = 0
  member          string
  producerID      i64
  producerEpoch   i16
  grantedAt       i64   epoch milliseconds
  deadline        i64   epoch milliseconds
```

Every integer is big-endian and signed. A string is an `int16` byte length and
then plain UTF-8 bytes, which is Kafka's own native string layout. A record
with an empty value is a tombstone. A tombstone of a registration key
deregisters one member, and a tombstone of a lease key clears the lease of one
role.

The layouts are frozen. `krabka-client-rs` and `krabka-streams-java`
re-implement them byte for byte, and all three projects assert the same golden
vectors. A change of a field, a field order, or a version number needs the
same change in the two ports.

```go
key := coordination.EncodeKey(coordination.LeaseKey(role))
value := coordination.EncodeLease(lease)

decoded, err := coordination.DecodeKey(key)
read, err := coordination.DecodeLease(value)
```

Malformed bytes return a `*coordination.FormatError`, and its `Part` field
names the record part that failed: `key`, `registration value`, or `lease
value`.

## The partition of a role

Both writers of a role pin the partition from the role name.

```go
partition, err := coordination.RolePartition(role, 16)
```

The rule is Kafka's own key partitioning: `murmur2` of the role name in UTF-8,
masked with `Utils.toPositive`, and then the remainder of the partition count.
The mask clears the sign bit. It is not an absolute value, and the two differ
for every negative hash.

The pin is a correctness requirement. The registration key of a role and the
lease key of the same role differ, because the registration key names a member
and the lease key does not. A partitioner that hashes the key would put the
two kinds in two partitions, and the total order would be gone.

## Rank comes from the log

A candidate appends a registration record. The offset of that record is the
join sequence of the candidate. Log compaction keeps the offset of every
record it retains, so a reader that walks the partition in offset order sees
the registrations in registration order.

```go
state, err := coordination.BuildRoleState(role, records)
rank, ranked := state.RankOf(member)
```

A recovered node registers again, takes a higher offset, and lands at the tail
of the roster. That is the no-failback rule, and it needs no counter and no
coordinator. A rank from a configuration file would put the recovered node at
the front, and the node would then preempt the member that replaced it.

`Evaluate` turns the state into one step.

| Action                | Meaning                                              |
| --------------------- | ---------------------------------------------------- |
| `ActionNotRegistered` | the member has no rank; it registers first           |
| `ActionHold`          | the lease names this member and the lease is live    |
| `ActionChallenge`     | the member calls `InitProducerId` now                |
| `ActionWait`          | the member waits until `Decision.WaitUntil`          |

A challenger of rank `n` challenges at the deadline plus `n` challenge
staggers. The stagger saves epoch churn, and it does nothing else.
`InitProducerId` is atomic at the coordinator, so a simultaneous challenge by
every standby still leaves exactly one member with the newest epoch.

## The fencing token is a pair

The producer epoch is an `int16` and it wraps. Kafka answers the exhaustion
with a fresh producer id and an epoch of zero. `FencingToken` holds the pair
`(ProducerID, ProducerEpoch)`, and `Compare` reads the producer id first. A
comparison of the epoch alone accepts a stale writer after about 32000
leadership changes.

`NoEpoch` is the token of a role that no member has taken. It ranks below
every minted token. It is a `FencingToken`, and it is a different value from
`columnar.NoEpoch`, which keys the snapshot a runner saves outside a barrier.

## The client

```go
leadership, err := coordination.AcquireLeadership(ctx, transport, role, member,
    coordination.WithPartitions(16),
    coordination.WithLeaseDuration(30*time.Second),
    coordination.WithRenewInterval(10*time.Second),
    coordination.WithChallengeStagger(5*time.Second))
if err != nil {
    return err
}
defer leadership.Resign(context.Background())

go func() {
    <-leadership.Done()
    log.Printf("the role ended: %v", leadership.Err())
}()

// Pass leadership.Token() to every guarded write. The broker fences a write
// of an older epoch.
```

`AcquireLeadership` registers the member, waits for its turn, mints the epoch,
and writes the first lease record. It renews the lease in the background at
every renew interval. A fence closes `Done`, and `Err` then wraps
`coordination.ErrFenced`. The caller stops the work of the role at that point.

A third party checks a writer with one request.

```go
status, err := coordination.DescribeLeadership(ctx, coordinator, role)
if !status.Authoritative(token) {
    return fmt.Errorf("the writer of %s is deposed", role)
}
```

`DescribeRoleState` reads the roster and the lease of a role for a tool that
reports the state of a cluster.

## The transport seam

This library pins no Kafka client, so the package reaches a broker through
four small interfaces. One adapter type satisfies all four.

```go
type Transport interface {
    Coordinator  // AcquireEpoch, DescribeEpoch
    StateReader  // ReadPartition
    Registrar    // Append
    LeaseWriter  // WriteLease
}
```

The adapter needs two producers for one role. The transactional producer
carries `transactional.id = <role>`, and it writes the lease records of that
role. A second, plain producer appends the registration records, because Kafka
puts every send of a transactional producer inside a transaction, and a
candidate holds no epoch to open one with.

The adapter must keep four rules.

1. `AcquireEpoch` calls `InitProducerId` for the transactional id of the role,
   and it keeps the producer that the epoch binds. `WriteLease` writes through
   that same producer.
2. `WriteLease` returns an error that wraps `coordination.ErrFenced` for
   broker error code 47 `INVALID_PRODUCER_EPOCH` and for code 90
   `PRODUCER_FENCED`. The acquire loop and the renewal loop read that error,
   and they read no other signal.
3. `ReadPartition` fetches with `read_committed`, so an aborted lease write
   stays invisible. It walks from the first offset of the partition to the
   last stable offset, and it returns the records in offset order.
4. Both producers pin the partition of the record and apply no partitioner of
   their own. `Append` and `WriteLease` take the partition as a parameter.

## Topic and cluster requirements

- `__coordination_state` needs `cleanup.policy=compact`.
- The client writes with `acks=all`. An operator should set
  `min.insync.replicas` to at least 2. The claim "durable in a quorum before
  the dispatch" rests on the topic configuration, and not on this library.
- Every member passes the real partition count of the topic with
  `WithPartitions`. Two members with two counts pick two partitions for one
  role, and the total order breaks.
- The adapter passes the lease duration as `transaction_timeout_ms` on
  `InitProducerId`. The coordinator then aborts the open transaction of a dead
  holder without a client.

## Clocks

The lease deadline is a wall-clock instant. The holder writes it, and a
challenger compares it against the clock of the challenger. Skew between the
two clocks moves the failover time by the size of the skew. Skew does not
affect safety, because no clock takes part in the fence. An operator that
needs a bound on the failover time needs a bound on the skew.

A test replaces the clock with `coordination.NewManualClock` and drives every
wait by hand.
