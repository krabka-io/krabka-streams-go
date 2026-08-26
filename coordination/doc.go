// Package coordination elects one leader per role, and it proves that
// leadership on every guarded write.
//
// A control plane that dispatches commands to external devices needs two
// properties from its log. A command is durable in a quorum before the
// dispatch, and a deposed leader cannot dispatch at all. The second property
// is the hard one. A leader that loses its lease does not know that it lost
// the lease, so it cannot fence itself. Something outside the leader must
// refuse its writes.
//
// # The epoch is the safety mechanism and the lease is not
//
// The leadership epoch is the producer epoch that Kafka's transaction
// coordinator mints for the transactional id of a role. That one decision
// supplies every property this package promises:
//
//   - The coordinator writes the epoch to __transaction_state, which is
//     replicated, so the quorum mints the epoch.
//   - The coordinator advances the epoch on every InitProducerId call and
//     reuses no value, so the epoch grows.
//   - The broker rejects a write that carries a superseded epoch. It answers
//     INVALID_PRODUCER_EPOCH or PRODUCER_FENCED. The cluster fences a deposed
//     leader, and the leader does not fence itself.
//   - DescribeTransactions reports the current producer id and producer epoch
//     of a transactional id. Any process calls it. A third party checks the
//     authority of a writer with one request and no membership. See
//     [DescribeLeadership].
//
// The lease adds no safety of its own. It is a liveness and anti-flap device.
// It tells a standby when to stop waiting for a quiet holder. A wrong lease
// makes a failover early or late. A wrong lease never makes two writers
// authoritative. Read that inversion against a hand-rolled design, where the
// lease deadline decides who may write. This package never asks the lease
// that question.
//
// # The fencing token is a pair
//
// The producer epoch is an int16 and it wraps. Kafka handles the exhaustion
// with a fresh producer id and an epoch of zero. An implementation that
// compares the epoch alone accepts a stale writer after about 32000
// leadership changes. [FencingToken] holds the pair, and
// [FencingToken.Compare] reads the producer id first. See [NoEpoch] for the
// token of a role that no member has taken.
//
// # Rank comes from the log
//
// A candidate appends a registration record to [StateTopic]. The offset of
// that record in the partition is the join sequence of the candidate. Log
// compaction keeps the offset of every record it retains, so a reader that
// walks the partition in offset order sees the registrations in registration
// order. [BuildRoleState] folds that walk into a [RoleState], and the roster
// it builds is in offset order.
//
// A recovered node registers again. The new record takes a higher offset, so
// the node lands at the tail of the roster and takes the last rank. That is
// the no-failback rule, and it needs no counter and no coordinator. A rank
// from a configuration file would put the recovered node at the front, and
// the node would then preempt the member that replaced it. The cluster would
// pay for a second failover that it does not need.
//
// # The stagger is an optimisation
//
// A challenger of rank n waits n challenge staggers past the deadline of the
// lease. This saves epoch churn, and it does nothing else. InitProducerId is
// atomic at the coordinator, so a simultaneous challenge by every standby
// still leaves exactly one member with the newest epoch. The losers keep an
// older epoch, and each one learns that it lost on its first guarded write.
// Set the stagger to make that outcome rare. Do not set it to make the
// outcome safe, because the outcome is already safe.
//
// # This package pins no Kafka client
//
// The library pins no Kafka client, so this package reaches a broker through
// the small interfaces in [Transport]. An adapter over franz-go or
// confluent-kafka-go is a few dozen lines of glue, and it is the same
// boundary that the columnar group runner draws. Everything else is here: the
// record codec, the roster and the rank logic, the challenge timing, the
// lease clock, the renewal loop, [AcquireLeadership], and
// [DescribeLeadership].
//
// # Clock assumptions
//
// The lease deadline is a wall-clock instant. The holder writes it, and a
// challenger compares it against the clock of the challenger. Skew between
// the two clocks moves the failover time by the size of the skew. Skew does
// not affect safety, for the reason the first section gives. An operator that
// needs a bound on the failover time needs a bound on the skew.
package coordination
