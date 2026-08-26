package coordination_test

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/krabka-io/krabka-streams-go/coordination"
)

// Both writers of a role pin the partition from the role name, so the records
// of one role keep a total order. The rule is Kafka's own key partitioning.
func ExampleRolePartition() {
	role, err := coordination.NewRole("controller")
	if err != nil {
		log.Fatal(err)
	}

	partition, err := coordination.RolePartition(role, 16)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("role %s writes to %s-%d\n", role, coordination.StateTopic, partition)
	// Output:
	// role controller writes to __coordination_state-12
}

// The producer epoch is an int16 and it wraps. Kafka answers the exhaustion
// with a fresh producer id and an epoch of zero, so a comparison of the epoch
// alone would keep a deposed leader in place.
func ExampleFencingToken_Compare() {
	exhausted, err := coordination.NewFencingToken(4, math.MaxInt16)
	if err != nil {
		log.Fatal(err)
	}
	fresh, err := coordination.NewFencingToken(5, 0)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s supersedes %s: %t\n", fresh, exhausted, fresh.Supersedes(exhausted))
	fmt.Printf("the epoch alone would say: %t\n", fresh.ProducerEpoch() > exhausted.ProducerEpoch())
	// Output:
	// 5:0 supersedes 4:32767: true
	// the epoch alone would say: false
}

// A member writes a lease record under the epoch of its role. A reader of the
// coordination topic decodes the record with the codec of this package.
func ExampleDecodeLease() {
	member, err := coordination.NewMemberID("node-1")
	if err != nil {
		log.Fatal(err)
	}
	token, err := coordination.NewFencingToken(4242, 7)
	if err != nil {
		log.Fatal(err)
	}
	written := coordination.Lease{
		Member:    member,
		Token:     token,
		GrantedAt: 1700000000000,
		Deadline:  1700000030000,
	}

	value := coordination.EncodeLease(written)
	read, err := coordination.DecodeLease(value)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d bytes\n", len(value))
	fmt.Printf("%s holds the role with token %s until %d\n", read.Member, read.Token, read.Deadline)
	// Output:
	// 36 bytes
	// node-1 holds the role with token 4242:7 until 1700000030000
}

// The succession rules decide when a member calls InitProducerId. They never
// decide who is authoritative, because the transaction coordinator decides
// that.
func ExampleEvaluate() {
	role, err := coordination.NewRole("controller")
	if err != nil {
		log.Fatal(err)
	}
	config, err := coordination.NewLeaseConfig(30*time.Second, 10*time.Second, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	records := []coordination.StateRecord{
		registration(role, "node-1", 0, 0),
		registration(role, "node-2", 1, 0),
		registration(role, "node-3", 2, 0),
		lease(role, "node-1", 3, 0, 30000),
	}
	state, err := coordination.BuildRoleState(role, records)
	if err != nil {
		log.Fatal(err)
	}

	for _, name := range []string{"node-1", "node-2", "node-3"} {
		member, err := coordination.NewMemberID(name)
		if err != nil {
			log.Fatal(err)
		}
		rank, _ := state.RankOf(member)
		decision := coordination.Evaluate(state, member, 30000, config)
		fmt.Printf("%s rank %d: %s %d\n", name, rank, decision.Action, decision.WaitUntil)
	}
	// Output:
	// node-1 rank 0: challenge 0
	// node-2 rank 0: challenge 0
	// node-3 rank 1: wait 35000
}

// A third party checks the authority of a writer with one request. It reads
// no topic, it joins no group, and it takes no epoch.
func ExampleDescribeLeadership() {
	role, err := coordination.NewRole("controller")
	if err != nil {
		log.Fatal(err)
	}
	token, err := coordination.NewFencingToken(4242, 7)
	if err != nil {
		log.Fatal(err)
	}

	status, err := coordination.DescribeLeadership(context.Background(),
		stubCoordinator{token: token}, role)
	if err != nil {
		log.Fatal(err)
	}

	stale, err := coordination.NewFencingToken(4242, 6)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(status)
	fmt.Printf("the holder writes: %t\n", status.Authoritative(token))
	fmt.Printf("a deposed member writes: %t\n", status.Authoritative(stale))
	// Output:
	// role controller: epoch 4242:7
	// the holder writes: true
	// a deposed member writes: false
}

// stubCoordinator answers with one token. A real adapter calls
// DescribeTransactions.
type stubCoordinator struct {
	token coordination.FencingToken
}

func (s stubCoordinator) AcquireEpoch(context.Context, coordination.Role) (coordination.FencingToken, error) {
	return s.token, nil
}

func (s stubCoordinator) DescribeEpoch(context.Context, coordination.Role) (coordination.FencingToken, bool, error) {
	return s.token, true, nil
}

// registration builds the log record of one registration.
func registration(role coordination.Role, name string, offset, registeredAt int64) coordination.StateRecord {
	member, err := coordination.NewMemberID(name)
	if err != nil {
		log.Fatal(err)
	}
	return coordination.StateRecord{
		Offset: offset,
		Key:    coordination.EncodeKey(coordination.RegistrationKey(role, member)),
		Value: coordination.EncodeRegistration(coordination.Registration{
			Member:       member,
			RegisteredAt: registeredAt,
		}),
	}
}

// lease builds the log record of one lease.
func lease(role coordination.Role, name string, offset, grantedAt, deadline int64) coordination.StateRecord {
	member, err := coordination.NewMemberID(name)
	if err != nil {
		log.Fatal(err)
	}
	token, err := coordination.NewFencingToken(11, 1)
	if err != nil {
		log.Fatal(err)
	}
	return coordination.StateRecord{
		Offset: offset,
		Key:    coordination.EncodeKey(coordination.LeaseKey(role)),
		Value: coordination.EncodeLease(coordination.Lease{
			Member:    member,
			Token:     token,
			GrantedAt: grantedAt,
			Deadline:  deadline,
		}),
	}
}
