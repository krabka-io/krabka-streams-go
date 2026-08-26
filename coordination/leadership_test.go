package coordination

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

// fakeCluster stands in for a Kafka cluster. It holds the records of the
// coordination topic and the epoch that the transaction coordinator holds for
// every role. It fences a lease write of a superseded token, which is the one
// broker behavior that the succession rules rest on.
type fakeCluster struct {
	mu             sync.Mutex
	records        map[TopicPartition][]StateRecord
	epochs         map[Role]FencingToken
	nextProducerID int64
	leaseWrites    int
	appends        int
	reads          int
	acquireErr     error
	appendErr      error
	writeErr       error
	readErr        error
	wrote          chan struct{}
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		records: map[TopicPartition][]StateRecord{},
		epochs:  map[Role]FencingToken{},
		wrote:   make(chan struct{}, 256),
	}
}

// AcquireEpoch mints the next epoch of the role. Kafka advances the epoch of
// one producer id, and it allocates a fresh producer id with an epoch of zero
// when the epoch is exhausted.
func (c *fakeCluster) AcquireEpoch(_ context.Context, role Role) (FencingToken, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.acquireErr != nil {
		return FencingToken{}, c.acquireErr
	}
	held, found := c.epochs[role]
	if !found || held.ProducerEpoch() == math.MaxInt16 {
		c.nextProducerID++
		return c.mint(role, c.nextProducerID, 0)
	}
	return c.mint(role, held.ProducerID(), held.ProducerEpoch()+1)
}

func (c *fakeCluster) mint(role Role, producerID int64, producerEpoch int16) (FencingToken, error) {
	token, err := NewFencingToken(producerID, producerEpoch)
	if err != nil {
		return FencingToken{}, err
	}
	c.epochs[role] = token
	return token, nil
}

func (c *fakeCluster) DescribeEpoch(_ context.Context, role Role) (FencingToken, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	token, found := c.epochs[role]
	return token, found, nil
}

func (c *fakeCluster) ReadPartition(_ context.Context, partition TopicPartition) ([]StateRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	if c.readErr != nil {
		return nil, c.readErr
	}
	return append([]StateRecord{}, c.records[partition]...), nil
}

func (c *fakeCluster) Append(_ context.Context, partition TopicPartition, key, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.appends++
	if c.appendErr != nil {
		return c.appendErr
	}
	c.put(partition, key, value)
	return nil
}

func (c *fakeCluster) WriteLease(_ context.Context, partition TopicPartition, token FencingToken, key, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeErr != nil {
		return c.writeErr
	}
	decoded, err := DecodeKey(key)
	if err != nil {
		return err
	}
	if held, found := c.epochs[decoded.Role]; !found || held != token {
		return fmt.Errorf("the broker rejected epoch %s of role %s: %w",
			token, decoded.Role, ErrFenced)
	}
	c.leaseWrites++
	c.put(partition, key, value)
	select {
	case c.wrote <- struct{}{}:
	default:
	}
	return nil
}

// put appends one record at the next offset of the partition.
func (c *fakeCluster) put(partition TopicPartition, key, value []byte) {
	held := c.records[partition]
	c.records[partition] = append(held, StateRecord{
		Offset: int64(len(held)),
		Key:    key,
		Value:  value,
	})
}

// seed appends one record outside the seams, so a test builds a starting log.
func (c *fakeCluster) seed(partition TopicPartition, records ...StateRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, record := range records {
		c.put(partition, record.Key, record.Value)
	}
}

func (c *fakeCluster) counts() (leaseWrites, appends int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaseWrites, c.appends
}

func (c *fakeCluster) fail(acquire, appendTo, write, read error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acquireErr, c.appendErr, c.writeErr, c.readErr = acquire, appendTo, write, read
}

// testOptions is the fixture policy: a 30-second lease, a 10-second renew
// interval, a 5-second challenge stagger, one partition, and a manual clock.
func testOptions(t *testing.T, clock Clock) []Option {
	t.Helper()
	return []Option{
		WithLeaseConfig(testConfig(t)),
		WithPartitions(1),
		WithClock(clock),
		WithPollInterval(time.Second),
	}
}

func testPartition() TopicPartition {
	return TopicPartition{Topic: StateTopic, Partition: 0}
}

// advanceUntil moves the manual clock forward until the condition holds. It
// drives the waits of a background loop that registers after the test moved
// the clock.
func advanceUntil(t *testing.T, clock *ManualClock, step time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("the condition stayed false while the clock moved forward")
		}
		clock.Advance(step)
		time.Sleep(time.Millisecond)
	}
}

func TestAcquireLeadershipRegistersMintsAnEpochAndWritesTheLease(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(1000)
	role := mustRole(t, "controller")
	member := mustMember(t, "node-1")

	leadership, err := AcquireLeadership(t.Context(), cluster, role, member, testOptions(t, clock)...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	t.Cleanup(func() { _ = leadership.Resign(context.Background()) })

	if leadership.Role() != role || leadership.Member() != member {
		t.Errorf("the handle names role %s and member %s, want %s and %s",
			leadership.Role(), leadership.Member(), role, member)
	}
	if want := mustToken(t, 1, 0); leadership.Token() != want {
		t.Errorf("Token() = %s, and the coordinator minted %s", leadership.Token(), want)
	}
	if got := leadership.Partition(); got != testPartition() {
		t.Errorf("Partition() = %+v, want %+v", got, testPartition())
	}
	if err := leadership.Err(); err != nil {
		t.Errorf("Err() = %v, and the leadership is live", err)
	}
	select {
	case <-leadership.Done():
		t.Fatal("Done() closed, and the leadership is live")
	default:
	}

	want := Lease{Member: member, Token: mustToken(t, 1, 0), GrantedAt: 1000, Deadline: 31000}
	if got := leadership.Lease(); got != want {
		t.Errorf("Lease() = %+v, want %+v", got, want)
	}

	state, err := DescribeRoleState(t.Context(), cluster, role, testOptions(t, clock)...)
	if err != nil {
		t.Fatalf("DescribeRoleState: %v", err)
	}
	wantState := RoleState{
		Roster: []RosterEntry{{Member: member, Offset: 0, RegisteredAt: 1000}},
		Lease:  &want,
	}
	if state.Lease == nil || *state.Lease != *wantState.Lease ||
		len(state.Roster) != 1 || state.Roster[0] != wantState.Roster[0] {
		t.Errorf("the topic holds %+v, and the acquisition writes %+v", state, wantState)
	}
}

// The member of an expired lease keeps the role in the log. The first standby
// takes the role at the deadline, and it mints a newer epoch.
func TestAStandbyTakesTheRoleWhenTheLeaseExpires(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(0)
	role := mustRole(t, "controller")
	partition := testPartition()
	cluster.seed(partition,
		registrationRecord(t, 0, role, "node-1", 0),
		registrationRecord(t, 1, role, "node-2", 0),
		leaseRecord(t, 2, role, "node-1", 0, 30000),
	)

	// Before the deadline the standby waits, and the caller stops the wait.
	waiting, stop := context.WithCancel(t.Context())
	failed := make(chan error, 1)
	go func() {
		_, err := AcquireLeadership(waiting, cluster, role, mustMember(t, "node-2"),
			testOptions(t, clock)...)
		failed <- err
	}()
	select {
	case err := <-failed:
		t.Fatalf("the standby took the role while the lease was live: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	stop()
	select {
	case err := <-failed:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the standby returned %v, and the caller cancelled the wait", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the standby ignored the cancellation")
	}

	// At the deadline the standby challenges and wins.
	clock.Set(30000)
	leadership, err := AcquireLeadership(t.Context(), cluster, role, mustMember(t, "node-2"),
		testOptions(t, clock)...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	t.Cleanup(func() { _ = leadership.Resign(context.Background()) })

	want := Lease{
		Member:    mustMember(t, "node-2"),
		Token:     mustToken(t, 1, 0),
		GrantedAt: 30000,
		Deadline:  60000,
	}
	if got := leadership.Lease(); got != want {
		t.Errorf("the standby wrote %+v, want %+v", got, want)
	}
	if _, appends := cluster.counts(); appends != 0 {
		t.Errorf("the standby appended %d registrations, and the log already held its own", appends)
	}
}

// A deposed holder learns that it lost the role from the fenced write, and
// from nothing else. No clock takes part in the check.
func TestAFencedRenewalEndsTheLeadership(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(1000)
	role := mustRole(t, "controller")

	leadership, err := AcquireLeadership(t.Context(), cluster, role, mustMember(t, "node-1"),
		testOptions(t, clock)...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}

	// Another member mints a newer epoch for the role.
	newer, err := cluster.AcquireEpoch(t.Context(), role)
	if err != nil {
		t.Fatalf("AcquireEpoch: %v", err)
	}
	if !newer.Supersedes(leadership.Token()) {
		t.Fatalf("the new token %s must supersede %s", newer, leadership.Token())
	}

	err = leadership.Renew(t.Context())
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("Renew returned %v, and the broker fenced the write", err)
	}
	select {
	case <-leadership.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done() stayed open after the broker fenced the member")
	}
	if !errors.Is(leadership.Err(), ErrFenced) {
		t.Errorf("Err() = %v, and the broker fenced the member", leadership.Err())
	}
	if err := leadership.Renew(t.Context()); !errors.Is(err, ErrFenced) {
		t.Errorf("a renewal after the end returned %v, and the leadership ended with a fence", err)
	}
}

func TestTheRenewalLoopWritesARenewalAtEveryRenewInterval(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(1000)

	leadership, err := AcquireLeadership(t.Context(), cluster, mustRole(t, "controller"),
		mustMember(t, "node-1"), testOptions(t, clock)...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	t.Cleanup(func() { _ = leadership.Resign(context.Background()) })

	first := leadership.Lease()
	advanceUntil(t, clock, 10*time.Second, func() bool {
		writes, _ := cluster.counts()
		return writes >= 2
	})

	renewed := leadership.Lease()
	if renewed.GrantedAt <= first.GrantedAt {
		t.Errorf("the renewal granted at %d, and the first grant was at %d",
			renewed.GrantedAt, first.GrantedAt)
	}
	if renewed.Deadline <= first.Deadline {
		t.Errorf("the renewal ends at %d, and the first lease ended at %d",
			renewed.Deadline, first.Deadline)
	}
	if renewed.Token != leadership.Token() {
		t.Errorf("the renewal carries token %s, and the member holds %s",
			renewed.Token, leadership.Token())
	}
	if err := leadership.Err(); err != nil {
		t.Errorf("Err() = %v, and the renewals succeeded", err)
	}
}

func TestResignClearsTheLeaseAndEndsTheLeadership(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(1000)
	role := mustRole(t, "controller")
	options := testOptions(t, clock)

	leadership, err := AcquireLeadership(t.Context(), cluster, role, mustMember(t, "node-1"), options...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}

	if err := leadership.Resign(t.Context()); err != nil {
		t.Fatalf("Resign: %v", err)
	}
	select {
	case <-leadership.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done() stayed open after the member resigned")
	}
	if !errors.Is(leadership.Err(), ErrNotHeld) {
		t.Errorf("Err() = %v, and the member resigned", leadership.Err())
	}
	if err := leadership.Resign(t.Context()); err != nil {
		t.Errorf("a second Resign returned %v, and it must do nothing", err)
	}

	state, err := DescribeRoleState(t.Context(), cluster, role, options...)
	if err != nil {
		t.Fatalf("DescribeRoleState: %v", err)
	}
	if state.Lease != nil {
		t.Errorf("the topic holds lease %+v, and the tombstone must clear it", state.Lease)
	}
	if len(state.Roster) != 1 {
		t.Errorf("the roster holds %d members, and a resignation keeps the registration",
			len(state.Roster))
	}
}

// A resignation clears the lease, so the first standby stops waiting for the
// deadline that the holder wrote. It challenges at its own stagger instead.
func TestAStandbyDoesNotWaitForTheOldDeadlineAfterAResignation(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(1000)
	role := mustRole(t, "controller")
	options := testOptions(t, clock)
	cluster.seed(testPartition(),
		registrationRecord(t, 0, role, "node-1", 0),
		registrationRecord(t, 1, role, "node-2", 0),
	)

	holder, err := AcquireLeadership(t.Context(), cluster, role, mustMember(t, "node-1"), options...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	deadline := holder.Lease().Deadline
	if err := holder.Resign(t.Context()); err != nil {
		t.Fatalf("Resign: %v", err)
	}

	// node-2 is rank 1 behind node-1 in the roster, so it challenges one
	// stagger after its own registration instant of 0.
	clock.Set(5000)
	standby, err := AcquireLeadership(t.Context(), cluster, role, mustMember(t, "node-2"), options...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	t.Cleanup(func() { _ = standby.Resign(context.Background()) })

	if clock.NowMillis() >= deadline {
		t.Fatalf("the standby took the role at %d, and the resigned holder wrote deadline %d",
			clock.NowMillis(), deadline)
	}
	if standby.Lease().Member != mustMember(t, "node-2") {
		t.Errorf("the lease names %s, and node-2 took the role", standby.Lease().Member)
	}
	if !standby.Token().Supersedes(holder.Token()) {
		t.Errorf("the standby holds %s, and it must supersede %s", standby.Token(), holder.Token())
	}
}

// The acquire loop reads the partition again when the broker fences the first
// lease write. Another member took the role between the read and the write.
func TestTheAcquireLoopReadsAgainWhenTheBrokerFencesTheLeaseWrite(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(1000)
	role := mustRole(t, "controller")
	fencing := &fencingOnce{Transport: cluster, role: role}

	leadership, err := AcquireLeadership(t.Context(), fencing, role, mustMember(t, "node-1"),
		testOptions(t, clock)...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	t.Cleanup(func() { _ = leadership.Resign(context.Background()) })

	if fencing.fenced != 1 {
		t.Errorf("the transport fenced %d writes, and the fixture fences one", fencing.fenced)
	}
	if want := mustToken(t, 1, 1); leadership.Token() != want {
		t.Errorf("Token() = %s, and the second challenge minted %s", leadership.Token(), want)
	}
}

// fencingOnce fences the first lease write and then passes every write on.
type fencingOnce struct {
	Transport
	role   Role
	fenced int
}

func (f *fencingOnce) WriteLease(ctx context.Context, partition TopicPartition, token FencingToken, key, value []byte) error {
	if f.fenced == 0 {
		f.fenced++
		return fmt.Errorf("the broker rejected epoch %s: %w", token, ErrFenced)
	}
	return f.Transport.WriteLease(ctx, partition, token, key, value)
}

func TestDescribeLeadershipReportsTheTokenOfTheCoordinator(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(1000)
	role := mustRole(t, "controller")

	status, err := DescribeLeadership(t.Context(), cluster, role)
	if err != nil {
		t.Fatalf("DescribeLeadership: %v", err)
	}
	if status.Held || status.Token != NoEpoch {
		t.Errorf("the status is %+v, and no member has taken the role", status)
	}
	if status.Authoritative(mustToken(t, 1, 0)) {
		t.Error("an untaken role authorizes no token")
	}
	if got, want := status.String(), "role controller: no epoch"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	leadership, err := AcquireLeadership(t.Context(), cluster, role, mustMember(t, "node-1"),
		testOptions(t, clock)...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	t.Cleanup(func() { _ = leadership.Resign(context.Background()) })

	status, err = DescribeLeadership(t.Context(), cluster, role)
	if err != nil {
		t.Fatalf("DescribeLeadership: %v", err)
	}
	want := LeadershipStatus{Role: role, Token: leadership.Token(), Held: true}
	if status != want {
		t.Errorf("the status is %+v, want %+v", status, want)
	}
	if !status.Authoritative(leadership.Token()) {
		t.Error("the status must authorize the token of the holder")
	}
	if status.Authoritative(mustToken(t, 99, 0)) {
		t.Error("the status must reject a token that the coordinator does not hold")
	}
	if got, want := status.String(), "role controller: epoch 1:0"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestAcquireLeadershipReportsAFailureOfEverySeam(t *testing.T) {
	role := mustRole(t, "controller")
	member := mustMember(t, "node-1")
	broken := errors.New("the broker is unreachable")
	cases := []struct {
		name     string
		acquire  error
		appendTo error
		write    error
		read     error
	}{
		{name: "a failed read", read: broken},
		{name: "a failed registration", appendTo: broken},
		{name: "a failed epoch", acquire: broken},
		{name: "a failed lease write", write: broken},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cluster := newFakeCluster()
			cluster.fail(testCase.acquire, testCase.appendTo, testCase.write, testCase.read)
			clock := NewManualClock(1000)

			_, err := AcquireLeadership(t.Context(), cluster, role, member, testOptions(t, clock)...)
			if !errors.Is(err, broken) {
				t.Fatalf("AcquireLeadership returned %v, and the seam returned %v", err, broken)
			}
		})
	}
}

func TestAcquireLeadershipRejectsAnIncompleteRequest(t *testing.T) {
	cluster := newFakeCluster()
	role := mustRole(t, "controller")
	member := mustMember(t, "node-1")
	cases := []struct {
		name      string
		transport Transport
		role      Role
		member    MemberID
		options   []Option
	}{
		{name: "no transport", transport: nil, role: role, member: member},
		{name: "no role", transport: cluster, role: Role{}, member: member},
		{name: "no member id", transport: cluster, role: role, member: MemberID{}},
		{
			name:      "a partition count below one",
			transport: cluster,
			role:      role,
			member:    member,
			options:   []Option{WithPartitions(0)},
		},
		{
			name:      "an empty topic",
			transport: cluster,
			role:      role,
			member:    member,
			options:   []Option{WithTopic("")},
		},
		{
			name:      "a renew interval over the lease duration",
			transport: cluster,
			role:      role,
			member:    member,
			options:   []Option{WithRenewInterval(time.Hour)},
		},
		{
			name:      "a lease duration under the renew interval",
			transport: cluster,
			role:      role,
			member:    member,
			options:   []Option{WithLeaseDuration(time.Second)},
		},
		{
			name:      "a zero challenge stagger",
			transport: cluster,
			role:      role,
			member:    member,
			options:   []Option{WithChallengeStagger(0)},
		},
		{
			name:      "a lease config that no constructor built",
			transport: cluster,
			role:      role,
			member:    member,
			options:   []Option{WithLeaseConfig(LeaseConfig{})},
		},
		{
			name:      "no clock",
			transport: cluster,
			role:      role,
			member:    member,
			options:   []Option{WithClock(nil)},
		},
		{
			name:      "a poll interval at zero",
			transport: cluster,
			role:      role,
			member:    member,
			options:   []Option{WithPollInterval(0)},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := AcquireLeadership(t.Context(), testCase.transport, testCase.role,
				testCase.member, testCase.options...)
			if err == nil {
				t.Fatal("AcquireLeadership took an incomplete request")
			}
		})
	}
}

func TestDescribeLeadershipAndDescribeRoleStateNeedTheirSeam(t *testing.T) {
	if _, err := DescribeLeadership(t.Context(), nil, mustRole(t, "controller")); err == nil {
		t.Error("DescribeLeadership took a nil coordinator")
	}
	if _, err := DescribeRoleState(t.Context(), nil, mustRole(t, "controller")); err == nil {
		t.Error("DescribeRoleState took a nil reader")
	}
}

func TestRoleTopicPartitionPinsTheRoleToOnePartition(t *testing.T) {
	role := mustRole(t, "controller")

	got, err := RoleTopicPartition(role)
	if err != nil {
		t.Fatalf("RoleTopicPartition: %v", err)
	}
	if want := (TopicPartition{Topic: StateTopic, Partition: 12}); got != want {
		t.Errorf("RoleTopicPartition(%s) = %+v, want %+v", role, got, want)
	}

	got, err = RoleTopicPartition(role, WithTopic("coordination"), WithPartitions(1))
	if err != nil {
		t.Fatalf("RoleTopicPartition: %v", err)
	}
	if want := (TopicPartition{Topic: "coordination", Partition: 0}); got != want {
		t.Errorf("RoleTopicPartition(%s) = %+v, want %+v", role, got, want)
	}
	if _, err := RoleTopicPartition(role, WithPartitions(0)); err == nil {
		t.Error("RoleTopicPartition took a partition count of zero")
	}
}

// A role state that names this member takes the same path as a challenge. The
// record came from an earlier incarnation of the member, and this call holds
// no epoch, so the member mints a new one and fences that incarnation.
func TestAMemberThatTheLogAlreadyNamesMintsANewEpoch(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(5000)
	role := mustRole(t, "controller")
	member := mustMember(t, "node-1")
	cluster.seed(testPartition(),
		registrationRecord(t, 0, role, "node-1", 0),
		leaseRecord(t, 1, role, "node-1", 0, 30000),
	)

	leadership, err := AcquireLeadership(t.Context(), cluster, role, member, testOptions(t, clock)...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	t.Cleanup(func() { _ = leadership.Resign(context.Background()) })

	if want := mustToken(t, 1, 0); leadership.Token() != want {
		t.Errorf("Token() = %s, and the coordinator minted %s", leadership.Token(), want)
	}
	want := Lease{Member: member, Token: mustToken(t, 1, 0), GrantedAt: 5000, Deadline: 35000}
	if got := leadership.Lease(); got != want {
		t.Errorf("Lease() = %+v, want %+v", got, want)
	}
	if _, appends := cluster.counts(); appends != 0 {
		t.Errorf("the member appended %d registrations, and the log already held its own", appends)
	}
}

// A renewal that fails for a reason other than a fence is retried. The
// leadership ends when the deadline of the lease passes with no successful
// renewal, because the standbys challenge from that instant on.
func TestTheLeadershipEndsWhenTheDeadlinePassesWithNoRenewal(t *testing.T) {
	cluster := newFakeCluster()
	clock := NewManualClock(1000)
	broken := errors.New("the broker is unreachable")

	leadership, err := AcquireLeadership(t.Context(), cluster, mustRole(t, "controller"),
		mustMember(t, "node-1"), testOptions(t, clock)...)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	deadline := leadership.Lease().Deadline
	cluster.fail(nil, nil, broken, nil)

	advanceUntil(t, clock, 5*time.Second, func() bool { return leadership.Err() != nil })

	select {
	case <-leadership.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done() stayed open after the lease expired")
	}
	if !errors.Is(leadership.Err(), broken) {
		t.Errorf("Err() = %v, and the renewal failed with %v", leadership.Err(), broken)
	}
	if clock.NowMillis() < deadline {
		t.Errorf("the leadership ended at %d, and the lease ran to %d", clock.NowMillis(), deadline)
	}
	if err := leadership.Renew(t.Context()); !errors.Is(err, broken) {
		t.Errorf("a renewal after the end returned %v, want the recorded reason", err)
	}
}

// The acquire loop registers exactly once. A read that has not caught up with
// the append sends it back to the topic instead of to a second registration,
// because a second registration would move the member to the tail.
func TestTheAcquireLoopRegistersOnceWhenTheReadLagsBehindTheAppend(t *testing.T) {
	cluster := newFakeCluster()
	lagging := &laggingReader{Transport: cluster}

	leadership, err := AcquireLeadership(t.Context(), lagging, mustRole(t, "controller"),
		mustMember(t, "node-1"),
		WithLeaseConfig(testConfig(t)), WithPartitions(1), WithPollInterval(time.Millisecond))
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	t.Cleanup(func() { _ = leadership.Resign(context.Background()) })

	if _, appends := cluster.counts(); appends != 1 {
		t.Errorf("the loop appended %d registrations, and one registration is enough", appends)
	}
}

// laggingReader hides the records of the topic on the first read after an
// append, which is what a follower fetch does before it catches up.
type laggingReader struct {
	Transport
	mu       sync.Mutex
	appended bool
	lagged   bool
}

func (l *laggingReader) Append(ctx context.Context, partition TopicPartition, key, value []byte) error {
	l.mu.Lock()
	l.appended = true
	l.mu.Unlock()
	return l.Transport.Append(ctx, partition, key, value)
}

func (l *laggingReader) ReadPartition(ctx context.Context, partition TopicPartition) ([]StateRecord, error) {
	records, err := l.Transport.ReadPartition(ctx, partition)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.appended && !l.lagged {
		l.lagged = true
		return nil, nil
	}
	return records, nil
}
