package coordination

import (
	"reflect"
	"testing"
	"time"
)

// testConfig is the lease policy of the succession fixtures: a 30-second
// lease, a 10-second renew interval, and a 5-second challenge stagger.
func testConfig(t *testing.T) LeaseConfig {
	t.Helper()
	config, err := NewLeaseConfig(30*time.Second, 10*time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("NewLeaseConfig: %v", err)
	}
	return config
}

// registrationRecord builds the log record of one registration.
func registrationRecord(t *testing.T, offset int64, role Role, name string, registeredAt int64) StateRecord {
	t.Helper()
	member := mustMember(t, name)
	return StateRecord{
		Offset: offset,
		Key:    EncodeKey(RegistrationKey(role, member)),
		Value:  EncodeRegistration(Registration{Member: member, RegisteredAt: registeredAt}),
	}
}

// deregistrationRecord builds the tombstone that removes one member.
func deregistrationRecord(t *testing.T, offset int64, role Role, name string) StateRecord {
	t.Helper()
	return StateRecord{
		Offset: offset,
		Key:    EncodeKey(RegistrationKey(role, mustMember(t, name))),
	}
}

// leaseRecord builds the log record of one lease.
func leaseRecord(t *testing.T, offset int64, role Role, name string, grantedAt, deadline int64) StateRecord {
	t.Helper()
	return StateRecord{
		Offset: offset,
		Key:    EncodeKey(LeaseKey(role)),
		Value: EncodeLease(Lease{
			Member:    mustMember(t, name),
			Token:     mustToken(t, 11, 1),
			GrantedAt: grantedAt,
			Deadline:  deadline,
		}),
	}
}

// leaseTombstone builds the tombstone that clears the lease of a role.
func leaseTombstone(t *testing.T, offset int64, role Role) StateRecord {
	t.Helper()
	return StateRecord{Offset: offset, Key: EncodeKey(LeaseKey(role))}
}

func rosterEntry(t *testing.T, name string, offset, registeredAt int64) RosterEntry {
	t.Helper()
	return RosterEntry{Member: mustMember(t, name), Offset: offset, RegisteredAt: registeredAt}
}

func liveLease(t *testing.T, name string) *Lease {
	t.Helper()
	return &Lease{
		Member:    mustMember(t, name),
		Token:     mustToken(t, 11, 1),
		GrantedAt: 0,
		Deadline:  30000,
	}
}

func buildState(t *testing.T, role Role, records []StateRecord) RoleState {
	t.Helper()
	state, err := BuildRoleState(role, records)
	if err != nil {
		t.Fatalf("BuildRoleState: %v", err)
	}
	return state
}

func TestTheRosterFollowsOffsetOrderAndNotArrivalOrder(t *testing.T) {
	role := mustRole(t, "controller")
	records := []StateRecord{
		registrationRecord(t, 30, role, "node-3", 300),
		registrationRecord(t, 10, role, "node-1", 100),
		registrationRecord(t, 20, role, "node-2", 200),
	}

	state := buildState(t, role, records)

	want := RoleState{Roster: []RosterEntry{
		rosterEntry(t, "node-1", 10, 100),
		rosterEntry(t, "node-2", 20, 200),
		rosterEntry(t, "node-3", 30, 300),
	}}
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("the fold returns %+v, and the offset order is %+v", state, want)
	}
}

// A recovered node registers again, takes a higher offset, and lands at the
// tail of the roster. That is the no-failback rule.
func TestARecoveredMemberReentersAtTheTail(t *testing.T) {
	role := mustRole(t, "controller")
	records := []StateRecord{
		registrationRecord(t, 10, role, "node-1", 100),
		registrationRecord(t, 20, role, "node-2", 200),
		registrationRecord(t, 30, role, "node-3", 300),
		registrationRecord(t, 40, role, "node-1", 400),
	}

	state := buildState(t, role, records)

	want := RoleState{Roster: []RosterEntry{
		rosterEntry(t, "node-2", 20, 200),
		rosterEntry(t, "node-3", 30, 300),
		rosterEntry(t, "node-1", 40, 400),
	}}
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("the fold returns %+v, and the tail order is %+v", state, want)
	}
	rank, ranked := state.RankOf(mustMember(t, "node-1"))
	if !ranked || rank != 2 {
		t.Errorf("the recovered member takes rank %d (found %t), and the tail rank is 2",
			rank, ranked)
	}
}

func TestATombstoneRemovesAMemberAndALaterRecordBringsItBack(t *testing.T) {
	role := mustRole(t, "controller")
	cases := []struct {
		name    string
		records []StateRecord
		want    RoleState
	}{
		{
			name: "a tombstone removes a member",
			records: []StateRecord{
				registrationRecord(t, 10, role, "node-1", 100),
				registrationRecord(t, 20, role, "node-2", 200),
				deregistrationRecord(t, 30, role, "node-1"),
			},
			want: RoleState{Roster: []RosterEntry{rosterEntry(t, "node-2", 20, 200)}},
		},
		{
			name: "a registration after a tombstone rejoins at the tail",
			records: []StateRecord{
				registrationRecord(t, 10, role, "node-1", 100),
				registrationRecord(t, 20, role, "node-2", 200),
				deregistrationRecord(t, 30, role, "node-1"),
				registrationRecord(t, 40, role, "node-1", 400),
			},
			want: RoleState{Roster: []RosterEntry{
				rosterEntry(t, "node-2", 20, 200),
				rosterEntry(t, "node-1", 40, 400),
			}},
		},
		{
			name: "a tombstone of a lower offset revives no member",
			records: []StateRecord{
				registrationRecord(t, 10, role, "node-1", 100),
				registrationRecord(t, 40, role, "node-1", 400),
				deregistrationRecord(t, 20, role, "node-1"),
			},
			want: RoleState{Roster: []RosterEntry{rosterEntry(t, "node-1", 40, 400)}},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			state := buildState(t, role, testCase.records)
			if !reflect.DeepEqual(state, testCase.want) {
				t.Fatalf("the fold returns %+v, and the rule gives %+v", state, testCase.want)
			}
		})
	}
}

func TestTheLeaseOfTheHighestOffsetWinsAndATombstoneClearsIt(t *testing.T) {
	role := mustRole(t, "controller")
	later := &Lease{
		Member:    mustMember(t, "node-2"),
		Token:     mustToken(t, 11, 1),
		GrantedAt: 10000,
		Deadline:  40000,
	}
	cases := []struct {
		name    string
		records []StateRecord
		want    *Lease
	}{
		{
			name: "the last lease wins",
			records: []StateRecord{
				leaseRecord(t, 10, role, "node-1", 0, 30000),
				leaseRecord(t, 20, role, "node-2", 10000, 40000),
			},
			want: later,
		},
		{
			name: "a tombstone clears the lease",
			records: []StateRecord{
				leaseRecord(t, 10, role, "node-1", 0, 30000),
				leaseTombstone(t, 20, role),
			},
			want: nil,
		},
		{
			name: "a lease of a lower offset overwrites no later one",
			records: []StateRecord{
				leaseRecord(t, 20, role, "node-2", 10000, 40000),
				leaseRecord(t, 10, role, "node-1", 0, 30000),
			},
			want: later,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			state := buildState(t, role, testCase.records)
			if !reflect.DeepEqual(state.Lease, testCase.want) {
				t.Fatalf("the fold returns lease %+v, and the rule gives %+v",
					state.Lease, testCase.want)
			}
		})
	}
}

func TestTheBuilderDropsTheRecordsOfAnotherRole(t *testing.T) {
	role := mustRole(t, "controller")
	other := mustRole(t, "compactor")
	records := []StateRecord{
		registrationRecord(t, 10, role, "node-1", 100),
		registrationRecord(t, 20, other, "node-9", 200),
		leaseRecord(t, 30, other, "node-9", 200, 40000),
	}

	builder := NewRoleStateBuilder(role)
	if builder.Role() != role {
		t.Errorf("the builder collects role %s, and the caller asked for %s", builder.Role(), role)
	}
	for _, record := range records {
		if err := builder.Apply(record); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	want := RoleState{Roster: []RosterEntry{rosterEntry(t, "node-1", 10, 100)}}
	if state := builder.Build(); !reflect.DeepEqual(state, want) {
		t.Fatalf("the fold returns %+v, and the records of the role give %+v", state, want)
	}
}

func TestTheBuilderReportsARecordThatDoesNotDecode(t *testing.T) {
	role := mustRole(t, "controller")
	cases := []struct {
		name   string
		record StateRecord
	}{
		{name: "a malformed key", record: StateRecord{Offset: 3, Key: []byte{0x00}}},
		{
			name: "a malformed registration value",
			record: StateRecord{
				Offset: 4,
				Key:    EncodeKey(RegistrationKey(role, mustMember(t, "node-1"))),
				Value:  []byte{0x00, 0x00, 0x00},
			},
		},
		{
			name: "a malformed lease value",
			record: StateRecord{
				Offset: 5,
				Key:    EncodeKey(LeaseKey(role)),
				Value:  []byte{0x00, 0x00, 0x00},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := BuildRoleState(role, []StateRecord{testCase.record}); err == nil {
				t.Fatalf("the fold took the malformed record %+v", testCase.record)
			}
		})
	}
}

func TestTheHolderLeavesTheRankOrderAndKeepsRankZero(t *testing.T) {
	state := RoleState{
		Roster: []RosterEntry{
			rosterEntry(t, "node-1", 10, 100),
			rosterEntry(t, "node-2", 20, 200),
			rosterEntry(t, "node-3", 30, 300),
		},
		Lease: liveLease(t, "node-2"),
	}

	holder, held := state.Holder()
	if !held || holder != mustMember(t, "node-2") {
		t.Fatalf("the state names holder %s (found %t), and the lease names node-2", holder, held)
	}
	cases := []struct {
		member string
		rank   int
		ranked bool
	}{
		{member: "node-2", rank: 0, ranked: true},
		{member: "node-1", rank: 0, ranked: true},
		{member: "node-3", rank: 1, ranked: true},
		{member: "node-9", rank: 0, ranked: false},
	}

	for _, testCase := range cases {
		rank, ranked := state.RankOf(mustMember(t, testCase.member))
		if rank != testCase.rank || ranked != testCase.ranked {
			t.Errorf("RankOf(%s) = (%d, %t), want (%d, %t)",
				testCase.member, rank, ranked, testCase.rank, testCase.ranked)
		}
	}
}

func TestTheLiveHolderHoldsAndEveryStandbyWaitsItsStagger(t *testing.T) {
	config := testConfig(t)
	state := RoleState{
		Roster: []RosterEntry{
			rosterEntry(t, "node-1", 10, 100),
			rosterEntry(t, "node-2", 20, 200),
			rosterEntry(t, "node-3", 30, 300),
		},
		Lease: liveLease(t, "node-1"),
	}
	cases := []struct {
		member string
		want   Decision
	}{
		{member: "node-1", want: Decision{Action: ActionHold}},
		{member: "node-2", want: Decision{Action: ActionWait, WaitUntil: 30000}},
		{member: "node-3", want: Decision{Action: ActionWait, WaitUntil: 35000}},
	}

	for _, testCase := range cases {
		got := Evaluate(state, mustMember(t, testCase.member), 1000, config)
		if got != testCase.want {
			t.Errorf("Evaluate(%s) = %+v, want %+v", testCase.member, got, testCase.want)
		}
	}
}

func TestTheRanksChallengeOneStaggerApartAfterTheDeadline(t *testing.T) {
	config := testConfig(t)
	state := RoleState{
		Roster: []RosterEntry{
			rosterEntry(t, "node-1", 10, 100),
			rosterEntry(t, "node-2", 20, 200),
			rosterEntry(t, "node-3", 30, 300),
		},
		Lease: liveLease(t, "node-1"),
	}
	// The deadline is 30000. Rank 0 is node-2 and rank 1 is node-3.
	cases := []struct {
		nowMillis int64
		member    string
		want      Decision
	}{
		{nowMillis: 29999, member: "node-2", want: Decision{Action: ActionWait, WaitUntil: 30000}},
		{nowMillis: 30000, member: "node-2", want: Decision{Action: ActionChallenge}},
		{nowMillis: 30000, member: "node-3", want: Decision{Action: ActionWait, WaitUntil: 35000}},
		{nowMillis: 34999, member: "node-3", want: Decision{Action: ActionWait, WaitUntil: 35000}},
		{nowMillis: 35000, member: "node-3", want: Decision{Action: ActionChallenge}},
		// The deposed holder keeps rank 0 and reclaims at its own deadline.
		{nowMillis: 29999, member: "node-1", want: Decision{Action: ActionHold}},
		{nowMillis: 30000, member: "node-1", want: Decision{Action: ActionChallenge}},
	}

	for _, testCase := range cases {
		got := Evaluate(state, mustMember(t, testCase.member), testCase.nowMillis, config)
		if got != testCase.want {
			t.Errorf("Evaluate(%s) at %d = %+v, want %+v",
				testCase.member, testCase.nowMillis, got, testCase.want)
		}
	}
}

func TestWithNoLeaseTheRegistrationInstantAnchorsTheStagger(t *testing.T) {
	config := testConfig(t)
	state := RoleState{Roster: []RosterEntry{
		rosterEntry(t, "node-1", 10, 1000),
		rosterEntry(t, "node-2", 20, 2000),
		rosterEntry(t, "node-3", 30, 3000),
	}}
	cases := []struct {
		nowMillis int64
		member    string
		want      Decision
	}{
		{nowMillis: 1000, member: "node-1", want: Decision{Action: ActionChallenge}},
		{nowMillis: 1000, member: "node-2", want: Decision{Action: ActionWait, WaitUntil: 7000}},
		{nowMillis: 7000, member: "node-2", want: Decision{Action: ActionChallenge}},
		{nowMillis: 1000, member: "node-3", want: Decision{Action: ActionWait, WaitUntil: 13000}},
		{nowMillis: 13000, member: "node-3", want: Decision{Action: ActionChallenge}},
	}

	for _, testCase := range cases {
		got := Evaluate(state, mustMember(t, testCase.member), testCase.nowMillis, config)
		if got != testCase.want {
			t.Errorf("Evaluate(%s) at %d = %+v, want %+v",
				testCase.member, testCase.nowMillis, got, testCase.want)
		}
	}
}

func TestARecoveredMemberDefersToTheMemberThatReplacedIt(t *testing.T) {
	role := mustRole(t, "controller")
	config := testConfig(t)
	records := []StateRecord{
		registrationRecord(t, 10, role, "node-1", 0),
		registrationRecord(t, 20, role, "node-2", 0),
		leaseRecord(t, 30, role, "node-1", 0, 30000),
		// node-1 dies, node-2 takes the role, and then node-1 comes back.
		deregistrationRecord(t, 40, role, "node-1"),
		leaseRecord(t, 50, role, "node-2", 30000, 60000),
		registrationRecord(t, 60, role, "node-1", 31000),
	}

	state := buildState(t, role, records)

	holder, held := state.Holder()
	if !held || holder != mustMember(t, "node-2") {
		t.Fatalf("the state names holder %s (found %t), and node-2 took the role", holder, held)
	}
	recovered, found := state.Entry(mustMember(t, "node-1"))
	if !found || recovered.Offset != 60 {
		t.Fatalf("the recovered member sits at offset %d (found %t), and it registered at 60",
			recovered.Offset, found)
	}
	if rank, _ := state.RankOf(mustMember(t, "node-1")); rank != 0 {
		t.Errorf("the recovered member takes rank %d, and it is the only standby", rank)
	}
	got := Evaluate(state, mustMember(t, "node-1"), 31000, config)
	if want := (Decision{Action: ActionWait, WaitUntil: 60000}); got != want {
		t.Errorf("the recovered member gets %+v, and the live lease keeps it waiting for %+v",
			got, want)
	}
	if got := Evaluate(state, mustMember(t, "node-2"), 31000, config); got.Action != ActionHold {
		t.Errorf("the holder gets %+v, and its lease is live", got)
	}
}

func TestAThirdMemberThatRejoinsRanksBehindTheOtherStandby(t *testing.T) {
	role := mustRole(t, "controller")
	config := testConfig(t)
	records := []StateRecord{
		registrationRecord(t, 10, role, "node-1", 0),
		registrationRecord(t, 20, role, "node-2", 0),
		registrationRecord(t, 30, role, "node-3", 0),
		leaseRecord(t, 40, role, "node-1", 0, 30000),
		deregistrationRecord(t, 50, role, "node-2"),
		registrationRecord(t, 60, role, "node-2", 5000),
	}

	state := buildState(t, role, records)

	// node-3 registered before the rejoin of node-2, so node-3 leads.
	if rank, _ := state.RankOf(mustMember(t, "node-3")); rank != 0 {
		t.Errorf("node-3 takes rank %d, and it registered before the rejoin of node-2", rank)
	}
	if rank, _ := state.RankOf(mustMember(t, "node-2")); rank != 1 {
		t.Errorf("node-2 takes rank %d, and it rejoined at the tail", rank)
	}
	if got := Evaluate(state, mustMember(t, "node-3"), 30000, config); got.Action != ActionChallenge {
		t.Errorf("node-3 gets %+v at the deadline, and rank 0 challenges there", got)
	}
	got := Evaluate(state, mustMember(t, "node-2"), 30000, config)
	if want := (Decision{Action: ActionWait, WaitUntil: 35000}); got != want {
		t.Errorf("node-2 gets %+v, and rank 1 waits one stagger for %+v", got, want)
	}
}

func TestALoneMemberWithNoLeaseChallengesAtOnce(t *testing.T) {
	config := testConfig(t)
	state := RoleState{Roster: []RosterEntry{rosterEntry(t, "node-1", 10, 1000)}}

	for _, nowMillis := range []int64{1000, 9000} {
		if got := Evaluate(state, mustMember(t, "node-1"), nowMillis, config); got.Action != ActionChallenge {
			t.Errorf("the lone member gets %+v at %d, and it challenges at once", got, nowMillis)
		}
	}
}

func TestAMemberOutsideTheRosterGetsNoRankAndNoChallenge(t *testing.T) {
	config := testConfig(t)
	empty := RoleState{}
	if got := Evaluate(empty, mustMember(t, "node-1"), 0, config); got.Action != ActionNotRegistered {
		t.Errorf("an empty state gives %+v, and no member registered", got)
	}
	if _, held := empty.Holder(); held {
		t.Error("an empty state names a holder, and no member took the role")
	}
	if _, ranked := empty.RankOf(mustMember(t, "node-1")); ranked {
		t.Error("an empty state ranks a member, and no member registered")
	}

	state := RoleState{
		Roster: []RosterEntry{rosterEntry(t, "node-1", 10, 100)},
		Lease:  liveLease(t, "node-1"),
	}
	if got := Evaluate(state, mustMember(t, "node-9"), 0, config); got.Action != ActionNotRegistered {
		t.Errorf("a member outside the roster gets %+v, and it must register first", got)
	}
}
