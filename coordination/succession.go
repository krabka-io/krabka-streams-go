package coordination

import (
	"cmp"
	"fmt"
	"slices"
)

// RosterEntry is one member of the roster of a role.
type RosterEntry struct {
	// Member is the member that registered.
	Member MemberID

	// Offset is the offset of the registration record of the member in the
	// partition of the role. The offset is the join sequence of the member,
	// because log compaction keeps the offset of every record it retains.
	Offset int64

	// RegisteredAt is the time the member registered, in milliseconds since
	// the Unix epoch.
	RegisteredAt int64
}

// RoleState is the state that a reader folds out of the records of one role.
type RoleState struct {
	// Roster holds the candidates of the role in registration order, which is
	// offset order.
	Roster []RosterEntry

	// Lease is the lease of the role, and nil when no member holds it.
	Lease *Lease
}

// Holder returns the member that the lease names, and it reports whether the
// role has a lease.
//
// The holder of an expired lease is still the holder. Ask
// [LeaseTiming.LiveAt] whether the lease is live.
func (s RoleState) Holder() (MemberID, bool) {
	if s.Lease == nil {
		return MemberID{}, false
	}
	return s.Lease.Member, true
}

// Entry returns the roster entry of a member, and it reports whether the
// member registered.
func (s RoleState) Entry(member MemberID) (RosterEntry, bool) {
	for _, entry := range s.Roster {
		if entry.Member == member {
			return entry, true
		}
	}
	return RosterEntry{}, false
}

// RankOf returns the challenge rank of a member, and it reports whether the
// member registered.
//
// The rank is the index of the member in the roster after the removal of the
// current holder, so the first standby takes rank 0. The holder itself keeps
// rank 0. A holder reaches the rank test only after its own lease expired,
// and it still owns the newest epoch at that point, so it reclaims the role
// for less churn than a failover costs.
func (s RoleState) RankOf(member MemberID) (int, bool) {
	if _, found := s.Entry(member); !found {
		return 0, false
	}
	holder, held := s.Holder()
	if held && holder == member {
		return 0, true
	}
	rank := 0
	for _, entry := range s.Roster {
		if held && entry.Member == holder {
			continue
		}
		if entry.Member == member {
			return rank, true
		}
		rank++
	}
	return 0, false
}

// StateRecord is one record of [StateTopic] as it sits in the log. A
// [StateReader] returns these, and [BuildRoleState] folds them.
type StateRecord struct {
	// Offset is the offset of the record in its partition. The succession
	// rules rank candidates on the offset of a registration.
	Offset int64

	// Key is the encoded record key. [DecodeKey] reads it.
	Key []byte

	// Value is the encoded record value. An empty value is a tombstone. See
	// [IsTombstone].
	Value []byte
}

// BuildRoleState folds the records of one partition into the state of one
// role. It reports the first record that does not decode.
//
// The caller reads the partition of the role in offset order. The fold gives
// the same answer for another order, because it keeps the record of the
// highest offset for every key.
func BuildRoleState(role Role, records []StateRecord) (RoleState, error) {
	builder := NewRoleStateBuilder(role)
	for _, record := range records {
		if err := builder.Apply(record); err != nil {
			return RoleState{}, err
		}
	}
	return builder.Build(), nil
}

// memberSlot is the slot that one member keeps in a fold. registered is false
// after a tombstone. The slot stays in the map, so a registration of a lower
// offset that arrives later revives no member.
type memberSlot struct {
	offset       int64
	registeredAt int64
	registered   bool
}

// leaseSlot is the slot that the lease keeps in a fold.
type leaseSlot struct {
	offset int64
	lease  *Lease
}

// RoleStateBuilder folds the records of one role into a [RoleState].
//
// The builder keeps the record of the highest offset for every key, which is
// what log compaction keeps. A later registration for a member replaces the
// earlier one and moves the member to the tail of the roster. A registration
// tombstone removes the member. A lease tombstone clears the lease.
//
// The builder drops a record of another role, so a caller folds a partition
// that holds several roles and needs no filter of its own.
type RoleStateBuilder struct {
	role    Role
	members map[MemberID]memberSlot
	lease   *leaseSlot
}

// NewRoleStateBuilder builds an empty fold for one role.
func NewRoleStateBuilder(role Role) *RoleStateBuilder {
	return &RoleStateBuilder{role: role, members: map[MemberID]memberSlot{}}
}

// Role returns the role that this fold collects.
func (b *RoleStateBuilder) Role() Role { return b.role }

// Apply applies one record of the coordination partition. It drops a record
// of another role, and it reports a record that does not decode.
//
// The key carries the identity, because the key is the compaction key. The
// builder takes the member and the record kind from the key, and it takes
// neither from the value.
func (b *RoleStateBuilder) Apply(record StateRecord) error {
	key, err := DecodeKey(record.Key)
	if err != nil {
		return fmt.Errorf("decode the key at offset %d of %s: %w", record.Offset, StateTopic, err)
	}
	if key.Role != b.role {
		return nil
	}
	tombstone := IsTombstone(record.Value)
	switch key.Kind {
	case KindRegistration:
		if tombstone {
			b.putMember(record.Offset, key.Member, memberSlot{offset: record.Offset})
			return nil
		}
		registration, err := DecodeRegistration(record.Value)
		if err != nil {
			return fmt.Errorf("decode the registration at offset %d of %s: %w",
				record.Offset, StateTopic, err)
		}
		b.putMember(record.Offset, key.Member, memberSlot{
			offset:       record.Offset,
			registeredAt: registration.RegisteredAt,
			registered:   true,
		})
	case KindLease:
		if tombstone {
			b.putLease(record.Offset, nil)
			return nil
		}
		lease, err := DecodeLease(record.Value)
		if err != nil {
			return fmt.Errorf("decode the lease at offset %d of %s: %w",
				record.Offset, StateTopic, err)
		}
		b.putLease(record.Offset, &lease)
	}
	return nil
}

// Build returns the role state, with the roster in offset order.
func (b *RoleStateBuilder) Build() RoleState {
	roster := make([]RosterEntry, 0, len(b.members))
	for member, slot := range b.members {
		if !slot.registered {
			continue
		}
		roster = append(roster, RosterEntry{
			Member:       member,
			Offset:       slot.offset,
			RegisteredAt: slot.registeredAt,
		})
	}
	slices.SortFunc(roster, func(left, right RosterEntry) int {
		return cmp.Compare(left.Offset, right.Offset)
	})
	state := RoleState{Roster: roster}
	if b.lease != nil && b.lease.lease != nil {
		lease := *b.lease.lease
		state.Lease = &lease
	}
	return state
}

func (b *RoleStateBuilder) putMember(offset int64, member MemberID, slot memberSlot) {
	if held, found := b.members[member]; found && held.offset >= offset {
		return
	}
	b.members[member] = slot
}

func (b *RoleStateBuilder) putLease(offset int64, lease *Lease) {
	if b.lease != nil && b.lease.offset >= offset {
		return
	}
	b.lease = &leaseSlot{offset: offset, lease: lease}
}

// Action is the step that one member takes about a role now.
type Action int

const (
	// ActionNotRegistered says that the member is not in the roster of the
	// role, so it has no rank. It registers first, reads the partition again,
	// and evaluates again.
	ActionNotRegistered Action = iota

	// ActionHold says that this member holds the role and that its lease is
	// live. It renews the lease, and it calls InitProducerId for nothing.
	ActionHold

	// ActionChallenge says that this member calls InitProducerId now.
	ActionChallenge

	// ActionWait says that this member waits and then evaluates the role
	// state again. [Decision.WaitUntil] carries the instant.
	ActionWait
)

// String names the action.
func (a Action) String() string {
	switch a {
	case ActionNotRegistered:
		return "not registered"
	case ActionHold:
		return "hold"
	case ActionChallenge:
		return "challenge"
	case ActionWait:
		return "wait"
	default:
		return fmt.Sprintf("Action(%d)", int(a))
	}
}

// Decision is what one member does about a role right now. [Evaluate] returns
// one.
type Decision struct {
	// Action is the step that the member takes now.
	Action Action

	// WaitUntil is the instant of the next evaluation, in milliseconds since
	// the Unix epoch. It carries a value for [ActionWait] only.
	WaitUntil int64
}

// Evaluate decides what one member does about a role at this instant.
//
// The rules are:
//
//  1. A member that is not in the roster gets [ActionNotRegistered]. It has
//     no rank, because rank comes from the registration record.
//  2. A member that the lease names, while that lease is live, gets
//     [ActionHold].
//  3. A challenger of rank n gets [ActionChallenge] from the deadline plus n
//     challenge staggers on. With no lease, the anchor is the registration
//     instant of the member instead of a deadline, so rank 0 challenges at
//     once and rank n challenges n staggers later.
//  4. Every other member gets [ActionWait]. The instant it carries is the
//     earliest instant at which this answer changes for an unchanged role
//     state.
//
// The anchor of rule 3 always comes from the role state, and never from the
// current instant. A member that has no lease to wait for anchors on its own
// registration record. An anchor of "now plus n staggers" would move forward
// on every evaluation, and a standby of rank 1 or more would then wait for
// ever while rank 0 is dead.
//
// The member of an expired lease keeps rank 0, so it reclaims its own role at
// its own deadline. See [RoleState.RankOf].
//
// A caller evaluates again when it reads a new record, and at the latest at
// the instant that [Decision.WaitUntil] names.
func Evaluate(state RoleState, member MemberID, nowMillis int64, config LeaseConfig) Decision {
	entry, registered := state.Entry(member)
	rank, ranked := state.RankOf(member)
	if !registered || !ranked {
		return Decision{Action: ActionNotRegistered}
	}
	var challengeAt int64
	if state.Lease != nil {
		timing := config.Timing(*state.Lease)
		if state.Lease.Member == member && timing.LiveAt(nowMillis) {
			return Decision{Action: ActionHold}
		}
		challengeAt = timing.ChallengeAt(rank)
	} else {
		challengeAt = addMillis(entry.RegisteredAt, config.ChallengeDelay(rank))
	}
	if nowMillis >= challengeAt {
		return Decision{Action: ActionChallenge}
	}
	return Decision{Action: ActionWait, WaitUntil: challengeAt}
}
