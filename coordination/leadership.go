package coordination

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// DefaultPollInterval is the default gap between two reads of [StateTopic]
// while a member waits for a role. It also bounds the retry of a renewal that
// failed for a reason other than a fence.
const DefaultPollInterval = time.Second

// Option configures [AcquireLeadership], [DescribeRoleState], and
// [RoleTopicPartition].
type Option func(*settings) error

// settings holds the configuration of one call.
type settings struct {
	topic        string
	partitions   int
	lease        LeaseConfig
	clock        Clock
	pollInterval time.Duration
}

// newSettings applies the options over the defaults.
func newSettings(options []Option) (settings, error) {
	applied := settings{
		topic:        StateTopic,
		partitions:   DefaultPartitions,
		lease:        DefaultLeaseConfig(),
		clock:        SystemClock{},
		pollInterval: DefaultPollInterval,
	}
	for _, option := range options {
		if err := option(&applied); err != nil {
			return settings{}, err
		}
	}
	return applied, nil
}

// partitionOf returns the partition that holds every record of the role.
func (s settings) partitionOf(role Role) (TopicPartition, error) {
	partition, err := RolePartition(role, s.partitions)
	if err != nil {
		return TopicPartition{}, err
	}
	return TopicPartition{Topic: s.topic, Partition: partition}, nil
}

// WithTopic reads and writes another topic than [StateTopic].
func WithTopic(topic string) Option {
	return func(s *settings) error {
		if topic == "" {
			return errors.New("the coordination topic must not be empty")
		}
		s.topic = topic
		return nil
	}
}

// WithPartitions sets the partition count of the coordination topic; the
// default is [DefaultPartitions]. Every member of a cluster passes the count
// that the cluster created the topic with, because the count picks the
// partition of a role.
func WithPartitions(partitions int) Option {
	return func(s *settings) error {
		if partitions < 1 {
			return fmt.Errorf("the coordination topic needs at least one partition, got %d",
				partitions)
		}
		s.partitions = partitions
		return nil
	}
}

// WithLeaseConfig takes a whole lease policy; the default is
// [DefaultLeaseConfig].
func WithLeaseConfig(config LeaseConfig) Option {
	return func(s *settings) error {
		if config.duration <= 0 {
			return errors.New("a lease config needs a positive duration; build one with NewLeaseConfig")
		}
		s.lease = config
		return nil
	}
}

// WithLeaseDuration sets the extent of a lease; the default is
// [DefaultLeaseDuration].
func WithLeaseDuration(duration time.Duration) Option {
	return func(s *settings) error {
		config, err := NewLeaseConfig(duration, s.lease.renewInterval, s.lease.challengeStagger)
		if err != nil {
			return err
		}
		s.lease = config
		return nil
	}
}

// WithRenewInterval sets the gap between two renewals by the holder; the
// default is [DefaultRenewInterval].
func WithRenewInterval(interval time.Duration) Option {
	return func(s *settings) error {
		config, err := NewLeaseConfig(s.lease.duration, interval, s.lease.challengeStagger)
		if err != nil {
			return err
		}
		s.lease = config
		return nil
	}
}

// WithChallengeStagger sets the extra delay that one rank of succession adds;
// the default is [DefaultChallengeStagger].
func WithChallengeStagger(stagger time.Duration) Option {
	return func(s *settings) error {
		config, err := NewLeaseConfig(s.lease.duration, s.lease.renewInterval, stagger)
		if err != nil {
			return err
		}
		s.lease = config
		return nil
	}
}

// WithClock reads the time from another clock than [SystemClock]. A test
// passes [NewManualClock] and drives every wait by hand.
func WithClock(clock Clock) Option {
	return func(s *settings) error {
		if clock == nil {
			return errors.New("a coordination clock must not be nil")
		}
		s.clock = clock
		return nil
	}
}

// WithPollInterval sets the longest gap between two reads of the coordination
// topic while a member waits; the default is [DefaultPollInterval]. A shorter
// gap finds a resigned holder sooner and costs more fetches.
func WithPollInterval(interval time.Duration) Option {
	return func(s *settings) error {
		if interval <= 0 {
			return fmt.Errorf("the poll interval must be a positive extent, got %s", interval)
		}
		s.pollInterval = interval
		return nil
	}
}

// RoleTopicPartition returns the partition of the coordination topic that
// holds every record of the role. Only [WithTopic] and [WithPartitions]
// change the answer.
func RoleTopicPartition(role Role, options ...Option) (TopicPartition, error) {
	applied, err := newSettings(options)
	if err != nil {
		return TopicPartition{}, err
	}
	return applied.partitionOf(role)
}

// DescribeRoleState reads the partition of one role and folds it into the
// roster and the lease of that role. A tool that reports the state of a
// cluster calls it, and it takes no epoch.
func DescribeRoleState(ctx context.Context, reader StateReader, role Role, options ...Option) (RoleState, error) {
	if reader == nil {
		return RoleState{}, errors.New("a coordination read needs a state reader")
	}
	applied, err := newSettings(options)
	if err != nil {
		return RoleState{}, err
	}
	partition, err := applied.partitionOf(role)
	if err != nil {
		return RoleState{}, err
	}
	return readRoleState(ctx, reader, partition, role)
}

func readRoleState(ctx context.Context, reader StateReader, partition TopicPartition, role Role) (RoleState, error) {
	records, err := reader.ReadPartition(ctx, partition)
	if err != nil {
		return RoleState{}, fmt.Errorf("read the state of role %s: %w", role, err)
	}
	state, err := BuildRoleState(role, records)
	if err != nil {
		return RoleState{}, fmt.Errorf("read the state of role %s: %w", role, err)
	}
	return state, nil
}

// LeadershipStatus reports the token that the transaction coordinator holds
// for one role. [DescribeLeadership] builds it.
type LeadershipStatus struct {
	// Role is the role that the status describes.
	Role Role

	// Token is the token that the transaction coordinator holds now. It is
	// [NoEpoch] when no member has ever taken the role.
	Token FencingToken

	// Held reports whether a member has taken the role.
	Held bool
}

// Authoritative reports whether the token is the token that the coordinator
// holds now. A third party checks the authority of a writer with this one
// question, and it joins no group.
func (s LeadershipStatus) Authoritative(token FencingToken) bool {
	return s.Held && s.Token == token
}

// String describes the status.
func (s LeadershipStatus) String() string {
	if !s.Held {
		return fmt.Sprintf("role %s: no epoch", s.Role)
	}
	return fmt.Sprintf("role %s: epoch %s", s.Role, s.Token)
}

// DescribeLeadership reports which token holds a role now.
//
// The call reaches the transaction coordinator only. It reads no topic, it
// joins no group, and it takes no epoch, so any process makes it. This is the
// third-party check of the authority of a writer.
func DescribeLeadership(ctx context.Context, coordinator Coordinator, role Role) (LeadershipStatus, error) {
	if coordinator == nil {
		return LeadershipStatus{}, errors.New("a leadership description needs a coordinator")
	}
	token, held, err := coordinator.DescribeEpoch(ctx, role)
	if err != nil {
		return LeadershipStatus{}, fmt.Errorf("describe the epoch of role %s: %w", role, err)
	}
	if !held {
		return LeadershipStatus{Role: role, Token: NoEpoch}, nil
	}
	return LeadershipStatus{Role: role, Token: token, Held: true}, nil
}

// AcquireLeadership competes for a role, and it returns when this member
// holds the role.
//
// The call registers the member, waits for its turn under the succession
// rules, mints the epoch of the role, and writes the first lease record. It
// returns a [*Leadership] that renews the lease in the background. Every
// write that the caller guards with [Leadership.Token] is fenced by the
// broker.
//
// The call reads the coordination topic in a loop, so the caller bounds the
// wait with the context. It returns the context error when the deadline
// passes before this member wins the role.
//
// A role state that names this member takes the same path as a challenge. The
// record came from an earlier incarnation of the member, because this call
// holds no epoch yet. The new epoch fences that earlier incarnation, and it
// costs one epoch.
func AcquireLeadership(ctx context.Context, transport Transport, role Role, member MemberID, options ...Option) (*Leadership, error) {
	if transport == nil {
		return nil, errors.New("a leadership acquisition needs a transport")
	}
	if role.name == "" {
		return nil, errors.New("a leadership acquisition needs a role; build one with NewRole")
	}
	if member.name == "" {
		return nil, errors.New("a leadership acquisition needs a member id; build one with NewMemberID")
	}
	applied, err := newSettings(options)
	if err != nil {
		return nil, err
	}
	partition, err := applied.partitionOf(role)
	if err != nil {
		return nil, err
	}

	registered := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		state, err := readRoleState(ctx, transport, partition, role)
		if err != nil {
			return nil, err
		}
		decision := Evaluate(state, member, applied.clock.NowMillis(), applied.lease)
		switch decision.Action {
		case ActionNotRegistered:
			if registered {
				// The append reached the broker, and the read has not caught
				// up with it. Read the partition again.
				if err := wait(ctx, applied.clock, applied.pollInterval); err != nil {
					return nil, err
				}
				continue
			}
			key := EncodeKey(RegistrationKey(role, member))
			value := EncodeRegistration(Registration{
				Member:       member,
				RegisteredAt: applied.clock.NowMillis(),
			})
			if err := transport.Append(ctx, partition, key, value); err != nil {
				return nil, fmt.Errorf("register member %s for role %s: %w", member, role, err)
			}
			registered = true
		case ActionWait:
			delay := min(millisUntil(decision.WaitUntil, applied.clock.NowMillis()), applied.pollInterval)
			if err := wait(ctx, applied.clock, delay); err != nil {
				return nil, err
			}
		case ActionHold, ActionChallenge:
			leadership, err := challenge(ctx, transport, applied, partition, role, member)
			if err != nil {
				if errors.Is(err, ErrFenced) {
					// A member with a newer epoch took the role between the
					// read and the write. Read the partition again.
					continue
				}
				return nil, err
			}
			return leadership, nil
		}
	}
}

// challenge mints the epoch of the role and writes the first lease record.
func challenge(ctx context.Context, transport Transport, applied settings, partition TopicPartition, role Role, member MemberID) (*Leadership, error) {
	token, err := transport.AcquireEpoch(ctx, role)
	if err != nil {
		return nil, fmt.Errorf("mint the epoch of role %s: %w", role, err)
	}
	lease := applied.lease.Grant(member, token, applied.clock.NowMillis())
	err = transport.WriteLease(ctx, partition, token,
		EncodeKey(LeaseKey(role)), EncodeLease(lease))
	if err != nil {
		return nil, fmt.Errorf("write the lease of role %s: %w", role, err)
	}
	return newLeadership(transport, applied, partition, role, member, token, lease), nil
}

// Leadership is the live leadership of one member over one role.
//
// The handle carries the epoch that the transaction coordinator minted, and
// the caller passes that epoch to every guarded write. The broker fences a
// write of an older epoch, so the caller proves its authority with the write
// itself and asks no question first.
//
// The handle renews the lease in the background. The renewal keeps the
// standbys from a challenge, and it adds no safety. [Leadership.Done] closes
// when the leadership ends, and [Leadership.Err] then says why. A caller
// watches Done and stops the work of the role at once.
//
// A Leadership is safe for concurrent use.
type Leadership struct {
	role      Role
	member    MemberID
	token     FencingToken
	partition TopicPartition
	writer    LeaseWriter
	config    LeaseConfig
	clock     Clock
	retry     time.Duration

	cancel context.CancelFunc
	done   chan struct{}
	closed sync.Once

	mu    sync.Mutex
	lease Lease
	err   error
}

// newLeadership builds a handle and starts its renewal loop.
func newLeadership(transport Transport, applied settings, partition TopicPartition, role Role, member MemberID, token FencingToken, lease Lease) *Leadership {
	// The renewal loop outlives the call that acquired the role, so it takes
	// no cancellation from the context of that call. Resign stops it.
	loopCtx, cancel := context.WithCancel(context.Background())
	leadership := &Leadership{
		role:      role,
		member:    member,
		token:     token,
		partition: partition,
		writer:    transport,
		config:    applied.lease,
		clock:     applied.clock,
		retry:     applied.pollInterval,
		cancel:    cancel,
		done:      make(chan struct{}),
		lease:     lease,
	}
	go leadership.renewLoop(loopCtx)
	return leadership
}

// Role returns the role that this member holds.
func (l *Leadership) Role() Role { return l.role }

// Member returns the member that holds the role.
func (l *Leadership) Member() MemberID { return l.member }

// Token returns the quorum-minted proof of this leadership. The caller passes
// it to every guarded write.
func (l *Leadership) Token() FencingToken { return l.token }

// Partition returns the partition of the coordination topic that holds every
// record of the role.
func (l *Leadership) Partition() TopicPartition { return l.partition }

// Lease returns the lease record that this member wrote last.
func (l *Leadership) Lease() Lease {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lease
}

// Done returns a channel that closes when the leadership ends. A fence closes
// it, and [Leadership.Resign] closes it. [Leadership.Err] says which.
func (l *Leadership) Done() <-chan struct{} { return l.done }

// Err returns the reason the leadership ended, and nil while it holds. The
// error of a fence wraps [ErrFenced], and the error of a resignation wraps
// [ErrNotHeld].
func (l *Leadership) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Renew writes a fresh lease record under the same epoch. The background loop
// calls it at every renew interval, and a caller that drives its own schedule
// calls it too.
//
// A renewal that the broker fences ends the leadership. Renew then returns an
// error that wraps [ErrFenced], and [Leadership.Done] closes.
func (l *Leadership) Renew(ctx context.Context) error {
	if err := l.Err(); err != nil {
		return err
	}
	lease := l.config.Grant(l.member, l.token, l.clock.NowMillis())
	err := l.writer.WriteLease(ctx, l.partition, l.token,
		EncodeKey(LeaseKey(l.role)), EncodeLease(lease))
	if err != nil {
		if errors.Is(err, ErrFenced) {
			l.end(fmt.Errorf("role %s: %w", l.role, ErrFenced))
		}
		return fmt.Errorf("renew the lease of role %s: %w", l.role, err)
	}
	l.mu.Lock()
	l.lease = lease
	l.mu.Unlock()
	return nil
}

// Resign gives the role up. It stops the renewal loop and writes a tombstone
// on the lease key, so the first standby challenges at once instead of at the
// deadline. It ends the leadership, whatever the write does.
//
// Resign takes no epoch back. The member keeps the epoch it minted until
// another member mints a newer one, and the broker still accepts a write of
// the resigned member until then. A caller that resigns stops its own guarded
// writes.
func (l *Leadership) Resign(ctx context.Context) error {
	ended := l.Err() != nil
	l.end(fmt.Errorf("role %s: %w", l.role, ErrNotHeld))
	if ended {
		return nil
	}
	err := l.writer.WriteLease(ctx, l.partition, l.token, EncodeKey(LeaseKey(l.role)), nil)
	if err != nil && !errors.Is(err, ErrFenced) {
		return fmt.Errorf("clear the lease of role %s: %w", l.role, err)
	}
	return nil
}

// end records the reason and closes the done channel exactly once.
func (l *Leadership) end(reason error) {
	l.mu.Lock()
	if l.err == nil {
		l.err = reason
	}
	l.mu.Unlock()
	l.closed.Do(func() { close(l.done) })
	l.cancel()
}

// renewLoop writes a renewal at every renew interval. It ends the leadership
// when the broker fences the member, and it ends the leadership when the
// deadline of the lease passes with no successful renewal.
func (l *Leadership) renewLoop(ctx context.Context) {
	for {
		timing := l.config.Timing(l.Lease())
		delay := millisUntil(timing.RenewAt(), l.clock.NowMillis())
		if err := wait(ctx, l.clock, delay); err != nil {
			return
		}
		err := l.Renew(ctx)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrFenced) || errors.Is(err, ErrNotHeld) {
			return
		}
		if l.clock.NowMillis() >= timing.ExpiresAt() {
			l.end(fmt.Errorf("role %s: the lease expired at %d after a failed renewal: %w",
				l.role, timing.ExpiresAt(), err))
			return
		}
		if err := wait(ctx, l.clock, l.retry); err != nil {
			return
		}
	}
}

// millisUntil returns the extent between two instants on the
// epoch-millisecond line, and zero for an instant that passed.
func millisUntil(instant, nowMillis int64) time.Duration {
	if instant <= nowMillis {
		return 0
	}
	return time.Duration(instant-nowMillis) * time.Millisecond
}

// wait sleeps on the clock, and it returns the error of the context when the
// caller cancels first.
func wait(ctx context.Context, clock Clock, extent time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if extent <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-clock.After(extent):
		return nil
	}
}
