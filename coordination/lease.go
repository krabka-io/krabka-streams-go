package coordination

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// The default timings of a lease. Change one with [WithLeaseDuration],
// [WithRenewInterval], or [WithChallengeStagger].
const (
	// DefaultLeaseDuration is the default extent of a lease.
	DefaultLeaseDuration = 30 * time.Second

	// DefaultRenewInterval is the default gap between two renewals by the
	// holder.
	DefaultRenewInterval = 10 * time.Second

	// DefaultChallengeStagger is the default extra delay that one rank of
	// succession adds.
	DefaultChallengeStagger = 5 * time.Second
)

// recommendedRenewFraction is the part of the lease duration that the
// documentation recommends for the renew interval.
const recommendedRenewFraction = 3

// LeaseConfig holds the three timings of one role. The timings are
// independent of the record layout, so one process holds several roles with
// different timings.
//
// The lease adds no safety. It decides when a standby challenges a quiet
// holder, and nothing else. A wrong timing makes a failover early or late. A
// wrong timing never makes two members authoritative.
//
// Build a value with [NewLeaseConfig] or take [DefaultLeaseConfig].
type LeaseConfig struct {
	duration         time.Duration
	renewInterval    time.Duration
	challengeStagger time.Duration
}

// DefaultLeaseConfig returns the config of [DefaultLeaseDuration],
// [DefaultRenewInterval], and [DefaultChallengeStagger].
func DefaultLeaseConfig() LeaseConfig {
	return LeaseConfig{
		duration:         DefaultLeaseDuration,
		renewInterval:    DefaultRenewInterval,
		challengeStagger: DefaultChallengeStagger,
	}
}

// NewLeaseConfig builds a lease policy from three extents. It rejects an
// extent at or below zero, and it rejects a renew interval at or above the
// lease duration, because the holder then never renews in time.
//
// A caller should set renewInterval to at most a third of duration. The
// holder then keeps two more attempts before the deadline. This constructor
// accepts a larger value. Ask [LeaseConfig.RenewsWithMargin] which side of
// that bound a value is on.
func NewLeaseConfig(duration, renewInterval, challengeStagger time.Duration) (LeaseConfig, error) {
	for _, extent := range []struct {
		field string
		value time.Duration
	}{
		{"lease duration", duration},
		{"renew interval", renewInterval},
		{"challenge stagger", challengeStagger},
	} {
		if extent.value <= 0 {
			return LeaseConfig{}, fmt.Errorf(
				"the %s must be a positive extent, got %s", extent.field, extent.value)
		}
	}
	if renewInterval >= duration {
		return LeaseConfig{}, fmt.Errorf(
			"the renew interval of %s is not shorter than the lease duration of %s",
			renewInterval, duration)
	}
	return LeaseConfig{
		duration:         duration,
		renewInterval:    renewInterval,
		challengeStagger: challengeStagger,
	}, nil
}

// Duration returns the extent of a lease that a member takes now.
func (c LeaseConfig) Duration() time.Duration { return c.duration }

// RenewInterval returns the gap between two renewals by the holder.
func (c LeaseConfig) RenewInterval() time.Duration { return c.renewInterval }

// ChallengeStagger returns the extra delay that one rank of succession adds.
func (c LeaseConfig) ChallengeStagger() time.Duration { return c.challengeStagger }

// RenewsWithMargin reports whether the renew interval leaves the holder two
// spare attempts. The interval this package recommends is at most a third of
// the lease duration. A config outside that bound still works, and the holder
// then loses the role after fewer missed renewals.
func (c LeaseConfig) RenewsWithMargin() bool {
	return c.renewInterval*recommendedRenewFraction <= c.duration
}

// ChallengeDelay returns the extra delay that a challenger of this rank
// takes. Rank 0 takes none. The value saturates, so a very large rank does
// not wrap the instant that a caller adds it to.
func (c LeaseConfig) ChallengeDelay(rank int) time.Duration {
	if rank <= 0 {
		return 0
	}
	if c.challengeStagger > 0 && time.Duration(rank) > math.MaxInt64/c.challengeStagger {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(rank) * c.challengeStagger
}

// Grant builds the lease record that a member writes after it mints token.
// A renewal writes the same record with the same token and a later nowMillis,
// so this one method covers a first grant and a renewal.
func (c LeaseConfig) Grant(member MemberID, token FencingToken, nowMillis int64) Lease {
	return Lease{
		Member:    member,
		Token:     token,
		GrantedAt: nowMillis,
		Deadline:  addMillis(nowMillis, c.duration),
	}
}

// Timing binds a lease record to this policy and answers the clock questions
// about it.
func (c LeaseConfig) Timing(lease Lease) LeaseTiming {
	return LeaseTiming{lease: lease, config: c}
}

// LeaseTiming is the lease clock of one lease record under one policy.
//
// Every method takes and returns milliseconds since the Unix epoch, because a
// deadline is a coordinate and not an extent. [LeaseTiming.RemainingAt] is
// the one exception, and it returns the extent that is left.
type LeaseTiming struct {
	lease  Lease
	config LeaseConfig
}

// NewLeaseTiming binds a lease record to a policy.
func NewLeaseTiming(lease Lease, config LeaseConfig) LeaseTiming {
	return LeaseTiming{lease: lease, config: config}
}

// Lease returns the lease record behind this clock.
func (t LeaseTiming) Lease() Lease { return t.lease }

// ExpiresAt returns the instant the lease expires, in milliseconds since the
// Unix epoch.
func (t LeaseTiming) ExpiresAt() int64 { return t.lease.Deadline }

// LiveAt reports whether the lease is live at this instant. The lease is live
// before its deadline and expired from the deadline on. The rank 0 challenger
// challenges at exactly the deadline, so the two tests leave no gap and no
// overlap.
func (t LeaseTiming) LiveAt(nowMillis int64) bool { return nowMillis < t.lease.Deadline }

// RemainingAt returns the extent that is left before the deadline, and zero
// after the deadline.
func (t LeaseTiming) RemainingAt(nowMillis int64) time.Duration {
	left := t.lease.Deadline - nowMillis
	if left <= 0 {
		return 0
	}
	return time.Duration(left) * time.Millisecond
}

// RenewAt returns the instant at which the holder writes its next renewal.
// The value is the grant instant plus the renew interval, and it never passes
// the deadline. A holder that follows it always writes before it loses the
// lease.
func (t LeaseTiming) RenewAt() int64 {
	renewAt := addMillis(t.lease.GrantedAt, t.config.renewInterval)
	return min(renewAt, t.lease.Deadline)
}

// RenewDueAt reports whether the holder writes a renewal at this instant.
func (t LeaseTiming) RenewDueAt(nowMillis int64) bool { return nowMillis >= t.RenewAt() }

// ChallengeAt returns the instant at which a challenger of this rank
// challenges. Rank 0 challenges at the deadline, and each later rank adds one
// challenge stagger. The stagger saves epoch churn and gives no safety.
func (t LeaseTiming) ChallengeAt(rank int) int64 {
	return addMillis(t.lease.Deadline, t.config.ChallengeDelay(rank))
}

// maxMillis is the largest instant on the epoch-millisecond line that this
// package produces. Every addition saturates at it.
const maxMillis int64 = math.MaxInt64

// addMillis adds an extent to an instant and saturates instead of wrapping.
func addMillis(instant int64, extent time.Duration) int64 {
	millis := extent.Milliseconds()
	if millis > 0 && instant > maxMillis-millis {
		return maxMillis
	}
	return instant + millis
}

// Clock is the source of the current time, and the source of every wait.
//
// The succession rules and the lease clock take an instant as a parameter, so
// they need no clock at all. This interface is the seam for the code around
// them that reads a real clock and that waits. A test supplies
// [*ManualClock], drives time by hand, and sleeps for nothing.
type Clock interface {
	// NowMillis returns the current time, in milliseconds since the Unix
	// epoch.
	NowMillis() int64

	// After returns a channel that receives one value after the extent
	// passes. An extent at or below zero receives at once.
	After(extent time.Duration) <-chan time.Time
}

// SystemClock reads the clock of the host. It is the clock that
// [AcquireLeadership] takes by default.
type SystemClock struct{}

// NowMillis returns the host time, in milliseconds since the Unix epoch.
func (SystemClock) NowMillis() int64 { return time.Now().UnixMilli() }

// After returns a channel that receives one value after the extent passes.
func (SystemClock) After(extent time.Duration) <-chan time.Time {
	if extent <= 0 {
		ready := make(chan time.Time, 1)
		ready <- time.Now()
		return ready
	}
	return time.After(extent)
}

// ManualClock is a clock that a test moves by hand. A wait that
// [ManualClock.After] returns receives when [ManualClock.Set] or
// [ManualClock.Advance] moves the clock to the instant of the wait.
//
// The clock is safe for concurrent use, so a test shares one clock between a
// holder and its challengers and moves them all together.
type ManualClock struct {
	mu        sync.Mutex
	nowMillis int64
	waits     []manualWait
}

// manualWait is one pending [ManualClock.After] call.
type manualWait struct {
	deadline int64
	ready    chan time.Time
}

// NewManualClock builds a clock that reads this instant, in milliseconds
// since the Unix epoch.
func NewManualClock(nowMillis int64) *ManualClock {
	return &ManualClock{nowMillis: nowMillis}
}

// NowMillis returns the instant the clock reads now.
func (c *ManualClock) NowMillis() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nowMillis
}

// After returns a channel that receives when the clock reaches the instant
// this extent names. A wait that is already due receives at once.
func (c *ManualClock) After(extent time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ready := make(chan time.Time, 1)
	deadline := addMillis(c.nowMillis, extent)
	if deadline <= c.nowMillis {
		ready <- time.UnixMilli(c.nowMillis)
		return ready
	}
	c.waits = append(c.waits, manualWait{deadline: deadline, ready: ready})
	return ready
}

// Set moves the clock to this instant and releases every wait that the move
// makes due. It moves the clock backwards too, and a backwards move releases
// nothing.
func (c *ManualClock) Set(nowMillis int64) {
	c.mu.Lock()
	c.nowMillis = nowMillis
	kept := c.waits[:0]
	var due []manualWait
	for _, wait := range c.waits {
		if wait.deadline <= nowMillis {
			due = append(due, wait)
			continue
		}
		kept = append(kept, wait)
	}
	c.waits = kept
	c.mu.Unlock()
	for _, wait := range due {
		wait.ready <- time.UnixMilli(nowMillis)
	}
}

// Advance moves the clock forward by this extent and releases every wait that
// the move makes due.
func (c *ManualClock) Advance(step time.Duration) {
	c.mu.Lock()
	next := addMillis(c.nowMillis, step)
	c.mu.Unlock()
	c.Set(next)
}
