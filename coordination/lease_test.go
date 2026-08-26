package coordination

import (
	"math"
	"testing"
	"time"
)

func TestNewLeaseConfigRejectsExtentsThatCannotWorkTogether(t *testing.T) {
	cases := []struct {
		name             string
		duration         time.Duration
		renewInterval    time.Duration
		challengeStagger time.Duration
	}{
		{name: "a zero duration", duration: 0, renewInterval: time.Second, challengeStagger: time.Second},
		{name: "a negative duration", duration: -time.Second, renewInterval: time.Second, challengeStagger: time.Second},
		{name: "a zero renew interval", duration: time.Minute, renewInterval: 0, challengeStagger: time.Second},
		{name: "a zero challenge stagger", duration: time.Minute, renewInterval: time.Second, challengeStagger: 0},
		{
			name:             "a renew interval at the lease duration",
			duration:         time.Minute,
			renewInterval:    time.Minute,
			challengeStagger: time.Second,
		},
		{
			name:             "a renew interval over the lease duration",
			duration:         time.Minute,
			renewInterval:    2 * time.Minute,
			challengeStagger: time.Second,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := NewLeaseConfig(testCase.duration, testCase.renewInterval, testCase.challengeStagger)
			if err == nil {
				t.Fatalf("NewLeaseConfig took %s, %s, %s",
					testCase.duration, testCase.renewInterval, testCase.challengeStagger)
			}
		})
	}
}

func TestTheDefaultLeaseConfigRenewsWithMargin(t *testing.T) {
	config := DefaultLeaseConfig()
	if config.Duration() != DefaultLeaseDuration {
		t.Errorf("Duration() = %s, want %s", config.Duration(), DefaultLeaseDuration)
	}
	if config.RenewInterval() != DefaultRenewInterval {
		t.Errorf("RenewInterval() = %s, want %s", config.RenewInterval(), DefaultRenewInterval)
	}
	if config.ChallengeStagger() != DefaultChallengeStagger {
		t.Errorf("ChallengeStagger() = %s, want %s", config.ChallengeStagger(), DefaultChallengeStagger)
	}
	if !config.RenewsWithMargin() {
		t.Error("the default config must leave the holder two spare attempts")
	}

	tight, err := NewLeaseConfig(30*time.Second, 20*time.Second, time.Second)
	if err != nil {
		t.Fatalf("NewLeaseConfig: %v", err)
	}
	if tight.RenewsWithMargin() {
		t.Error("a renew interval of two thirds of the duration leaves no margin")
	}
}

func TestTheLeaseClockAnswersTheTimingQuestionsOfOneLease(t *testing.T) {
	config := testConfig(t)
	lease := config.Grant(mustMember(t, "node-1"), mustToken(t, 4242, 7), 1000)

	if lease.GrantedAt != 1000 || lease.Deadline != 31000 {
		t.Fatalf("Grant returns %+v, and a 30-second lease taken at 1000 ends at 31000", lease)
	}

	timing := config.Timing(lease)
	if timing.Lease() != lease {
		t.Errorf("Lease() = %+v, want %+v", timing.Lease(), lease)
	}
	if timing.ExpiresAt() != 31000 {
		t.Errorf("ExpiresAt() = %d, want 31000", timing.ExpiresAt())
	}
	if !timing.LiveAt(30999) {
		t.Error("the lease is live one millisecond before the deadline")
	}
	if timing.LiveAt(31000) {
		t.Error("the lease is expired at the deadline, so rank 0 challenges there")
	}
	if got := timing.RemainingAt(21000); got != 10*time.Second {
		t.Errorf("RemainingAt(21000) = %s, want 10s", got)
	}
	if got := timing.RemainingAt(40000); got != 0 {
		t.Errorf("RemainingAt(40000) = %s, want 0s", got)
	}
	if timing.RenewAt() != 11000 {
		t.Errorf("RenewAt() = %d, want 11000", timing.RenewAt())
	}
	if timing.RenewDueAt(10999) {
		t.Error("no renewal is due before the renew interval passes")
	}
	if !timing.RenewDueAt(11000) {
		t.Error("a renewal is due at the renew interval")
	}
	if got := timing.ChallengeAt(0); got != 31000 {
		t.Errorf("ChallengeAt(0) = %d, want the deadline 31000", got)
	}
	if got := timing.ChallengeAt(3); got != 46000 {
		t.Errorf("ChallengeAt(3) = %d, want 46000", got)
	}
}

// The renew instant never passes the deadline, so a holder that follows it
// always writes before it loses the lease.
func TestTheRenewInstantNeverPassesTheDeadline(t *testing.T) {
	config := testConfig(t)
	// A truncated lease: the holder wrote a deadline before its own renew
	// instant. The clock takes the deadline.
	lease := Lease{
		Member:    mustMember(t, "node-1"),
		Token:     mustToken(t, 1, 1),
		GrantedAt: 1000,
		Deadline:  5000,
	}

	if got := NewLeaseTiming(lease, config).RenewAt(); got != 5000 {
		t.Errorf("RenewAt() = %d, and it must stop at the deadline 5000", got)
	}
}

func TestTheTimingsSaturateInsteadOfWrapping(t *testing.T) {
	config := testConfig(t)

	if got := config.ChallengeDelay(-1); got != 0 {
		t.Errorf("ChallengeDelay(-1) = %s, want 0s", got)
	}
	if got := config.ChallengeDelay(math.MaxInt64); got != time.Duration(math.MaxInt64) {
		t.Errorf("ChallengeDelay(math.MaxInt64) = %s, and it must saturate", got)
	}

	lease := config.Grant(mustMember(t, "node-1"), mustToken(t, 1, 1), math.MaxInt64-10)
	if lease.Deadline != math.MaxInt64 {
		t.Errorf("Grant near the end of the line returns deadline %d, and it must saturate",
			lease.Deadline)
	}
	if got := config.Timing(lease).ChallengeAt(math.MaxInt64); got != math.MaxInt64 {
		t.Errorf("ChallengeAt(math.MaxInt64) = %d, and it must saturate", got)
	}
}

func TestTheManualClockReleasesAWaitWhenItReachesTheInstant(t *testing.T) {
	clock := NewManualClock(1000)
	if got := clock.NowMillis(); got != 1000 {
		t.Fatalf("NowMillis() = %d, want 1000", got)
	}

	pending := clock.After(5 * time.Second)
	select {
	case <-pending:
		t.Fatal("the wait released before the clock reached the instant")
	default:
	}

	clock.Advance(4 * time.Second)
	select {
	case <-pending:
		t.Fatal("the wait released four seconds into a five-second wait")
	default:
	}

	clock.Advance(time.Second)
	select {
	case <-pending:
	case <-time.After(time.Second):
		t.Fatal("the wait held after the clock reached the instant")
	}
	if got := clock.NowMillis(); got != 6000 {
		t.Errorf("NowMillis() = %d, want 6000", got)
	}
}

func TestAWaitThatIsAlreadyDueReleasesAtOnce(t *testing.T) {
	clocks := []Clock{NewManualClock(1000), SystemClock{}}
	for _, clock := range clocks {
		for _, extent := range []time.Duration{0, -time.Second} {
			select {
			case <-clock.After(extent):
			case <-time.After(time.Second):
				t.Fatalf("%T held a wait of %s", clock, extent)
			}
		}
	}
}

func TestTheSystemClockReadsTheHostTime(t *testing.T) {
	before := time.Now().UnixMilli()
	got := SystemClock{}.NowMillis()
	after := time.Now().UnixMilli()

	if got < before || got > after {
		t.Errorf("NowMillis() = %d, and the host time is between %d and %d", got, before, after)
	}
}
