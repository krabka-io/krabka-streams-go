package coordination

import (
	"math"
	"testing"
)

// The producer epoch is an int16 and it wraps. Kafka answers the exhaustion
// with a fresh producer id and an epoch of zero. A comparison on the epoch
// alone would rank the fresh token below the stale one, and the deposed
// leader would keep the role after about 32000 leadership changes.
func TestAFreshProducerIDSupersedesAnExhaustedEpoch(t *testing.T) {
	exhausted := mustToken(t, 4, math.MaxInt16)
	fresh := mustToken(t, 5, 0)

	if !fresh.Supersedes(exhausted) {
		t.Fatalf("%s must supersede %s, because the producer id leads the comparison",
			fresh, exhausted)
	}
	if fresh.Compare(exhausted) <= 0 {
		t.Errorf("Compare(%s, %s) = %d, want a positive number",
			fresh, exhausted, fresh.Compare(exhausted))
	}
	if fresh.ProducerEpoch() >= exhausted.ProducerEpoch() {
		t.Fatal("this fixture needs the fresh epoch below the exhausted one")
	}
}

func TestTheTokenComparisonReadsTheProducerIDFirst(t *testing.T) {
	cases := []struct {
		name  string
		left  FencingToken
		right FencingToken
		want  int
	}{
		{name: "the same token", left: mustToken(t, 7, 3), right: mustToken(t, 7, 3), want: 0},
		{name: "a later epoch of one producer", left: mustToken(t, 7, 4), right: mustToken(t, 7, 3), want: 1},
		{name: "an earlier epoch of one producer", left: mustToken(t, 7, 2), right: mustToken(t, 7, 3), want: -1},
		{name: "a later producer id", left: mustToken(t, 8, 0), right: mustToken(t, 7, 9), want: 1},
		{name: "an earlier producer id", left: mustToken(t, 6, 9), right: mustToken(t, 7, 0), want: -1},
		{name: "no epoch against a minted token", left: NoEpoch, right: mustToken(t, 0, 0), want: -1},
		{name: "no epoch against itself", left: NoEpoch, right: NoEpoch, want: 0},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.left.Compare(testCase.right); got != testCase.want {
				t.Errorf("Compare(%s, %s) = %d, want %d",
					testCase.left, testCase.right, got, testCase.want)
			}
		})
	}
}

func TestANegativeTokenIsRejected(t *testing.T) {
	cases := []struct {
		name          string
		producerID    int64
		producerEpoch int16
	}{
		{name: "a negative producer id", producerID: -1, producerEpoch: 0},
		{name: "a negative producer epoch", producerID: 0, producerEpoch: -1},
		{name: "the pair Kafka writes for no producer", producerID: -1, producerEpoch: -1},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewFencingToken(testCase.producerID, testCase.producerEpoch); err == nil {
				t.Fatalf("NewFencingToken(%d, %d) took a negative value",
					testCase.producerID, testCase.producerEpoch)
			}
		})
	}
}

func TestATokenSurvivesTheTextRoundTrip(t *testing.T) {
	token := mustToken(t, 4242, 7)
	if token.String() != "4242:7" {
		t.Errorf("String() = %q, want %q", token.String(), "4242:7")
	}

	parsed, err := ParseFencingToken(token.String())
	if err != nil {
		t.Fatalf("ParseFencingToken(%q): %v", token.String(), err)
	}
	if parsed != token {
		t.Errorf("the round trip returns %s, and the input is %s", parsed, token)
	}
}

func TestParseFencingTokenRejectsAMalformedString(t *testing.T) {
	cases := []string{"", "4242", "4242:7:1", "4242:", ":7", "x:7", "4242:x", "-1:7", "4242:40000"}

	for _, text := range cases {
		if _, err := ParseFencingToken(text); err == nil {
			t.Errorf("ParseFencingToken(%q) took a malformed string", text)
		}
	}
}
