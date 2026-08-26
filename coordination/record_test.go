package coordination

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func mustRole(t *testing.T, name string) Role {
	t.Helper()
	role, err := NewRole(name)
	if err != nil {
		t.Fatalf("NewRole(%q): %v", name, err)
	}
	return role
}

func mustMember(t *testing.T, name string) MemberID {
	t.Helper()
	member, err := NewMemberID(name)
	if err != nil {
		t.Fatalf("NewMemberID(%q): %v", name, err)
	}
	return member
}

func mustToken(t *testing.T, producerID int64, producerEpoch int16) FencingToken {
	t.Helper()
	token, err := NewFencingToken(producerID, producerEpoch)
	if err != nil {
		t.Fatalf("NewFencingToken(%d, %d): %v", producerID, producerEpoch, err)
	}
	return token
}

func TestAKeyAndAValueSurviveTheRoundTrip(t *testing.T) {
	role := mustRole(t, "controller")
	member := mustMember(t, "node-1")
	cases := []struct {
		name string
		key  Key
	}{
		{name: "a registration key", key: RegistrationKey(role, member)},
		{name: "a lease key", key: LeaseKey(role)},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			decoded, err := DecodeKey(EncodeKey(testCase.key))
			if err != nil {
				t.Fatalf("DecodeKey: %v", err)
			}
			if !reflect.DeepEqual(decoded, testCase.key) {
				t.Errorf("the round trip returns %+v, and the input is %+v", decoded, testCase.key)
			}
		})
	}

	registration := Registration{Member: member, RegisteredAt: 1700000000000}
	decodedRegistration, err := DecodeRegistration(EncodeRegistration(registration))
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if !reflect.DeepEqual(decodedRegistration, registration) {
		t.Errorf("the round trip returns %+v, and the input is %+v",
			decodedRegistration, registration)
	}

	lease := Lease{
		Member:    member,
		Token:     mustToken(t, math.MaxInt64, math.MaxInt16),
		GrantedAt: -1,
		Deadline:  math.MaxInt64,
	}
	decodedLease, err := DecodeLease(EncodeLease(lease))
	if err != nil {
		t.Fatalf("DecodeLease: %v", err)
	}
	if !reflect.DeepEqual(decodedLease, lease) {
		t.Errorf("the round trip returns %+v, and the input is %+v", decodedLease, lease)
	}
}

// The frozen layout gives a lease no member, so the encoder drops one that a
// caller put in the key by hand.
func TestALeaseKeyCarriesTheEmptyMemberString(t *testing.T) {
	role := mustRole(t, "controller")
	byHand := Key{Kind: KindLease, Role: role, Member: mustMember(t, "node-1")}

	encoded := EncodeKey(byHand)
	if want := EncodeKey(LeaseKey(role)); !reflect.DeepEqual(encoded, want) {
		t.Fatalf("the encoder wrote %v for a lease key with a member, and the layout wants %v",
			encoded, want)
	}
	decoded, err := DecodeKey(encoded)
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	if decoded.Member != (MemberID{}) {
		t.Errorf("the decoded lease key names member %q, and a lease belongs to the role",
			decoded.Member)
	}
}

func TestTheDecoderRejectsAMalformedRecordAndNamesTheRecordPart(t *testing.T) {
	registrationKey := EncodeKey(RegistrationKey(mustRole(t, "controller"), mustMember(t, "node-1")))
	leaseKey := EncodeKey(LeaseKey(mustRole(t, "controller")))
	registrationValue := EncodeRegistration(Registration{
		Member:       mustMember(t, "node-1"),
		RegisteredAt: 1700000000000,
	})
	leaseValue := EncodeLease(Lease{
		Member:    mustMember(t, "node-1"),
		Token:     mustToken(t, 4242, 7),
		GrantedAt: 1,
		Deadline:  2,
	})

	cases := []struct {
		name    string
		data    []byte
		decode  func([]byte) error
		part    string
		message string
	}{
		{
			name:    "an empty key",
			data:    nil,
			decode:  func(data []byte) error { _, err := DecodeKey(data); return err },
			part:    "key",
			message: "truncated",
		},
		{
			name:    "a truncated key",
			data:    registrationKey[:len(registrationKey)-1],
			decode:  func(data []byte) error { _, err := DecodeKey(data); return err },
			part:    "key",
			message: "truncated",
		},
		{
			name:    "trailing bytes after a key",
			data:    append(append([]byte{}, registrationKey...), 0x00),
			decode:  func(data []byte) error { _, err := DecodeKey(data); return err },
			part:    "key",
			message: "trailing bytes",
		},
		{
			name:    "an unknown key version",
			data:    append([]byte{0x00, 0x01}, registrationKey[2:]...),
			decode:  func(data []byte) error { _, err := DecodeKey(data); return err },
			part:    "key",
			message: "unsupported",
		},
		{
			name:    "an unknown record kind",
			data:    append(append([]byte{}, registrationKey[:2]...), append([]byte{0x00, 0x09}, registrationKey[4:]...)...),
			decode:  func(data []byte) error { _, err := DecodeKey(data); return err },
			part:    "key",
			message: "unknown coordination state record kind 9",
		},
		{
			name:    "a negative string length",
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF},
			decode:  func(data []byte) error { _, err := DecodeKey(data); return err },
			part:    "key",
			message: "negative string length",
		},
		{
			name:    "an empty role",
			data:    []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
			decode:  func(data []byte) error { _, err := DecodeKey(data); return err },
			part:    "key",
			message: "must not be empty",
		},
		{
			name:    "a lease key that names a member",
			data:    append(append([]byte{}, leaseKey[:len(leaseKey)-2]...), append([]byte{0x00, 0x01}, 'x')...),
			decode:  func(data []byte) error { _, err := DecodeKey(data); return err },
			part:    "key",
			message: "a lease key carries the empty member string",
		},
		{
			name:    "a non-UTF-8 role",
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF, 0x00, 0x01, 'x'},
			decode:  func(data []byte) error { _, err := DecodeKey(data); return err },
			part:    "key",
			message: "non-UTF-8",
		},
		{
			name:    "a truncated registration value",
			data:    registrationValue[:len(registrationValue)-1],
			decode:  func(data []byte) error { _, err := DecodeRegistration(data); return err },
			part:    "registration value",
			message: "truncated",
		},
		{
			name:    "trailing bytes after a registration value",
			data:    append(append([]byte{}, registrationValue...), 0x00),
			decode:  func(data []byte) error { _, err := DecodeRegistration(data); return err },
			part:    "registration value",
			message: "trailing bytes",
		},
		{
			name:    "an unknown registration value version",
			data:    append([]byte{0x00, 0x07}, registrationValue[2:]...),
			decode:  func(data []byte) error { _, err := DecodeRegistration(data); return err },
			part:    "registration value",
			message: "unsupported",
		},
		{
			name:    "a truncated lease value",
			data:    leaseValue[:len(leaseValue)-1],
			decode:  func(data []byte) error { _, err := DecodeLease(data); return err },
			part:    "lease value",
			message: "truncated",
		},
		{
			name:    "trailing bytes after a lease value",
			data:    append(append([]byte{}, leaseValue...), 0x00),
			decode:  func(data []byte) error { _, err := DecodeLease(data); return err },
			part:    "lease value",
			message: "trailing bytes",
		},
		{
			name: "a negative producer id",
			data: func() []byte {
				data := append([]byte{}, leaseValue...)
				for index := 10; index < 18; index++ {
					data[index] = 0xFF
				}
				return data
			}(),
			decode:  func(data []byte) error { _, err := DecodeLease(data); return err },
			part:    "lease value",
			message: "must not be negative",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.decode(testCase.data)
			if err == nil {
				t.Fatalf("the decoder took %v, and the bytes are malformed", testCase.data)
			}
			var format *FormatError
			if !errors.As(err, &format) {
				t.Fatalf("the decoder returned %T, and it must return a *FormatError", err)
			}
			if format.Part != testCase.part {
				t.Errorf("the error names part %q, and the fault is in the %q",
					format.Part, testCase.part)
			}
			if !strings.Contains(format.Error(), testCase.message) {
				t.Errorf("the error says %q, and it must name %q", format.Error(), testCase.message)
			}
		})
	}
}

// The decoder reads bytes that arrive from a network. No input panics it.
func TestTheDecoderReturnsAnErrorAndDoesNotPanicOnArbitraryBytes(t *testing.T) {
	seed := EncodeKey(RegistrationKey(mustRole(t, "controller"), mustMember(t, "node-1")))
	for cut := range len(seed) + 1 {
		for _, mutant := range [][]byte{seed[:cut], append(append([]byte{}, seed[:cut]...), 0xFF, 0xFE)} {
			if _, err := DecodeKey(mutant); err == nil && cut != len(seed) {
				t.Errorf("DecodeKey took the truncated buffer %v", mutant)
			}
			if _, err := DecodeRegistration(mutant); err == nil {
				t.Errorf("DecodeRegistration took the buffer %v of a key", mutant)
			}
			if _, err := DecodeLease(mutant); err == nil {
				t.Errorf("DecodeLease took the buffer %v of a key", mutant)
			}
		}
	}
}

func TestAConstructorRejectsAnEmptyNameAndANameOverTheBound(t *testing.T) {
	cases := []struct {
		name  string
		build func(string) error
		input string
	}{
		{name: "an empty role", build: func(n string) error { _, err := NewRole(n); return err }, input: ""},
		{
			name:  "a role over the bound",
			build: func(n string) error { _, err := NewRole(n); return err },
			input: strings.Repeat("r", MaxRoleLen+1),
		},
		{name: "an empty member id", build: func(n string) error { _, err := NewMemberID(n); return err }, input: ""},
		{
			name:  "a member id over the bound",
			build: func(n string) error { _, err := NewMemberID(n); return err },
			input: strings.Repeat("m", MaxMemberIDLen+1),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.build(testCase.input); err == nil {
				t.Fatalf("the constructor took %q", testCase.input)
			}
		})
	}

	if _, err := NewRole(strings.Repeat("r", MaxRoleLen)); err != nil {
		t.Errorf("the constructor rejected a role of exactly %d bytes: %v", MaxRoleLen, err)
	}
	if _, err := NewMemberID(strings.Repeat("m", MaxMemberIDLen)); err != nil {
		t.Errorf("the constructor rejected a member id of exactly %d bytes: %v", MaxMemberIDLen, err)
	}
}

func TestARecordKindAndAnActionNameThemselves(t *testing.T) {
	cases := []struct {
		got  string
		want string
	}{
		{got: KindRegistration.String(), want: "registration"},
		{got: KindLease.String(), want: "lease"},
		{got: RecordKind(9).String(), want: "RecordKind(9)"},
		{got: ActionHold.String(), want: "hold"},
		{got: ActionChallenge.String(), want: "challenge"},
		{got: ActionWait.String(), want: "wait"},
		{got: ActionNotRegistered.String(), want: "not registered"},
		{got: Action(9).String(), want: "Action(9)"},
	}

	for _, testCase := range cases {
		if testCase.got != testCase.want {
			t.Errorf("String() = %q, want %q", testCase.got, testCase.want)
		}
	}
}

func TestATombstoneIsAnEmptyValue(t *testing.T) {
	if !IsTombstone(nil) {
		t.Error("a nil value is a tombstone")
	}
	if !IsTombstone([]byte{}) {
		t.Error("an empty value is a tombstone")
	}
	if IsTombstone(EncodeLease(Lease{Member: mustMember(t, "node-1")})) {
		t.Error("a lease value is no tombstone")
	}
}
