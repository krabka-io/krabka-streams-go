package coordination

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
)

// The record layouts of __coordination_state are frozen. krabka-client-rs and
// krabka-streams-java assert the same arrays. Copy an array into a port with
// no change. A change here is a change of the wire format, and it needs the
// same change in the two ports.
//
// The fixture is role "controller", member "node-1", registeredAt and
// grantedAt 1700000000000, deadline 1700000030000, and token 4242:7.
const (
	goldenRegistrationKey   = "00 00 00 00 00 0A 63 6F 6E 74 72 6F 6C 6C 65 72 00 06 6E 6F 64 65 2D 31"
	goldenLeaseKey          = "00 00 00 01 00 0A 63 6F 6E 74 72 6F 6C 6C 65 72 00 00"
	goldenRegistrationValue = "00 00 00 06 6E 6F 64 65 2D 31 00 00 01 8B CF E5 68 00"
	goldenLeaseValue        = "00 00 00 06 6E 6F 64 65 2D 31 00 00 00 00 00 00 10 92 00 07 " +
		"00 00 01 8B CF E5 68 00 00 00 01 8B CF E5 DD 30"
)

const (
	goldenRole         = "controller"
	goldenMember       = "node-1"
	goldenRegisteredAt = 1700000000000
	goldenGrantedAt    = 1700000000000
	goldenDeadline     = 1700000030000
	goldenProducerID   = 4242
	goldenEpoch        = 7
)

// goldenBytes parses one spaced hexadecimal vector.
func goldenBytes(t *testing.T, vector string) []byte {
	t.Helper()
	data, err := hex.DecodeString(strings.ReplaceAll(vector, " ", ""))
	if err != nil {
		t.Fatalf("parse the golden vector %q: %v", vector, err)
	}
	return data
}

func goldenRegistrationRecord(t *testing.T) Registration {
	t.Helper()
	return Registration{Member: mustMember(t, goldenMember), RegisteredAt: goldenRegisteredAt}
}

func goldenLeaseRecord(t *testing.T) Lease {
	t.Helper()
	return Lease{
		Member:    mustMember(t, goldenMember),
		Token:     mustToken(t, goldenProducerID, goldenEpoch),
		GrantedAt: goldenGrantedAt,
		Deadline:  goldenDeadline,
	}
}

func TestTheEncoderWritesTheFrozenGoldenBytes(t *testing.T) {
	cases := []struct {
		name   string
		vector string
		encode func(*testing.T) []byte
	}{
		{
			name:   "registration key",
			vector: goldenRegistrationKey,
			encode: func(t *testing.T) []byte {
				return EncodeKey(RegistrationKey(mustRole(t, goldenRole), mustMember(t, goldenMember)))
			},
		},
		{
			name:   "lease key",
			vector: goldenLeaseKey,
			encode: func(t *testing.T) []byte {
				return EncodeKey(LeaseKey(mustRole(t, goldenRole)))
			},
		},
		{
			name:   "registration value",
			vector: goldenRegistrationValue,
			encode: func(t *testing.T) []byte {
				return EncodeRegistration(goldenRegistrationRecord(t))
			},
		},
		{
			name:   "lease value",
			vector: goldenLeaseValue,
			encode: func(t *testing.T) []byte {
				return EncodeLease(goldenLeaseRecord(t))
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			want := goldenBytes(t, testCase.vector)
			got := testCase.encode(t)
			if !bytes.Equal(got, want) {
				t.Fatalf("the %s encodes to %s, and the frozen vector is %s",
					testCase.name, hex.EncodeToString(got), hex.EncodeToString(want))
			}
			if len(got) != len(want) {
				t.Fatalf("the %s is %d bytes, and the frozen vector is %d bytes",
					testCase.name, len(got), len(want))
			}
		})
	}
}

func TestTheDecoderReadsTheFrozenGoldenBytes(t *testing.T) {
	role := mustRole(t, goldenRole)
	member := mustMember(t, goldenMember)

	registrationKey, err := DecodeKey(goldenBytes(t, goldenRegistrationKey))
	if err != nil {
		t.Fatalf("decode the golden registration key: %v", err)
	}
	if want := RegistrationKey(role, member); !reflect.DeepEqual(registrationKey, want) {
		t.Errorf("the golden registration key decodes to %+v, and the fixture is %+v",
			registrationKey, want)
	}

	leaseKey, err := DecodeKey(goldenBytes(t, goldenLeaseKey))
	if err != nil {
		t.Fatalf("decode the golden lease key: %v", err)
	}
	if want := LeaseKey(role); !reflect.DeepEqual(leaseKey, want) {
		t.Errorf("the golden lease key decodes to %+v, and the fixture is %+v", leaseKey, want)
	}

	registration, err := DecodeRegistration(goldenBytes(t, goldenRegistrationValue))
	if err != nil {
		t.Fatalf("decode the golden registration value: %v", err)
	}
	if want := goldenRegistrationRecord(t); !reflect.DeepEqual(registration, want) {
		t.Errorf("the golden registration value decodes to %+v, and the fixture is %+v",
			registration, want)
	}

	lease, err := DecodeLease(goldenBytes(t, goldenLeaseValue))
	if err != nil {
		t.Fatalf("decode the golden lease value: %v", err)
	}
	if want := goldenLeaseRecord(t); !reflect.DeepEqual(lease, want) {
		t.Errorf("the golden lease value decodes to %+v, and the fixture is %+v", lease, want)
	}
}

func TestTheRolePartitionMatchesTheFrozenValues(t *testing.T) {
	cases := []struct {
		role       string
		partitions int
		want       int
	}{
		{role: "controller", partitions: 16, want: 12},
		{role: "dispatcher", partitions: 16, want: 10},
		{role: "role-a", partitions: 16, want: 10},
		{role: "role-b", partitions: 16, want: 12},
		{role: "controller", partitions: 1, want: 0},
	}

	for _, testCase := range cases {
		got, err := RolePartition(mustRole(t, testCase.role), testCase.partitions)
		if err != nil {
			t.Fatalf("RolePartition(%q, %d): %v", testCase.role, testCase.partitions, err)
		}
		if got != testCase.want {
			t.Errorf("RolePartition(%q, %d) = %d, and the frozen value is %d",
				testCase.role, testCase.partitions, got, testCase.want)
		}
	}
}

// The last four cases are the vectors of Apache Kafka's own UtilsTest for
// Utils.murmur2. The keys cover every tail length of the block loop: "abcd"
// and "a-little-bit-long-string" leave no tail, "a" and "kafka" leave one
// byte, "my-key" leaves two, and "abc" leaves three.
func TestTheKeyHashMatchesKafkaMurmur2(t *testing.T) {
	cases := []struct {
		key  string
		want int32
	}{
		{key: "", want: 275646681},
		{key: "a", want: -1563381124},
		{key: "abcd", want: -1323649548},
		{key: "kafka", want: -798503068},
		{key: "my-key", want: 1748425209},
		{key: "21", want: -973932308},
		{key: "abc", want: 479470107},
		{key: "foobar", want: -790332482},
		{key: "a-little-bit-long-string", want: -985981536},
		{key: "a-little-bit-longer-string", want: -1486304829},
	}

	for _, testCase := range cases {
		if got := murmur2([]byte(testCase.key)); got != testCase.want {
			t.Errorf("murmur2(%q) = %d, and Kafka answers %d", testCase.key, got, testCase.want)
		}
	}
}

// The mask clears the sign bit, and it is not an absolute value. The two
// differ for every negative hash, so a port that takes the absolute value
// picks another partition for most roles.
func TestTheMaskClearsTheSignBitAndDoesNotTakeTheAbsoluteValue(t *testing.T) {
	role := mustRole(t, "role-a")
	hash := murmur2([]byte(role.String()))
	if hash >= 0 {
		t.Fatalf("this fixture needs a role with a negative hash, and %q hashes to %d",
			role, hash)
	}

	got, err := RolePartition(role, 16)
	if err != nil {
		t.Fatalf("RolePartition: %v", err)
	}
	absolute := int(-hash) % 16
	if got == absolute {
		t.Fatalf("the mask and the absolute value both answer %d, so this fixture proves nothing",
			got)
	}
	if want := int(hash&0x7fffffff) % 16; got != want {
		t.Errorf("RolePartition(%q, 16) = %d, and the mask answers %d", role, got, want)
	}
}

func TestRolePartitionRejectsAPartitionCountBelowOne(t *testing.T) {
	if _, err := RolePartition(mustRole(t, "controller"), 0); err == nil {
		t.Fatal("RolePartition took a partition count of zero, and it must reject one")
	}
}
