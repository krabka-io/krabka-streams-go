package coordination

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// StateTopic is the compacted internal topic that holds the coordination
// state of a cluster. Every record of one role goes to one partition, so the
// records of a role carry a total order. See [RolePartition].
//
// The topic needs cleanup.policy=compact. A client writes it with acks=all,
// and an operator should set min.insync.replicas to at least 2. The claim
// "durable in a quorum before the dispatch" rests on that topic
// configuration, and not on this package.
const StateTopic = "__coordination_state"

// The maximum length of the two names of the topic, in bytes. The bound is
// the bound Kafka puts on a topic name, so a role stays short enough to log
// and to compare.
const (
	// MaxRoleLen is the maximum length of a [Role], in bytes.
	MaxRoleLen = 249

	// MaxMemberIDLen is the maximum length of a [MemberID], in bytes.
	MaxMemberIDLen = 249
)

// recordVersion is the version that every key and every value of [StateTopic]
// carries.
const recordVersion int16 = 0

// The record part that a decoder failed on. Each one names the
// [FormatError.Part] field of the error the decoder returns.
const (
	keyPart               = "key"
	registrationValuePart = "registration value"
	leaseValuePart        = "lease value"
)

// The names that a rejected role and a rejected member id report.
const (
	roleField   = "role"
	memberField = "member id"
)

// RecordKind names the record of [StateTopic] that a key holds. The key
// carries the kind, so one topic holds both kinds and one compaction key
// covers both.
type RecordKind int16

const (
	// KindRegistration marks the record that one member writes to announce
	// that it is available. A registration carries no authority.
	KindRegistration RecordKind = 0

	// KindLease marks the record that names the member that holds a role now,
	// and the time the claim of that member ends.
	KindLease RecordKind = 1
)

// String names the kind.
func (k RecordKind) String() string {
	switch k {
	case KindRegistration:
		return "registration"
	case KindLease:
		return "lease"
	default:
		return fmt.Sprintf("RecordKind(%d)", int16(k))
	}
}

// Role is the validated name of one coordination role, such as the name of an
// active controller.
//
// A role becomes a Kafka transactional.id. The transaction coordinator mints
// one producer epoch for that id, so the role names the fenced group of
// members that compete for the same leadership.
//
// [NewRole] is the only way to build a role with a name, so a non-zero Role
// value is proof that the name is well formed.
type Role struct {
	name string
}

// NewRole parses a role name. It rejects an empty name and a name longer than
// [MaxRoleLen] bytes.
func NewRole(name string) (Role, error) {
	if err := checkName(name, roleField, MaxRoleLen); err != nil {
		return Role{}, err
	}
	return Role{name: name}, nil
}

// String returns the role name.
func (r Role) String() string { return r.name }

// MemberID is the validated identity of one member that competes for a role.
//
// A member picks its own id and keeps that id across a reconnect, so the
// succession order survives a short network failure.
//
// [NewMemberID] is the only way to build a member id with a name, so a
// non-zero MemberID value is proof that the id is well formed. The zero value
// names no member, and a lease key carries it.
type MemberID struct {
	name string
}

// NewMemberID parses a member id. It rejects an empty id and an id longer
// than [MaxMemberIDLen] bytes.
func NewMemberID(name string) (MemberID, error) {
	if err := checkName(name, memberField, MaxMemberIDLen); err != nil {
		return MemberID{}, err
	}
	return MemberID{name: name}, nil
}

// String returns the member id.
func (m MemberID) String() string { return m.name }

// checkName rejects an empty name and a name past bound.
func checkName(name string, field string, bound int) error {
	if name == "" {
		return fmt.Errorf("a coordination %s must not be empty", field)
	}
	if len(name) > bound {
		return fmt.Errorf("a coordination %s of %d bytes is longer than the %d-byte maximum",
			field, len(name), bound)
	}
	return nil
}

// FencingToken is the quorum-minted proof that one member holds a role.
//
// The transaction coordinator mints the pair when a member calls
// InitProducerId for the transactional id of the role. The quorum picks the
// values, the broker enforces them, and a write from an older pair fails with
// a fenced error. A holder passes the token to every guarded write.
//
// The pair is load-bearing. [FencingToken.Compare] reads the producer id
// first and the producer epoch second, so the order is lexicographic. A
// producer epoch is an int16 and it wraps. Kafka then allocates a new
// producer id and resets the epoch to zero. A comparison on the epoch alone
// would rank that fresh token below the stale one, and the old leader would
// keep the role. Never compare the epoch on its own.
type FencingToken struct {
	producerID    int64
	producerEpoch int16
}

// NoEpoch is the token of a role that no member has ever taken. Kafka writes
// -1 for "no producer", and that pair proves no leadership. NoEpoch ranks
// below every minted token, because [NewFencingToken] rejects a negative
// value. Treat NoEpoch as a constant.
var NoEpoch = FencingToken{producerID: -1, producerEpoch: -1}

// NewFencingToken builds a token from a minted producer id and producer
// epoch. It rejects a negative value in either position.
func NewFencingToken(producerID int64, producerEpoch int16) (FencingToken, error) {
	if producerID < 0 || producerEpoch < 0 {
		return FencingToken{}, fmt.Errorf(
			"a fencing token must not be negative, got %d:%d", producerID, producerEpoch)
	}
	return FencingToken{producerID: producerID, producerEpoch: producerEpoch}, nil
}

// ParseFencingToken parses the producer_id:producer_epoch form that
// [FencingToken.String] writes.
func ParseFencingToken(text string) (FencingToken, error) {
	malformed := fmt.Errorf(
		"expected a fencing token of the form producer_id:producer_epoch, got %q", text)
	producerID, producerEpoch, found := strings.Cut(text, ":")
	if !found || strings.Contains(producerEpoch, ":") {
		return FencingToken{}, malformed
	}
	id, err := strconv.ParseInt(producerID, 10, 64)
	if err != nil {
		return FencingToken{}, malformed
	}
	epoch, err := strconv.ParseInt(producerEpoch, 10, 16)
	if err != nil {
		return FencingToken{}, malformed
	}
	return NewFencingToken(id, int16(epoch))
}

// ProducerID returns the producer id that the transaction coordinator minted.
func (t FencingToken) ProducerID() int64 { return t.producerID }

// ProducerEpoch returns the producer epoch that the transaction coordinator
// minted.
func (t FencingToken) ProducerEpoch() int16 { return t.producerEpoch }

// Compare orders two tokens lexicographically. It reads the producer id
// first, and it reads the producer epoch only for two tokens of one producer
// id. It returns a negative number when t ranks below other, zero for two
// equal tokens, and a positive number when t supersedes other.
//
// The producer id leads, because Kafka allocates producer ids from a
// monotonic block allocator and resets the epoch to zero at every new id. The
// pair stays monotonic where the epoch alone does not.
func (t FencingToken) Compare(other FencingToken) int {
	if order := cmp.Compare(t.producerID, other.producerID); order != 0 {
		return order
	}
	return cmp.Compare(t.producerEpoch, other.producerEpoch)
}

// Supersedes reports whether t ranks above other under [FencingToken.Compare].
func (t FencingToken) Supersedes(other FencingToken) bool { return t.Compare(other) > 0 }

// String writes the producer_id:producer_epoch form.
func (t FencingToken) String() string {
	return strconv.FormatInt(t.producerID, 10) + ":" + strconv.FormatInt(int64(t.producerEpoch), 10)
}

// Key is the decoded key of one record of [StateTopic]. The key is the
// compaction key, so the topic keeps the last record of every role and every
// member.
type Key struct {
	// Kind names the record that the key holds.
	Kind RecordKind

	// Role is the role that the record belongs to.
	Role Role

	// Member is the member of a registration key. A lease key carries the
	// zero [MemberID], because a lease belongs to the role and not to one
	// member.
	Member MemberID
}

// RegistrationKey builds the key of the registration of one member of a role.
func RegistrationKey(role Role, member MemberID) Key {
	return Key{Kind: KindRegistration, Role: role, Member: member}
}

// LeaseKey builds the key of the lease of a role. It carries no member.
func LeaseKey(role Role) Key {
	return Key{Kind: KindLease, Role: role}
}

// Registration is the value of a record that announces one member of a role.
// A member writes a registration when it joins, and it writes a tombstone on
// the same key when it leaves.
type Registration struct {
	// Member is the member that registered.
	Member MemberID

	// RegisteredAt is the time the member registered, in milliseconds since
	// the Unix epoch.
	RegisteredAt int64
}

// Lease is the value of a record that names the member that holds a role now,
// and the time the claim of that member ends.
//
// Token is the authority. The broker fences a write that carries an older
// token, and no clock takes part in that check. Deadline is an anti-flap
// device. A standby waits for the deadline to pass before it challenges the
// holder, so a short pause does not move the role.
type Lease struct {
	// Member is the member that holds the role.
	Member MemberID

	// Token is the quorum-minted proof of the claim of the holder.
	Token FencingToken

	// GrantedAt is the time the holder took the lease, in milliseconds since
	// the Unix epoch. A renewal moves it forward.
	GrantedAt int64

	// Deadline is the time the lease expires, in milliseconds since the Unix
	// epoch.
	Deadline int64
}

// FormatError reports a malformed record of [StateTopic].
type FormatError struct {
	// Part names the record part that failed to decode. It is "key",
	// "registration value", or "lease value".
	Part string

	message string
}

// Error describes the fault and names the record part that carries it.
func (e *FormatError) Error() string { return e.message }

// IsTombstone reports whether a record value is a tombstone. A tombstone of a
// registration key deregisters one member, and a tombstone of a lease key
// clears the lease of one role. No value of this topic is legally empty, so
// an empty value and a nil value are both tombstones.
func IsTombstone(value []byte) bool { return len(value) == 0 }

// EncodeKey encodes a key of [StateTopic].
//
// The function writes the empty member string for a lease key, whatever
// key.Member holds, because the frozen layout gives a lease no member.
func EncodeKey(key Key) []byte {
	member := ""
	if key.Kind == KindRegistration {
		member = key.Member.name
	}
	buffer := make([]byte, 0, 8+len(key.Role.name)+len(member))
	buffer = appendInt16(buffer, recordVersion)
	buffer = appendInt16(buffer, int16(key.Kind))
	buffer = appendString(buffer, key.Role.name)
	return appendString(buffer, member)
}

// EncodeRegistration encodes the value of a registration record.
func EncodeRegistration(registration Registration) []byte {
	buffer := make([]byte, 0, 12+len(registration.Member.name))
	buffer = appendInt16(buffer, recordVersion)
	buffer = appendString(buffer, registration.Member.name)
	return appendInt64(buffer, registration.RegisteredAt)
}

// EncodeLease encodes the value of a lease record.
func EncodeLease(lease Lease) []byte {
	buffer := make([]byte, 0, 30+len(lease.Member.name))
	buffer = appendInt16(buffer, recordVersion)
	buffer = appendString(buffer, lease.Member.name)
	buffer = appendInt64(buffer, lease.Token.producerID)
	buffer = appendInt16(buffer, lease.Token.producerEpoch)
	buffer = appendInt64(buffer, lease.GrantedAt)
	return appendInt64(buffer, lease.Deadline)
}

// DecodeKey decodes a key of [StateTopic]. It returns a [*FormatError] for a
// truncated buffer, for trailing bytes, for an unknown version, for an
// unknown kind, for a negative string length, for a role or a member id that
// its constructor rejects, and for a lease key that names a member.
func DecodeKey(data []byte) (Key, error) {
	reader := &stateReader{data: data, part: keyPart}
	if err := reader.version(); err != nil {
		return Key{}, err
	}
	code, err := reader.int16()
	if err != nil {
		return Key{}, err
	}
	kind := RecordKind(code)
	if kind != KindRegistration && kind != KindLease {
		return Key{}, reader.errorf("unknown coordination state record kind %d", code)
	}
	roleName, err := reader.string()
	if err != nil {
		return Key{}, err
	}
	memberName, err := reader.string()
	if err != nil {
		return Key{}, err
	}
	if !reader.empty() {
		return Key{}, reader.errorf("trailing bytes in coordination state %s", reader.part)
	}
	role, err := NewRole(roleName)
	if err != nil {
		return Key{}, reader.wrap(err)
	}
	if kind == KindLease {
		if memberName != "" {
			return Key{}, reader.errorf(
				"a lease key carries the empty member string, got %q", memberName)
		}
		return LeaseKey(role), nil
	}
	member, err := NewMemberID(memberName)
	if err != nil {
		return Key{}, reader.wrap(err)
	}
	return RegistrationKey(role, member), nil
}

// DecodeRegistration decodes the value of a registration record. It returns a
// [*FormatError] for a truncated buffer, for trailing bytes, for an unknown
// version, for a negative string length, and for a member id that its
// constructor rejects.
func DecodeRegistration(data []byte) (Registration, error) {
	reader := &stateReader{data: data, part: registrationValuePart}
	if err := reader.version(); err != nil {
		return Registration{}, err
	}
	memberName, err := reader.string()
	if err != nil {
		return Registration{}, err
	}
	registeredAt, err := reader.int64()
	if err != nil {
		return Registration{}, err
	}
	if !reader.empty() {
		return Registration{}, reader.errorf(
			"trailing bytes in coordination state %s", reader.part)
	}
	member, err := NewMemberID(memberName)
	if err != nil {
		return Registration{}, reader.wrap(err)
	}
	return Registration{Member: member, RegisteredAt: registeredAt}, nil
}

// DecodeLease decodes the value of a lease record. It returns a
// [*FormatError] for a truncated buffer, for trailing bytes, for an unknown
// version, for a negative string length, for a member id that its constructor
// rejects, and for a negative producer id or producer epoch.
func DecodeLease(data []byte) (Lease, error) {
	reader := &stateReader{data: data, part: leaseValuePart}
	if err := reader.version(); err != nil {
		return Lease{}, err
	}
	memberName, err := reader.string()
	if err != nil {
		return Lease{}, err
	}
	producerID, err := reader.int64()
	if err != nil {
		return Lease{}, err
	}
	producerEpoch, err := reader.int16()
	if err != nil {
		return Lease{}, err
	}
	grantedAt, err := reader.int64()
	if err != nil {
		return Lease{}, err
	}
	deadline, err := reader.int64()
	if err != nil {
		return Lease{}, err
	}
	if !reader.empty() {
		return Lease{}, reader.errorf("trailing bytes in coordination state %s", reader.part)
	}
	member, err := NewMemberID(memberName)
	if err != nil {
		return Lease{}, reader.wrap(err)
	}
	token, err := NewFencingToken(producerID, producerEpoch)
	if err != nil {
		return Lease{}, reader.wrap(err)
	}
	return Lease{Member: member, Token: token, GrantedAt: grantedAt, Deadline: deadline}, nil
}

// appendString writes one string as an int16 byte length and then UTF-8
// bytes. [NewRole] and [NewMemberID] bound a name at 249 bytes, so the length
// always fits.
func appendString(buffer []byte, value string) []byte {
	buffer = appendInt16(buffer, int16(len(value)))
	return append(buffer, value...)
}

func appendInt16(buffer []byte, value int16) []byte {
	return binary.BigEndian.AppendUint16(buffer, uint16(value))
}

func appendInt64(buffer []byte, value int64) []byte {
	return binary.BigEndian.AppendUint64(buffer, uint64(value))
}

// stateReader reads the big-endian layout of [StateTopic]. Every integer is
// signed, and a string is an int16 byte length and then UTF-8 bytes. The
// reader sizes no allocation from a length that it read, and it takes a
// subslice of its input instead.
type stateReader struct {
	data []byte
	part string
}

func (r *stateReader) empty() bool { return len(r.data) == 0 }

func (r *stateReader) errorf(format string, args ...any) error {
	return &FormatError{Part: r.part, message: fmt.Sprintf(format, args...)}
}

// wrap names the record part on an error that a constructor returned.
func (r *stateReader) wrap(err error) error {
	return r.errorf("malformed coordination state %s: %s", r.part, err)
}

func (r *stateReader) truncated() error {
	return r.errorf("truncated coordination state %s", r.part)
}

func (r *stateReader) int16() (int16, error) {
	if len(r.data) < 2 {
		return 0, r.truncated()
	}
	result := int16(binary.BigEndian.Uint16(r.data))
	r.data = r.data[2:]
	return result, nil
}

func (r *stateReader) int64() (int64, error) {
	if len(r.data) < 8 {
		return 0, r.truncated()
	}
	result := int64(binary.BigEndian.Uint64(r.data))
	r.data = r.data[8:]
	return result, nil
}

// string reads one string. A negative length is malformed, and the int16
// length bounds the read at 32767 bytes. The layout is Kafka's own native
// string layout. It is not Java's modified UTF-8, which DataOutput.writeUTF
// writes.
func (r *stateReader) string() (string, error) {
	length, err := r.int16()
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", r.errorf("negative string length %d in coordination state %s", length, r.part)
	}
	if len(r.data) < int(length) {
		return "", r.truncated()
	}
	result := string(r.data[:length])
	r.data = r.data[length:]
	if !utf8.ValidString(result) {
		return "", r.errorf("non-UTF-8 string in coordination state %s", r.part)
	}
	return result, nil
}

// version reads the version and rejects every version but [recordVersion].
func (r *stateReader) version() error {
	version, err := r.int16()
	if err != nil {
		return err
	}
	if version != recordVersion {
		return r.errorf("unsupported coordination state %s version %d", r.part, version)
	}
	return nil
}
