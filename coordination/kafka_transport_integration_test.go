package coordination

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaTransportFencesAStaleLeaderAndKeepsSuccessionOrder(t *testing.T) {
	bootstrap := os.Getenv("KRABKA_INTEGRATION_BOOTSTRAP")
	if bootstrap == "" {
		t.Skip("KRABKA_INTEGRATION_BOOTSTRAP is unset")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	createCoordinationTopic(t, ctx, bootstrap)

	leases, _ := NewLeaseConfig(750*time.Millisecond, 200*time.Millisecond, 100*time.Millisecond)
	first, err := NewKafkaTransport(leases, bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewKafkaTransport(leases, bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	role, _ := NewRole("m19-go")
	memberOne, _ := NewMemberID("go-first")
	memberTwo, _ := NewMemberID("go-second")
	recovered, _ := NewMemberID("go-first-recovered")
	partition, _ := RoleTopicPartition(role)
	appendRegistration(t, ctx, first, partition, role, memberOne)
	if records, err := first.ReadPartition(ctx, partition); err != nil || len(records) == 0 {
		t.Fatalf("read registration: records=%d err=%v", len(records), err)
	}
	one, err := AcquireLeadership(ctx, first, role, memberOne, WithLeaseConfig(leases), WithPollInterval(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	newToken, err := second.AcquireEpoch(ctx, role)
	if err != nil {
		t.Fatal(err)
	}
	if !newToken.Supersedes(one.Token()) {
		t.Fatalf("new token %s does not supersede %s", newToken, one.Token())
	}
	select {
	case <-one.Done():
		if !errors.Is(one.Err(), ErrFenced) {
			t.Fatalf("stale leader ended with %v, want ErrFenced", one.Err())
		}
	case <-ctx.Done():
		t.Fatal("stale leader was not fenced")
	}

	appendRegistration(t, ctx, second, partition, role, memberTwo)
	appendRegistration(t, ctx, second, partition, role, recovered)
	state, err := DescribeRoleState(ctx, second, role)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(state.Roster))
	for i, entry := range state.Roster {
		got[i] = entry.Member.String()
	}
	want := []string{memberOne.String(), memberTwo.String(), recovered.String()}
	if len(got) != len(want) {
		t.Fatalf("roster = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("roster = %v, want %v", got, want)
		}
	}
}

func appendRegistration(t *testing.T, ctx context.Context, transport *KafkaTransport, partition TopicPartition, role Role, member MemberID) {
	t.Helper()
	err := transport.Append(ctx, partition, EncodeKey(RegistrationKey(role, member)), EncodeRegistration(Registration{
		Member: member, RegisteredAt: time.Now().UnixMilli(),
	}))
	if err != nil {
		t.Fatal(err)
	}
}

func createCoordinationTopic(t *testing.T, ctx context.Context, bootstrap string) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic: StateTopic, NumPartitions: DefaultPartitions, ReplicationFactor: 1,
	}}
	response, err := client.Request(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	created := response.(*kmsg.CreateTopicsResponse).Topics[0]
	if err := kerr.ErrorForCode(created.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		t.Fatalf("create %s: %v", StateTopic, err)
	}
}
