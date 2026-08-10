package schema

import "testing"

func TestTopicNameStrategyAddsRoleSuffix(t *testing.T) {
	if TopicNameStrategy("orders", RoleKey) != "orders-key" {
		t.Fatal("unexpected key subject")
	}
	if TopicNameStrategy("orders", RoleValue) != "orders-value" {
		t.Fatal("unexpected value subject")
	}
}
