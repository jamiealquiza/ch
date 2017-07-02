package ch_test

import (
	"testing"

	"github.com/jamiealquiza/ch"
)

func TestAddGetNode(t *testing.T) {
	r, _ := ch.New(&ch.Config{VNodes: 3})

	r.AddNode("node-a")
	node, _ := r.GetNode("key")

	if node != "node-a" {
		t.Errorf("Expected node-a, got %s", node)
	}
}

func TestRemoveNode(t *testing.T) {
	r, _ := ch.New(&ch.Config{VNodes: 3})

	r.AddNode("node-a")
	r.RemoveNode("node-a")
	_, err := r.GetNode("key")

	if err != ch.ErrRingEmpty {
		t.Errorf("Expected empty hash ring")
	}
}
