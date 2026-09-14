package grpcclient

import "testing"

func TestDialAndClose(t *testing.T) {
	client, err := Dial("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer client.Close()

	if client.client == nil {
		t.Fatal("expected a non-nil generated client")
	}
}
