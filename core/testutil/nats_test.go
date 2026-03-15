package testutil_test

import (
	"testing"

	"github.com/ad-hok/agent-os/core/testutil"
)

func TestStartNATS(t *testing.T) {
	_, nc := testutil.StartNATS(t)
	if !nc.IsConnected() {
		t.Fatal("expected connection to be active")
	}
}
