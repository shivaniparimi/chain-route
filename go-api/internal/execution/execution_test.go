package execution

import "testing"

func TestExecute_Deterministic(t *testing.T) {
	ids := []string{
		"c869364e-8d1c-4e54-bda8-0465dd935abc",
		"00000000-0000-0000-0000-000000000000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
		"11111111-1111-1111-1111-111111111111",
	}
	for _, id := range ids {
		first := Execute(id)
		second := Execute(id)
		if first != second {
			t.Fatalf("Execute(%q) is not deterministic: first=%+v second=%+v", id, first, second)
		}
	}
}

func TestExecute_BothOutcomesReachable(t *testing.T) {
	sawSuccess := false
	sawFailure := false
	for i := 0; i < 1000 && !(sawSuccess && sawFailure); i++ {
		id := randomLikeID(i)
		result := Execute(id)
		if result.Success {
			sawSuccess = true
		} else {
			sawFailure = true
			if result.Reason == "" {
				t.Fatalf("expected a non-empty Reason on failure for id %q", id)
			}
		}
	}
	if !sawSuccess {
		t.Fatal("expected at least one success outcome across 1000 ids")
	}
	if !sawFailure {
		t.Fatal("expected at least one failure outcome across 1000 ids")
	}
}

func randomLikeID(i int) string {
	return "test-execution-id-" + string(rune('a'+i%26)) + string(rune('0'+i%10)) + "-" + string(rune(i))
}
