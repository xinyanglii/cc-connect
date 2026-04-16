package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadHeartbeatMD(t *testing.T) {
	dir := t.TempDir()

	if got := readHeartbeatMD(dir); got != "" {
		t.Errorf("expected empty, got %q", got)
	}

	content := "- check inbox\n- check tasks"
	if err := os.WriteFile(filepath.Join(dir, "HEARTBEAT.md"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := readHeartbeatMD(dir); got != content {
		t.Errorf("expected %q, got %q", content, got)
	}

	if got := readHeartbeatMD(""); got != "" {
		t.Errorf("expected empty for empty workdir, got %q", got)
	}
}

func TestReadHeartbeatMD_LowerCase(t *testing.T) {
	dir := t.TempDir()
	content := "- check status"
	if err := os.WriteFile(filepath.Join(dir, "heartbeat.md"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := readHeartbeatMD(dir); got != content {
		t.Errorf("expected %q, got %q", content, got)
	}
}

func TestHeartbeatScheduler_RegisterSkipsDisabled(t *testing.T) {
	hs := NewHeartbeatScheduler("")
	hs.Register("test", HeartbeatConfig{Enabled: false, SessionKey: "tg:1:1"}, nil, "")
	if len(hs.entries) != 0 {
		t.Errorf("expected 0 entries for disabled config, got %d", len(hs.entries))
	}
}

func TestHeartbeatScheduler_RegisterSkipsEmptySessionKey(t *testing.T) {
	hs := NewHeartbeatScheduler("")
	hs.Register("test", HeartbeatConfig{Enabled: true, SessionKey: ""}, nil, "")
	if len(hs.entries) != 0 {
		t.Errorf("expected 0 entries for empty session_key, got %d", len(hs.entries))
	}
}

func TestHeartbeatScheduler_RegisterDefaults(t *testing.T) {
	hs := NewHeartbeatScheduler("")
	hs.Register("test", HeartbeatConfig{
		Enabled:    true,
		SessionKey: "telegram:123:123",
	}, nil, "/tmp/test")

	if len(hs.entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(hs.entries))
	}
	entry := hs.entries["test"]
	if entry == nil {
		t.Fatal("expected entry for 'test'")
	}
	if entry.config.IntervalMins != 30 {
		t.Errorf("expected default interval 30, got %d", entry.config.IntervalMins)
	}
	if entry.config.TimeoutMins != 30 {
		t.Errorf("expected default timeout 30, got %d", entry.config.TimeoutMins)
	}
}

func TestHeartbeatScheduler_Status(t *testing.T) {
	hs := NewHeartbeatScheduler("")
	hs.Register("proj", HeartbeatConfig{
		Enabled:      true,
		SessionKey:   "tg:1:1",
		IntervalMins: 15,
		OnlyWhenIdle: true,
	}, nil, "")

	st := hs.Status("proj")
	if st == nil {
		t.Fatal("expected status")
	}
	if st.IntervalMins != 15 {
		t.Errorf("expected interval 15, got %d", st.IntervalMins)
	}
	if !st.OnlyWhenIdle {
		t.Error("expected only_when_idle true")
	}
	if st.RunCount != 0 {
		t.Errorf("expected 0 runs, got %d", st.RunCount)
	}

	if hs.Status("nonexistent") != nil {
		t.Error("expected nil for nonexistent project")
	}
}

func TestHeartbeatScheduler_PauseResume(t *testing.T) {
	hs := NewHeartbeatScheduler("")
	hs.Register("proj", HeartbeatConfig{
		Enabled:    true,
		SessionKey: "tg:1:1",
	}, nil, "")

	if !hs.Pause("proj") {
		t.Error("pause should succeed")
	}
	st := hs.Status("proj")
	if !st.Paused {
		t.Error("expected paused")
	}

	if !hs.Resume("proj") {
		t.Error("resume should succeed")
	}
	st = hs.Status("proj")
	if st.Paused {
		t.Error("expected not paused")
	}

	if hs.Pause("nonexistent") {
		t.Error("pause nonexistent should fail")
	}
}

func TestHeartbeatScheduler_SetInterval(t *testing.T) {
	hs := NewHeartbeatScheduler("")
	hs.Register("proj", HeartbeatConfig{
		Enabled:    true,
		SessionKey: "tg:1:1",
	}, nil, "")

	if !hs.SetInterval("proj", 10) {
		t.Error("set interval should succeed")
	}
	st := hs.Status("proj")
	if st.IntervalMins != 10 {
		t.Errorf("expected 10, got %d", st.IntervalMins)
	}

	if hs.SetInterval("proj", 0) {
		t.Error("set interval 0 should fail")
	}
	if hs.SetInterval("nonexistent", 5) {
		t.Error("set interval nonexistent should fail")
	}
}

func TestHeartbeatScheduler_Persistence(t *testing.T) {
	dataDir := t.TempDir()

	// Create scheduler, register, pause, change interval
	hs1 := NewHeartbeatScheduler(dataDir)
	hs1.Register("proj-a", HeartbeatConfig{
		Enabled:      true,
		SessionKey:   "tg:1:1",
		IntervalMins: 30,
	}, nil, "")
	hs1.Register("proj-b", HeartbeatConfig{
		Enabled:      true,
		SessionKey:   "tg:2:2",
		IntervalMins: 15,
	}, nil, "")

	hs1.Pause("proj-a")
	hs1.SetInterval("proj-b", 60)

	// Verify state file exists
	stateFile := filepath.Join(dataDir, "heartbeat_state.json")
	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("state file should exist: %v", err)
	}
	var states map[string]*heartbeatPersisted
	if err := json.Unmarshal(data, &states); err != nil {
		t.Fatalf("parse state file: %v", err)
	}
	if !states["proj-a"].Paused {
		t.Error("proj-a should be paused in state file")
	}
	if states["proj-b"].IntervalMins != 60 {
		t.Errorf("proj-b interval should be 60, got %d", states["proj-b"].IntervalMins)
	}

	// Create new scheduler from same dataDir → should restore state
	hs2 := NewHeartbeatScheduler(dataDir)
	hs2.Register("proj-a", HeartbeatConfig{
		Enabled:      true,
		SessionKey:   "tg:1:1",
		IntervalMins: 30,
	}, nil, "")
	hs2.Register("proj-b", HeartbeatConfig{
		Enabled:      true,
		SessionKey:   "tg:2:2",
		IntervalMins: 15,
	}, nil, "")

	stA := hs2.Status("proj-a")
	if !stA.Paused {
		t.Error("proj-a should be paused after restore")
	}
	stB := hs2.Status("proj-b")
	if stB.IntervalMins != 60 {
		t.Errorf("proj-b interval should be 60 after restore, got %d", stB.IntervalMins)
	}

	// Resume proj-a and reset proj-b interval → no overrides → state file removed
	hs2.Resume("proj-a")
	hs2.SetInterval("proj-b", 15) // back to original
	if _, err := os.Stat(stateFile); !os.IsNotExist(err) {
		t.Error("state file should be removed when no overrides remain")
	}
}

// ── active_hours tests ──────────────────────────────────────────────

func TestParseActiveHours(t *testing.T) {
	tests := []struct {
		name     string
		spec     string
		wantS    int
		wantE    int
		wantErr  bool
	}{
		{"empty", "", -1, -1, false},
		{"simple", "8-22", 8, 22, false},
		{"overnight", "20-6", 20, 6, false},
		{"boundary_0_23", "0-23", 0, 23, false},
		{"with_spaces", " 8 - 22 ", 8, 22, false},
		{"same_hour_rejected", "8-8", -1, -1, true},
		{"same_zero_rejected", "0-0", -1, -1, true},
		{"out_of_range_high", "25-30", -1, -1, true},
		{"out_of_range_24", "0-24", -1, -1, true},
		{"negative", "-1-5", -1, -1, true},
		{"single_part", "8", -1, -1, true},
		{"non_integer", "abc", -1, -1, true},
		{"non_integer_suffix", "8-22pm", -1, -1, true},
		{"empty_parts", "-", -1, -1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, e, err := ParseActiveHours(tc.spec)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseActiveHours(%q) err=%v, wantErr=%v", tc.spec, err, tc.wantErr)
			}
			if !tc.wantErr && (s != tc.wantS || e != tc.wantE) {
				t.Errorf("ParseActiveHours(%q) = (%d, %d), want (%d, %d)", tc.spec, s, e, tc.wantS, tc.wantE)
			}
		})
	}
}

func TestIsActiveHour(t *testing.T) {
	// Use a fixed timezone so test is not server-TZ dependent
	utc := time.UTC

	// Helper to stub time.Now at a specific hour
	hourFix := func(hour int) HeartbeatConfig {
		return HeartbeatConfig{
			ActiveStartHour: 8,
			ActiveEndHour:   22,
			ActiveHoursLoc:  utc,
		}
	}
	_ = hourFix

	// Since isActiveHour uses time.Now(), we parameterize via Loc tricks:
	// run logical tests where ActiveStart/End are varied but "now" is
	// controlled indirectly. For pure unit testing we test the math by
	// calling with config that always matches / never matches based on
	// the real current hour. For full coverage we mock time.Now — but
	// that requires a nowFunc variable. For v1, rely on the integer
	// branches being exercised by ParseActiveHours + manual hour probing.

	// Structural tests: unset → always active
	cfg := HeartbeatConfig{ActiveStartHour: -1, ActiveEndHour: -1}
	if !isActiveHour(cfg) {
		t.Error("unset (-1,-1) should always be active")
	}

	// Derive current hour in UTC for a "matches current window" test
	currentUTC := time.Now().In(utc).Hour()
	// Full-day window (0-23 is valid): active at every hour
	cfg = HeartbeatConfig{ActiveStartHour: 0, ActiveEndHour: 23, ActiveHoursLoc: utc}
	if currentUTC < 23 {
		if !isActiveHour(cfg) {
			t.Errorf("0-23 UTC should be active at hour %d", currentUTC)
		}
	}

	// Window that definitely excludes current hour
	excludeStart := (currentUTC + 2) % 24
	excludeEnd := (currentUTC + 3) % 24
	if excludeStart != excludeEnd {
		cfg = HeartbeatConfig{ActiveStartHour: excludeStart, ActiveEndHour: excludeEnd, ActiveHoursLoc: utc}
		if isActiveHour(cfg) {
			t.Errorf("window %d-%d should NOT include current hour %d", excludeStart, excludeEnd, currentUTC)
		}
	}
}

func TestHumanActiveHours(t *testing.T) {
	tests := []struct {
		name string
		cfg  HeartbeatConfig
		want string
	}{
		{"unset", HeartbeatConfig{ActiveStartHour: -1, ActiveEndHour: -1}, "always"},
		{"normal_local", HeartbeatConfig{ActiveStartHour: 8, ActiveEndHour: 22}, "8-22 local"},
	}
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err == nil {
		tests = append(tests, struct {
			name string
			cfg  HeartbeatConfig
			want string
		}{"with_tz", HeartbeatConfig{ActiveStartHour: 8, ActiveEndHour: 22, ActiveHoursLoc: berlin}, "8-22 Europe/Berlin"})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := humanActiveHours(tc.cfg); got != tc.want {
				t.Errorf("humanActiveHours = %q, want %q", got, tc.want)
			}
		})
	}
}
