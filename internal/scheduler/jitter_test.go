package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/pool"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
)

// withoutSlotJitter 关闭时点抖动，供仍按「整点」断言的回归用例使用。
func withoutSlotJitter(t *testing.T) {
	t.Helper()
	old := slotJitterWindow
	slotJitterWindow = 0
	t.Cleanup(func() { slotJitterWindow = old })
}

// TestSlotJitterStableAndBounded 同一 (日, 小时) 的抖动稳定，且落在窗口内。
func TestSlotJitterStableAndBounded(t *testing.T) {
	old := slotJitterWindow
	slotJitterWindow = 20 * time.Minute
	t.Cleanup(func() { slotJitterWindow = old })

	day := time.Date(2026, 9, 11, 0, 0, 0, 0, time.Local)
	for _, hour := range []int{0, 1, 9, 21, 23} {
		j := slotJitter(day, hour)
		if j < 0 || j >= 20*time.Minute {
			t.Fatalf("hour=%d jitter=%v 越界 [0,20m)", hour, j)
		}
		if again := slotJitter(day, hour); again != j {
			t.Fatalf("hour=%d jitter 不稳定: %v vs %v", hour, j, again)
		}
	}
}

// TestNextFireIncludesSlotJitter nextFire 返回的是整点 + 当天抖动，
// 而不是整点本身。
func TestNextFireIncludesSlotJitter(t *testing.T) {
	old := slotJitterWindow
	slotJitterWindow = 20 * time.Minute
	t.Cleanup(func() { slotJitterWindow = old })

	now := time.Date(2026, 9, 11, 8, 0, 0, 0, time.Local)
	want := slotOn(now, 9)
	got := nextFire(now, []int{9, 21})
	if !got.Equal(want) {
		t.Fatalf("nextFire=%v want %v（09:00+抖动）", got, want)
	}
	if got.Hour() != 9 {
		t.Fatalf("nextFire=%v 应落在 9 点窗口", got)
	}
}

// TestNextFireCatchesTodayWhenStartedBeforeJitteredSlot 进程在整点 00 秒启动时，
// 若当天抖动时点还没到，应跑今天而不是直接滚到明天。
func TestNextFireCatchesTodayWhenStartedBeforeJitteredSlot(t *testing.T) {
	old := slotJitterWindow
	slotJitterWindow = 20 * time.Minute
	t.Cleanup(func() { slotJitterWindow = old })

	day := time.Date(2026, 9, 11, 0, 0, 0, 0, time.Local)
	for slotJitter(day, 9) == 0 {
		day = day.AddDate(0, 0, 1)
	}
	now := time.Date(day.Year(), day.Month(), day.Day(), 9, 0, 0, 0, day.Location())
	got := nextFire(now, []int{9})
	if got.Day() != day.Day() || got.Hour() != 9 {
		t.Fatalf("nextFire=%v want 当天 09:xx（抖动时点尚未到达）", got)
	}
	if !got.After(now) {
		t.Fatalf("nextFire=%v 应晚于 now=%v", got, now)
	}
}

// TestPlanSlotsStableOrderedAndSpread 账号打散：确定、升序、不重复、
// 有最小间隔、跨天/跨任务族会变。
func TestPlanSlotsStableOrderedAndSpread(t *testing.T) {
	old := accountSpreadWindow
	accountSpreadWindow = 10 * time.Minute
	t.Cleanup(func() { accountSpreadWindow = old })

	slot := time.Date(2026, 9, 11, 9, 0, 0, 0, time.Local)
	uids := make([]string, 0, 20)
	for i := 1; i <= 20; i++ {
		uids = append(uids, fmt.Sprintf("u%02d", i))
	}
	a := planSlots(uids, slot, taskCheckin, true)
	b := planSlots(uids, slot, taskCheckin, true)
	if len(a) != len(uids) {
		t.Fatalf("len=%d want %d", len(a), len(uids))
	}
	seen := make(map[string]bool, len(uids))
	var prev time.Time
	sameAsInputOrder := true
	for i, ps := range a {
		if seen[ps.uid] {
			t.Fatalf("账号重复: %s", ps.uid)
		}
		seen[ps.uid] = true
		if ps.uid != uids[i] {
			sameAsInputOrder = false
		}
		if ps.at.Before(slot) {
			t.Fatalf("%s at=%v 早于时点 %v", ps.uid, ps.at, slot)
		}
		if ps.at.Sub(slot) > 10*time.Minute+time.Millisecond {
			t.Fatalf("%s at=%v 偏移超出打散窗口（>10m）", ps.uid, ps.at.Sub(slot))
		}
		if i > 0 {
			if ps.at.Before(prev) {
				t.Fatalf("顺序未按时间升序: %v < %v", ps.at, prev)
			}
			if ps.at.Sub(prev) < 5*time.Second {
				t.Fatalf("账号间隔过小: %v", ps.at.Sub(prev))
			}
		}
		prev = ps.at
		if a[i] != b[i] {
			t.Fatalf("同一输入两次 planSlots 不一致: %+v vs %+v", a[i], b[i])
		}
	}
	if sameAsInputOrder {
		t.Fatalf("顺序仍与 UID 字典序一致，未打散: %v", uids)
	}

	nextDay := slot.AddDate(0, 0, 1)
	c := planSlots(uids, nextDay, taskCheckin, true)
	diffDay := false
	for i := range a {
		if a[i].uid != c[i].uid || !a[i].at.Equal(c[i].at) {
			diffDay = true
		}
	}
	if !diffDay {
		t.Fatal("跨天后排程形态未变化")
	}

	d := planSlots(uids, slot, taskTravel, true)
	diffKind := false
	for i := range a {
		if a[i].uid != d[i].uid || !a[i].at.Equal(d[i].at) {
			diffKind = true
		}
	}
	if !diffKind {
		t.Fatal("不同任务族的账号排程形态相同")
	}
}

// TestPlanSlotsManualKeepsOrder 手动触发不打散：保持传入顺序、无等待时点。
func TestPlanSlotsManualKeepsOrder(t *testing.T) {
	uids := []string{"u3", "u1", "u2"}
	slot := time.Date(2026, 9, 11, 9, 0, 0, 0, time.Local)
	a := planSlots(uids, slot, taskCheckin, false)
	for i := range uids {
		if a[i].uid != uids[i] || !a[i].at.IsZero() {
			t.Fatalf("i=%d got %+v want uid=%s at=zero", i, a[i], uids[i])
		}
	}
}

// TestRunBatchKeepaliveSpreadStaggers 排程主循环的分发路径确实按计划时点
// 错开账号，而不是背靠背刷新。
func TestRunBatchKeepaliveSpreadStaggers(t *testing.T) {
	oldW, oldJ, oldG := accountSpreadWindow, slotJitterWindow, accountMinGap
	accountSpreadWindow = 120 * time.Millisecond
	slotJitterWindow = 0
	accountMinGap = 10 * time.Millisecond
	t.Cleanup(func() {
		accountSpreadWindow = oldW
		slotJitterWindow = oldJ
		accountMinGap = oldG
	})

	var mu sync.Mutex
	var at []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/token/refresh") {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		at = append(at, time.Now())
		mu.Unlock()
		w.Write([]byte(`{"code":0,"data":{"accessToken":"new","expiresIn":3600}}`))
	}))
	defer srv.Close()

	p := pool.New("")
	for _, uid := range []string{"u1", "u2", "u3"} {
		p.Add(&auth.Auth{UID: uid, AccessToken: "old", RefreshToken: "rt", ExpiresAt: 1})
	}
	up := &upstream.Client{HTTP: srv.Client(), ChatBaseCN: srv.URL, BillingBaseCN: srv.URL}
	s := New(Config{Pool: p, Upstream: up})

	s.runBatch(context.Background(), time.Now(), []taskKind{taskKeepalive})

	mu.Lock()
	defer mu.Unlock()
	if len(at) != 3 {
		t.Fatalf("refresh calls=%d want 3", len(at))
	}
	sort.Slice(at, func(i, j int) bool { return at[i].Before(at[j]) })
	if spread := at[2].Sub(at[0]); spread < 50*time.Millisecond || spread > 2*time.Second {
		t.Fatalf("account spread=%v want [50ms,2s]", spread)
	}
}

// TestRunKeepaliveSpreadCancelResponsive 排程等待可被 ctx 立即取消，
// 不会为了等某个账号的偏移而拖住优雅停机。
func TestRunKeepaliveSpreadCancelResponsive(t *testing.T) {
	old := accountSpreadWindow
	accountSpreadWindow = time.Hour
	t.Cleanup(func() { accountSpreadWindow = old })

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "old", RefreshToken: "rt", ExpiresAt: 1})
	up := &upstream.Client{}
	s := New(Config{Pool: p, Upstream: up})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	// 计划时点在未来 30 分钟：所有账号偏移都还没到，取消应立即返回。
	s.runKeepalive(ctx, time.Now().Add(30*time.Minute), true)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("取消响应过慢: %v", elapsed)
	}
}
