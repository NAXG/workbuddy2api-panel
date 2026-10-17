package upstream

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

func testDeviceProfile() *DeviceProfile {
	return &DeviceProfile{
		MachineID: "11111111-2222-4333-8444-555555555555",
		SessionID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		OS:        "darwin",
		Arch:      "arm64",
		OSVersion: "27.0.1",
		CPUCores:  8,
		MemoryGB:  16,
		Timezone:  "Asia/Shanghai",
	}
}

// TestDeviceProfileSharedAcrossPaths 单设备模型：chat / billing / 桌面事件 / mp
// 事件必须使用同一组 machineId/sessionId，而不是按 uid 各派生一套。
func TestDeviceProfileSharedAcrossPaths(t *testing.T) {
	p := testDeviceProfile()
	c := &Client{DeviceProfile: p}
	a := &auth.Auth{AccessToken: "at", UID: "u1", Nickname: "n1"}

	chatReq := httptest.NewRequest(http.MethodPost, "https://example.com/v2/chat/completions", nil)
	c.ChatHeaders(chatReq, a, "", ChatMeta{ConversationID: "conv-1"})
	if got := chatReq.Header.Get("X-Machine-ID"); got != p.MachineID {
		t.Errorf("chat X-Machine-ID=%q want %q", got, p.MachineID)
	}
	if got := chatReq.Header.Get("X-Session-ID"); got != p.SessionID {
		t.Errorf("chat X-Session-ID=%q want %q", got, p.SessionID)
	}

	billingReq := httptest.NewRequest(http.MethodPost, "https://example.com/v2/report", nil)
	c.BillingHeaders(billingReq, a)
	if got := billingReq.Header.Get("X-Machine-ID"); got != p.MachineID {
		t.Errorf("billing X-Machine-ID=%q want shared", got)
	}
	if got := billingReq.Header.Get("X-Session-ID"); got != p.SessionID {
		t.Errorf("billing X-Session-ID=%q want shared", got)
	}

	fp := c.desktopFingerprint(a)
	if fp["machineId"] != p.MachineID || fp["sessionId"] != p.SessionID {
		t.Errorf("desktop ids=%v/%v want %v/%v", fp["machineId"], fp["sessionId"], p.MachineID, p.SessionID)
	}
	if fp["os"] != p.OS || fp["arch"] != p.Arch || fp["osVersion"] != p.OSVersion {
		t.Errorf("desktop os=%v/%v/%v want %v/%v/%v", fp["os"], fp["arch"], fp["osVersion"], p.OS, p.Arch, p.OSVersion)
	}
	if fp["cpuCores"] != p.CPUCores || fp["memorySize"] != p.MemoryGB {
		t.Errorf("desktop hardware=%v/%v want %v/%v", fp["cpuCores"], fp["memorySize"], p.CPUCores, p.MemoryGB)
	}

	mp := c.mpEventBase(a)
	if mp["machineId"] != p.MachineID {
		t.Errorf("mp machineId=%v want shared %v", mp["machineId"], p.MachineID)
	}
	if mp["os"] != p.OS || mp["arch"] != p.Arch {
		t.Errorf("mp os=%v/%v want %v/%v", mp["os"], mp["arch"], p.OS, p.Arch)
	}
}

// TestConversationHeadersAlignWorkBuddy 对齐 WorkBuddy 5.8.3 applyGatewayHeaders：
// 消息/请求 ID 是 UUID，traceparent/b3/X-B3-* 同源，X-Product=SaaS、
// X-Agent-Intent=craft、X-Private-Data=true。
func TestConversationHeadersAlignWorkBuddy(t *testing.T) {
	c := &Client{DeviceProfile: testDeviceProfile()}
	a := &auth.Auth{AccessToken: "at", UID: "u1"}
	req := httptest.NewRequest(http.MethodPost, "https://example.com/v2/chat/completions", nil)
	c.ChatHeaders(req, a, "", ChatMeta{
		ConversationID:        "conv-1",
		ConversationRequestID: "0123456789abcdef0123456789abcdef",
	})

	for _, h := range []string{"X-Request-ID", "X-Conversation-Message-ID"} {
		got := req.Header.Get(h)
		if len(got) != 36 || strings.Count(got, "-") != 4 {
			t.Errorf("%s=%q want UUID (36 chars, 4 hyphens)", h, got)
		}
	}
	trace := req.Header.Get("X-B3-TraceId")
	span := req.Header.Get("X-B3-SpanId")
	parent := req.Header.Get("X-B3-ParentSpanId")
	if len(trace) != 32 || len(span) != 16 || len(parent) != 16 {
		t.Errorf("B3 lengths trace=%d span=%d parent=%d", len(trace), len(span), len(parent))
	}
	if got := req.Header.Get("traceparent"); got != "00-"+trace+"-"+span+"-01" {
		t.Errorf("traceparent=%q want 00-%s-%s-01", got, trace, span)
	}
	if got := req.Header.Get("b3"); got != trace+"-"+span+"-1-"+parent {
		t.Errorf("b3=%q want %s-%s-1-%s", got, trace, span, parent)
	}
	if got := req.Header.Get("X-Product"); got != "SaaS" {
		t.Errorf("X-Product=%q want SaaS", got)
	}
	if got := req.Header.Get("X-Agent-Intent"); got != "craft" {
		t.Errorf("X-Agent-Intent=%q want craft", got)
	}
	if got := req.Header.Get("X-Private-Data"); got != "true" {
		t.Errorf("X-Private-Data=%q want true", got)
	}
	if got := req.Header.Get("X-IDE-Version"); got != defaultClientVersion {
		t.Errorf("X-IDE-Version=%q want %s", got, defaultClientVersion)
	}
	if got := req.Header.Get("Accept-Encoding"); got != "identity" {
		t.Errorf("Accept-Encoding=%q want identity", got)
	}
}

// TestDeviceProfileFileStable machineId 跨进程稳定；session_id 不落盘。
func TestDeviceProfileFileStable(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/device.json"
	p1, err := LoadOrCreateDeviceProfile(path)
	if err != nil {
		t.Fatalf("load or create: %v", err)
	}
	if p1.MachineID == "" || p1.SessionID == "" {
		t.Fatalf("profile ids empty: %+v", p1)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read profile: %v", err)
	}
	if strings.Contains(string(raw), "session_id") {
		t.Errorf("device.json must not persist session_id: %s", raw)
	}
	p2, err := LoadDeviceProfile(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if p2.MachineID != p1.MachineID {
		t.Errorf("machineId changed across reload: %q -> %q", p1.MachineID, p2.MachineID)
	}
	if p2.SessionID == "" {
		t.Error("reloaded sessionId empty")
	}
}

// TestSchoolAndMPRequestsUseWorkBuddyUA 两条此前漏设 UA 的路径不能再发
// Go-http-client/1.1。
func TestSchoolAndMPRequestsUseWorkBuddyUA(t *testing.T) {
	var uas []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uas = append(uas, r.Header.Get("User-Agent"))
		w.Write([]byte(`{"code":0,"msg":"","data":{}}`))
	}))
	defer srv.Close()

	c := &Client{
		HTTP:          srv.Client(),
		BillingBaseCN: srv.URL,
		DeviceProfile: testDeviceProfile(),
	}
	a := &auth.Auth{AccessToken: "at", UID: "u1"}
	if err := c.schoolJSON(a, http.MethodGet, "/vouchers", nil, nil); err != nil {
		t.Fatalf("schoolJSON: %v", err)
	}
	if err := c.ReportMPEvent(a, map[string]any{"eventCode": "chat_request_send"}); err != nil {
		t.Fatalf("ReportMPEvent: %v", err)
	}
	for i, ua := range uas {
		if ua != c.userAgent(a) {
			t.Errorf("request %d UA=%q want %q", i, ua, c.userAgent(a))
		}
		if strings.HasPrefix(ua, "Go-http-client") {
			t.Errorf("request %d leaked Go default UA", i)
		}
	}
}
