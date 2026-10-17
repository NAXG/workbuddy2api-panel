// device.go 单设备（host）身份档案：一台机器上跑多个账号时，所有账号共享同一套
// machineId / OS / 硬件指纹，只有账号级字段（userId / conversation / token）不同。
//
// 对齐 WorkBuddy Desktop 5.8.3 的真实实现：
//   - machineId 来自宿主硬件 UUID（macOS IOPlatformUUID / Linux dbus machine-id /
//     Windows MachineGuid），不是按账号哈希；
//   - sessionId 是进程级 randomUUID（每次启动换新，等同客户端一次启动）；
//   - OS / arch / osVersion / cpuCores / memorySize 是宿主真实值。
//
// 数据文件默认 data/device.json（与 state_file 同目录），Docker 部署时随 data/ 挂载；
// 文件缺失时生成一份仅含随机 machineId 的兜底档案并原子落盘，保证跨重启稳定。
// 文件内不要写 session_id：它应由每次进程启动重新生成。
package upstream

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DeviceProfile 一台设备的稳定指纹。JSON 字段与 data/device.json 对应。
// 零值字段在 normalize 中补齐；SessionID 不落盘，启动时随机生成。
type DeviceProfile struct {
	MachineID string `json:"machine_id"`
	SessionID string `json:"session_id,omitempty"`
	OS        string `json:"os,omitempty"`         // darwin / win32 / linux
	Arch      string `json:"arch,omitempty"`       // arm64 / x64
	OSVersion string `json:"os_version,omitempty"` // e.g. 27.0.1 / 10.0.26220
	CPUCores  int    `json:"cpu_cores,omitempty"`
	MemoryGB  int    `json:"memory_gb,omitempty"`
	Timezone  string `json:"timezone,omitempty"`
}

// NewUUID 生成标准 36 字符 UUID v4（带连字符）。WorkBuddy 5.8.3 的
// X-Request-ID / X-Conversation-Message-ID / machineId 均使用该形态。
func NewUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 熵源故障的极端兜底：仍返回合法 UUID 形态，不 panic。
		n := uint64(time.Now().UnixNano())
		for i := range b {
			b[i] = byte(n >> (8 * (i % 8)))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// defaultOSName 把 Go 的 GOOS 映射为 WorkBuddy 事件里的 os 值。
func defaultOSName(goos string) string {
	switch goos {
	case "windows":
		return "win32"
	case "darwin":
		return "darwin"
	case "linux":
		return "linux"
	default:
		return goos
	}
}

func localTimezone() string {
	if name := time.Now().Location().String(); name != "" && name != "Local" {
		return name
	}
	return "Asia/Shanghai"
}

// DefaultDeviceProfile 返回运行期兜底档案：随机 machineId/sessionId + 当前 Go
// 运行时的 OS/arch/核数。生产应优先通过 data/device.json 注入宿主真实值。
func DefaultDeviceProfile() *DeviceProfile {
	return &DeviceProfile{
		MachineID: NewUUID(),
		SessionID: NewUUID(),
		OS:        defaultOSName(runtime.GOOS),
		Arch:      runtime.GOARCH,
		CPUCores:  runtime.NumCPU(),
		Timezone:  localTimezone(),
	}
}

// normalize 补齐空字段；SessionID 为空时生成新的进程级 UUID。
func (p *DeviceProfile) normalize() {
	if p == nil {
		return
	}
	if strings.TrimSpace(p.MachineID) == "" {
		p.MachineID = NewUUID()
	}
	if strings.TrimSpace(p.SessionID) == "" {
		p.SessionID = NewUUID()
	}
	if strings.TrimSpace(p.OS) == "" {
		p.OS = defaultOSName(runtime.GOOS)
	}
	if strings.TrimSpace(p.Arch) == "" {
		p.Arch = runtime.GOARCH
	}
	if p.CPUCores <= 0 {
		p.CPUCores = runtime.NumCPU()
	}
	if p.MemoryGB < 0 {
		p.MemoryGB = 0
	}
	if strings.TrimSpace(p.Timezone) == "" {
		p.Timezone = localTimezone()
	}
}

// LoadDeviceProfile 读取 data/device.json 并补齐字段。文件不存在返回 os.ErrNotExist。
func LoadDeviceProfile(path string) (*DeviceProfile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p DeviceProfile
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("parse device profile: %w", err)
	}
	p.normalize()
	return &p, nil
}

// LoadOrCreateDeviceProfile 读取档案；文件不存在时生成兜底档案并原子落盘
// （不落 session_id）。用于 main 启动时注入 Client.DeviceProfile。
func LoadOrCreateDeviceProfile(path string) (*DeviceProfile, error) {
	p, err := LoadDeviceProfile(path)
	if err == nil {
		return p, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	p = DefaultDeviceProfile()
	p.OSVersion = detectOSVersion()
	p.MemoryGB = detectMemoryGB()
	// 落盘副本不写 session_id：每次进程启动都应重新生成。
	persisted := *p
	persisted.SessionID = ""
	if err := persisted.save(path); err != nil {
		return nil, err
	}
	return p, nil
}

// save 原子写入 JSON（0600）。session_id 为空时不写。
func (p *DeviceProfile) save(path string) error {
	if p == nil {
		return fmt.Errorf("nil device profile")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir device profile dir: %w", err)
		}
	}
	raw, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write device profile: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace device profile: %w", err)
	}
	return nil
}

// detectOSVersion 仅用于首次生成兜底档案；生产建议直接编辑 data/device.json。
func detectOSVersion() string {
	switch runtime.GOOS {
	case "darwin":
		if out, err := exec.Command("sw_vers", "-productVersion").Output(); err == nil {
			return strings.TrimSpace(string(out))
		}
	case "linux":
		if raw, err := os.ReadFile("/etc/os-release"); err == nil {
			for _, line := range strings.Split(string(raw), "\n") {
				if strings.HasPrefix(line, "VERSION_ID=") {
					return strings.Trim(strings.TrimPrefix(line, "VERSION_ID="), `"`)
				}
			}
		}
	}
	return ""
}

// detectMemoryGB 仅用于首次生成兜底档案；失败返回 0（字段省略）。
func detectMemoryGB() int {
	switch runtime.GOOS {
	case "darwin":
		if out, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			if n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); err == nil && n > 0 {
				return int(n >> 30)
			}
		}
	case "linux":
		if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
			for _, line := range strings.Split(string(raw), "\n") {
				if strings.HasPrefix(line, "MemTotal:") {
					fields := strings.Fields(line)
					if len(fields) >= 2 {
						if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil && kb > 0 {
							return int(kb >> 20)
						}
					}
				}
			}
		}
	}
	return 0
}

var (
	fallbackDeviceOnce sync.Once
	fallbackDevice     *DeviceProfile
)

// deviceProfile 返回生效档案；Client.DeviceProfile 为空时使用进程级兜底。
func (c *Client) deviceProfile() *DeviceProfile {
	if c != nil && c.DeviceProfile != nil {
		return c.DeviceProfile
	}
	fallbackDeviceOnce.Do(func() { fallbackDevice = DefaultDeviceProfile() })
	return fallbackDevice
}

func (c *Client) machineID() string { return c.deviceProfile().MachineID }
func (c *Client) sessionID() string { return c.deviceProfile().SessionID }
