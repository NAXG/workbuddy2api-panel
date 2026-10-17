// jitter.go 调度流量形态：时点抖动 + 账号级打散。
//
// 背景：旧排程在整点 00 秒精确触发，并按 Pool.List() 的 UID 字典序以
// 800ms 固定间隔逐号执行——多账号部署的出站流量因此呈现「整点齐发、
// 顺序固定、间隔固定」的机器特征。本文件把这两层时间形态改成确定性
// 伪随机：
//
//   - 时点抖动：每个 (本地自然日, 小时) 在 [0, slotJitterWindow) 内取一个
//     稳定偏移，同一小时的所有任务族共享（保持同槽合并执行）；跨天/跨小时
//     重新取值。进程重启不改变当天已确定的时点。
//   - 账号打散：同一任务族内，每个账号按 (日期, 小时, 任务族, uid) 取一个
//     稳定偏移，排序后按偏移先后执行，顺序与 UID 字典序无关；账号间保留
//     最小间隔，避免哈希碰撞挤成一团。
//
// 所有偏移都由 FNV-1a 哈希确定性导出，不使用全局随机源，便于复现与测试；
// 手动触发（面板按钮）走 spread=false，不等待、不打散。
package scheduler

import (
	"hash/fnv"
	"sort"
	"strconv"
	"time"
)

// jitterSalt 域名分隔盐：升级抖动算法时改这里即可整体换一套形态，
// 不影响账号/签到等业务数据。
const jitterSalt = "wb2api-schedule-v1"

var (
	// slotJitterWindow 时点抖动窗口：任务不会在整点 00 秒齐发，而是在
	// 整点后 0–7 分钟的确定性偏移处触发。窗口保持 < 1 小时，保证相邻
	// 配置小时（如 [9,10]）不会因抖动而互相越过。
	slotJitterWindow = 7 * time.Minute

	// accountSpreadWindow 账号打散窗口：同一任务族内账号按确定性偏移
	// 分布在约 0–10 分钟内；账号越多间隔越密，避免几百个号时拖太久。
	accountSpreadWindow = 10 * time.Minute

	// accountMinGap 账号间最小间隔（避免哈希碰撞把几个号挤在同一秒）。
	// 取 800ms 与旧手动限速同口径：账号多时它只修碰撞，不主导整体节奏。
	// 测试可置小值以缩短用例耗时，与 travelAccountDelay 同口径。
	accountMinGap = 800 * time.Millisecond
)

// String 任务族名称。用于日志、测试与抖动哈希的稳定输入——改名会改变
// 已生成的排程形态，非必要不要动。
func (k taskKind) String() string {
	switch k {
	case taskCheckin:
		return "checkin"
	case taskTravel:
		return "travel"
	case taskActivity:
		return "activity"
	case taskKeepalive:
		return "keepalive"
	case taskBlackcat:
		return "blackcat"
	case taskGrowth:
		return "growth"
	default:
		return "unknown"
	}
}

// hash64 FNV-1a 64 位哈希；parts 之间插入 NUL 分隔，避免
// ("ab","c") 与 ("a","bc") 撞成同一输入。
func hash64(parts ...string) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

// slotJitter 返回某本地自然日、某小时时点的确定性抖动偏移。
// 同一 (day, hour) 在进程重启后仍得到同一个偏移；不同天/小时重新取值。
func slotJitter(day time.Time, hour int) time.Duration {
	w := slotJitterWindow
	if w <= 0 {
		return 0
	}
	h := hash64(jitterSalt, "slot", day.Format("2006-01-02"), strconv.Itoa(hour))
	return time.Duration(h % uint64(w))
}

// slotOn 返回 day 当天 hour 点的实际触发时刻（整点 + 抖动）。
func slotOn(day time.Time, hour int) time.Time {
	return time.Date(day.Year(), day.Month(), day.Day(), hour, 0, 0, 0, day.Location()).
		Add(slotJitter(day, hour))
}

// plannedSlot 一个账号在本批任务里的执行位置；at 为零值表示手动触发、
// 不等待（spread=false）。
type plannedSlot struct {
	uid string
	at  time.Time
}

// planSlots 为一组账号生成本批任务的执行顺序：
//
//   - spread=false（面板手动触发）：保持传入顺序、at 为零值，调用方不等待；
//   - spread=true：按 (日期, 小时, 任务族, uid) 的稳定哈希取偏移并升序排序，
//     目标间隔 window/账号数（封顶 2 分钟、保底 800ms），避免哈希碰撞把多个号
//     挤在同一秒；若顺延把尾巴推出窗口，则整体等比压缩回窗口内。
//
// 偏移只依赖账号与日期，不依赖账号在 Pool.List() 里的下标——增删账号不会
// 把其他账号的基准偏移整体平移（只有最小间隔修正可能受账号数影响）。
func planSlots(uids []string, slot time.Time, kind taskKind, spread bool) []plannedSlot {
	out := make([]plannedSlot, 0, len(uids))
	if !spread || slot.IsZero() || accountSpreadWindow <= 0 || len(uids) == 0 {
		for _, uid := range uids {
			out = append(out, plannedSlot{uid: uid})
		}
		return out
	}

	type raw struct {
		uid string
		off time.Duration
	}
	items := make([]raw, 0, len(uids))
	window := accountSpreadWindow
	for _, uid := range uids {
		h := hash64(jitterSalt, "account",
			slot.Format("2006-01-02"), strconv.Itoa(slot.Hour()), kind.String(), uid)
		items = append(items, raw{uid: uid, off: time.Duration(h % uint64(window))})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].off != items[j].off {
			return items[i].off < items[j].off
		}
		return items[i].uid < items[j].uid // 同偏移时顺序也稳定
	})

	gap := window / time.Duration(len(items))
	if gap > 2*time.Minute {
		gap = 2 * time.Minute
	}
	if gap < accountMinGap {
		gap = accountMinGap
	}
	prev := -gap
	for i := range items {
		if items[i].off < prev+gap {
			items[i].off = prev + gap
		}
		prev = items[i].off
	}
	// 账号多或哈希碰撞集中时，逐号顺延可能把尾巴推出窗口；按比例压回窗口内，
	// 保持顺序与相对间隔，确保整批不会拖过下一个配置时点。
	if prev > window {
		scale := float64(window) / float64(prev)
		for i := range items {
			items[i].off = time.Duration(float64(items[i].off) * scale)
		}
	}
	for _, it := range items {
		out = append(out, plannedSlot{uid: it.uid, at: slot.Add(it.off)})
	}
	return out
}
