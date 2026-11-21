package banner

import (
	"fmt"
	"io"
	"strings"
	"unicode"
)

const (
	ansiGreen       = "\033[32m"
	ansiBrightGreen = "\033[92m"
	ansiReset       = "\033[0m"
)

const asciiLogo = `  █████╗ ██╗   ██╗████████╗ ██████╗  ██████╗██╗      █████╗ ██╗    ██╗    ██████╗  █████╗ ██████╗ ██╗
 ██╔══██╗██║   ██║╚══██╔══╝██╔═══██╗██╔════╝██║     ██╔══██╗██║    ██║    ╚════██╗██╔══██╗██╔══██╗██║
 ███████║██║   ██║   ██║   ██║   ██║██║     ██║     ███████║██║ █╗ ██║     █████╔╝███████║██████╔╝██║
 ██╔══██║██║   ██║   ██║   ██║   ██║██║     ██║     ██╔══██║██║███╗██║    ██╔═══╝ ██╔══██║██╔═══╝ ██║
 ██║  ██║╚██████╔╝   ██║   ╚██████╔╝╚██████╗███████╗██║  ██║╚███╔███╔╝    ███████╗██║  ██║██║     ██║
 ╚═╝  ╚═╝ ╚═════╝    ╚═╝    ╚═════╝  ╚═════╝╚══════╝╚═╝  ╚═╝ ╚══╝╚══╝     ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝`

// Info 描述启动横幅中展示的动态运行状态。
type Info struct {
	ListenAddr      string
	Port            string
	ReadyTotal      int
	ReadyCN         int
	ReadyGlobal     int
	CheckinEnabled  bool
	CheckinTime     string
	CheckinWinStart string
	CheckinWinEnd   string
	Version         string
}

// Render 渲染完整的绿色 ANSI 启动横幅文本。
func Render(info Info) string {
	port := strings.TrimSpace(info.Port)
	if port == "" {
		port = "8080"
	}
	ver := strings.TrimSpace(info.Version)
	if ver == "" {
		ver = "v1.0.0"
	}

	checkinText := "未开启"
	if info.CheckinEnabled {
		ws := strings.TrimSpace(info.CheckinWinStart)
		we := strings.TrimSpace(info.CheckinWinEnd)
		if ws != "" && we != "" {
			checkinText = fmt.Sprintf("已开启（每日随机 %s–%s）", ws, we)
		} else {
			t := strings.TrimSpace(info.CheckinTime)
			if t == "" {
				t = "08:30"
			}
			checkinText = fmt.Sprintf("已开启（每日 %s）", t)
		}
	}

	rows := [][2]string{
		{"运行模式：", "双区多账号轮询池（CN + Global）"},
		{"管理控制台：", fmt.Sprintf("http://localhost:%s", port)},
		{"OpenAI 接口：", fmt.Sprintf("http://localhost:%s/v1/chat/completions", port)},
		{"Anthropic：", fmt.Sprintf("http://localhost:%s/v1/messages", port)},
		{"就绪账号：", fmt.Sprintf("%d 个（国内 %d / 国际 %d）", info.ReadyTotal, info.ReadyCN, info.ReadyGlobal)},
		{"自动签到：", checkinText},
		{"监听地址：", info.ListenAddr},
	}

	// 计算内容列宽（最小 70 列，保证与规范样式一致且不截断长地址）
	const labelColWidth = 14
	innerWidth := 70
	formatted := make([]string, 0, len(rows))
	for _, r := range rows {
		line := "  " + padRightDisplay(r[0], labelColWidth) + r[1]
		if w := displayWidth(line) + 2; w > innerWidth {
			innerWidth = w
		}
		formatted = append(formatted, line)
	}

	var b strings.Builder
	b.WriteString("\n")
	b.WriteString(ansiBrightGreen)
	b.WriteString(asciiLogo)
	b.WriteString("\n\n")
	b.WriteString(ansiGreen)
	b.WriteString("┌")
	b.WriteString(strings.Repeat("─", innerWidth))
	b.WriteString("┐\n")
	for _, line := range formatted {
		b.WriteString("│")
		b.WriteString(padRightDisplay(line, innerWidth))
		b.WriteString("│\n")
	}
	b.WriteString("└")
	b.WriteString(strings.Repeat("─", innerWidth))
	b.WriteString("┘\n\n")
	b.WriteString(ansiBrightGreen)
	b.WriteString(fmt.Sprintf("[ 系统状态 ] 已成功启动 | 开发者：VILK | 版本：%s", ver))
	b.WriteString(ansiReset)
	b.WriteString("\n")
	return b.String()
}

// Print 开启终端 ANSI 支持并将启动横幅打印至目标输出流。
func Print(w io.Writer, info Info) {
	EnableANSI()
	_, _ = io.WriteString(w, Render(info))
}

// displayWidth 估算字符串在等宽终端中的显示列数（中日韩全角字符按 2 列计算）。
func displayWidth(s string) int {
	width := 0
	for _, r := range s {
		if isWideRune(r) {
			width += 2
		} else {
			width++
		}
	}
	return width
}

func padRightDisplay(s string, targetWidth int) string {
	w := displayWidth(s)
	if w >= targetWidth {
		return s
	}
	return s + strings.Repeat(" ", targetWidth-w)
}

func isWideRune(r rune) bool {
	if r < 0x1100 {
		return false
	}
	if unicode.Is(unicode.Han, r) || unicode.Is(unicode.Hiragana, r) || unicode.Is(unicode.Katakana, r) || unicode.Is(unicode.Hangul, r) {
		return true
	}
	switch {
	case r >= 0x3000 && r <= 0x303F: // CJK 标点
		return true
	case r >= 0xFF00 && r <= 0xFF60: // 全角 ASCII 与标点
		return true
	case r >= 0xFFE0 && r <= 0xFFE6:
		return true
	}
	return false
}
