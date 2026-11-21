# AutoClaw2API

`AutoClaw2API` 是一款使用纯 Go 标准库构建的独立多协议 AI 网关，内置 AutoClaw 双区协议引擎（国内版 `autoclawcn` / 国际版 `autoclawglobal`）、多账号负载均衡池、独立代理池与「一号一代理」出口路由、每日自动签到以及终端极客风单页 Web 控制台。

> **⚠️ 重要合规提示**：本项目仅限个人学习与技术研究使用，严禁用于任何商业售卖、API 转售、有偿代搭、代充代刷、公开/收费共享池或任何违反上游平台服务条款与法律法规的用途。下载、编译或使用本项目即表示您已阅读并同意文末[免责声明](#免责声明)。

- **作者**：`VILK`
- **许可证**：[MIT License](LICENSE)
- **技术栈**：Go 1.24+ 纯标准库（零第三方运行时依赖，不启用 CGO，单页控制台使用 `go:embed` 内嵌）

---

## 功能特性

- **多协议对外兼容**
  - `GET /v1/models`：实时暴露已同步区域的可用模型列表（支持裸名、渠道前缀及自定义别名）。
  - `POST /v1/chat/completions`：完整支持 OpenAI Chat Completions 协议（非流式、流式 SSE、Tool Calling、多模态输入）。
  - `POST /v1/responses`：支持 OpenAI Responses API 与上游对话协议的双向转换（含流式 SSE 事件）。
  - `POST /v1/messages`：支持 Anthropic Messages API 双向转换（System Prompt、多模态、Tool Use / Tool Result、Thinking / Reasoning 字段与流式 SSE 事件），可直接接入 Claude Code 等客户端。
- **双区实时模型同步与多账号高可用**
  - 双区（`autoclawcn` 与 `autoclawglobal`）统一在存在已登录且启用账号时，实时向对应区域上游请求 `/autoclaw-proxy/proxy/autoclaw-model-config` 同步模型，不预置静态假列表；当某区域无有效账号时自动清空并不暴露。
  - 多账号轮询负载均衡、失败自动切号、指数退避重试（识别 `Retry-After`）与错误分级冷却（会话失效暂停调度、积分不足长冷却、429 限流短冷却）。
  - 支持凭证目录热扫描与无损 JSON 合并写回、Token 自动续期、每日定时签到并上报登录活跃。
- **独立代理池与「一号一代理」**
  - 批量导入 `http://`、`https://`、`socks5://`、`socks5h://` 代理（支持多行 URI、Base64 订阅与 Clash YAML 节点自动解析、去重与逐条反馈）。
  - 节点密码掩码展示、手动与后台定时测活、防误删保护，节点变更或停用时自动关闭旧连接池并重建。
  - 三级出口绑定（账号级 `fixed` / `pool` / `direct` / `inherit` → 区域默认出口 `cn` / `global` → 默认直连），覆盖对话、流式 SSE、续期、查余额、签到与登录全链路。
- **面板密码与 API Key 彻底分离**
  - **面板密码**：首次打开 Web 控制台自动弹出初始化向导设置面板登录密码（加盐 SHA-256 哈希落盘），支持修改密码与退出登录。
  - **API Key 管理**：独立标签页支持一键随机生成 `vKey-` 开头密钥，支持自定义备注、一键复制、启用/停用与删除。

---

## 快速启动

需要 **Go 1.24+** 环境。

### 方式一：Windows 一键启动脚本

双击或在终端运行根目录的 `start.bat`：

```bat
start.bat
```

### 方式二：命令行直接运行

```bash
# 运行全部单元测试
go test ./...

# 启动网关服务（首次启动自动生成默认 config.json）
go run ./cmd/autoclaw2api -config config.json
```

### 跨平台单文件编译

```bash
# Windows amd64
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o autoclaw2api.exe ./cmd/autoclaw2api

# Linux amd64
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o autoclaw2api ./cmd/autoclaw2api

# macOS arm64
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o autoclaw2api ./cmd/autoclaw2api
```

启动后浏览器访问 `http://localhost:8080`，按向导完成**面板密码**初始化即可进入控制台。

---

## 配置说明

可参考模板文件 [config.example.json](config.example.json)：

```json
{
  "listen": "0.0.0.0:8080",
  "panel_auth": {
    "password_hash": ""
  },
  "api_keys": [],
  "auth_dir": "./auths",
  "data_dir": "./data",
  "log_level": "info",
  "upstream": {
    "cn_base_url": "",
    "global_base_url": ""
  },
  "retry": {
    "max_retries": 3,
    "base_backoff_ms": 500,
    "max_backoff_ms": 8000,
    "short_cooldown_sec": 60,
    "long_cooldown_sec": 1800
  },
  "checkin": {
    "enabled": true,
    "time": "08:30",
    "window_start": "08:00",
    "window_end": "11:30"
  },
  "routing": {
    "prefix_mode": "both",
    "disabled_models": [],
    "aliases": {}
  }
}
```

- `panel_auth.password_hash`：控制台登录密码哈希（由首次初始化向导自动生成，请勿手填明文）。
- `api_keys`：独立网关访问密钥列表（建议通过控制台「API Key」页面一键生成 `vKey-` 密钥）；留空时网关免鉴权，创建任意密钥后仅放行已启用的匹配密钥。
- `auth_dir`：账号凭证目录，支持自动扫描 `autoclawcn-*.json` 与 `autoclawglobal-*.json`。
- `data_dir`：运行态数据目录，用于保存代理池配置（`proxypool.json`）与签到历史（`checkin_history.json`）。
- `routing`：`prefix_mode` 支持 `both` / `bare` / `prefixed`；`aliases` 支持将同一别名映射到单个或多个目标模型实现自动跨区负载均衡。
- `checkin`：`window_start` / `window_end` 定义每日随机签到时段（本地时区，默认 `08:00`–`11:30`）。每天按日期确定性随机一个时刻触发（同一天重启不重掷、跨天自动换），多账号会打乱顺序并插入 20–150 秒随机间隔，避免固定时刻 + 同秒批量上报的脚本化特征；未配置时段时回落到 `time` 的固定时刻。进程时区建议设为与出口节点一致（如香港家宽节点用 `TZ=Asia/Hong_Kong`）。
- 代理测活默认使用明文 HTTP 的 `http://www.gstatic.com/generate_204` 标准 204 测法（连续 3 次采样取最小延迟），可在控制台「代理」页修改测活地址与间隔。

---

## API 调用示例

在控制台「API Key」页面生成 `vKey-` 密钥并添加启用账号后即可调用（模型名以 `/v1/models` 实时返回的模型为准）：

### 1. 获取当前可用模型列表

```bash
curl http://localhost:8080/v1/models \
  -H "Authorization: Bearer vKey-YOUR_API_KEY"
```

### 2. OpenAI Chat Completions

```bash
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer vKey-YOUR_API_KEY" \
  -d '{
    "model": "<已同步的模型名>",
    "stream": true,
    "messages": [
      {"role": "user", "content": "请用 Go 写一个快速排序"}
    ]
  }'
```

### 3. OpenAI Responses API

```bash
curl http://localhost:8080/v1/responses \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer vKey-YOUR_API_KEY" \
  -d '{
    "model": "<已同步的模型名>",
    "input": "你好，请简单介绍一下你自己"
  }'
```

### 4. Anthropic Messages API（兼容 Claude Code）

```bash
export ANTHROPIC_BASE_URL="http://localhost:8080"
export ANTHROPIC_API_KEY="vKey-YOUR_API_KEY"

curl http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: vKey-YOUR_API_KEY" \
  -d '{
    "model": "<已同步的模型名>",
    "max_tokens": 2048,
    "messages": [
      {"role": "user", "content": "你好"}
    ]
  }'
```

---

## 免责声明

1. **仅限个人学习与技术研究**：本项目（`AutoClaw2API`）仅用于个人学习、技术交流与协议互操作性研究，严禁将本项目（或其源码、编译产物、衍生服务）用于任何形式的商业售卖、API 转售、有偿代搭、代充代刷、公开或收费算力共享池，以及任何违反法律法规的用途。
2. **无官方关联**：本项目为独立开发的开源工具，与智谱 / Z.AI（AutoClaw）或任何其他上游平台及服务商均无任何隶属、合作、授权或背书关系。涉及的商标、产品名称及服务权益均归其各自权利人所有。
3. **合规与风险自负**：通过非官方客户端或网关调用接口、执行自动签到等操作可能违反上游平台的用户服务协议（ToS），并可能引发账号受限、积分清零、封禁及其他合规风险。使用者必须在遵守当地法律法规及个人自用范围内谨慎使用本项目；因使用、修改或滥用本项目产生的一切直接或间接后果（包括但不限于账号损失、数据损失及法律责任）均由使用者本人自行承担，项目作者不承担任何责任。
4. **条款生效**：任何个人或组织在下载、克隆、编译、部署或使用本项目的任意部分时，即视为已充分阅读、理解并完全同意上述全部条款；如不同意，请立即停止使用并删除本项目全部文件。
