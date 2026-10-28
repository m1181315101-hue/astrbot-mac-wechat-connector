# 微信连接器（WeChat → OneBot v11）

让 AstrBot 用**一个微信小号**收发微信消息。它和 NapCat 接 QQ 的方式一样：对 AstrBot 来说，就是又多了一个 OneBot v11（aiocqhttp）平台。

微信那边跑的是**官方 Linux 版微信**，装在 Docker 容器里。连接器不改微信客户端，也不碰微信的网络协议：

- **收**：读微信自己写在本地的聊天数据库。
- **发**：用无障碍接口和模拟键盘，像人一样在微信界面里操作。

> **当前能做的**：
> - **收**：只收文字。私聊只放行白名单里的人；群聊只放行白名单里的群，群里有人 @ 小号时才会触发回复。
> - **发**：文字、图片、表情包（GIF 会变成微信的动画表情）、**原生语音条**（用微信自带的「发语音」录一段虚拟麦克风里的声音；录不了时退回成 mp3 音频文件）。AstrBot 的插件和 MCP 工具都照常可用，它们发出的图片、语音会被原样转发。
> - 还不能收别人发的图片和语音，正在做。

## ⚠️ 先看风险

- **只用小号。** 用程序自动收发消息违反《微信软件许可及服务协议》，账号可能被限制功能甚至封号。别用主号，也别用对你重要的号。
- **只读自己账号的数据。** 微信的本地数据库是加密的。连接器会在你扫码登录的那一刻，从微信进程里取出这个账号的数据库密钥，用来解密**你自己登录的这个号**的聊天记录。不要拿它去读别人的数据。
- 腾讯曾对类似的微信数据库解密工具发起过 DMCA 下架。本目录的代码只供个人学习和自用，使用后果由使用者自己承担。
- 被白名单放行的私聊和群消息，会交给 AstrBot 和它配置的大模型处理。把群加进白名单前，先想好群友是否介意。
- 本目录**不包含**任何账号、密钥、wxid 或聊天内容。`config.json` 和 `keys.json` 都只放在容器的数据目录里，权限 600。

## 工作原理

```
┌ Docker（和 AstrBot 在同一个网络里）──────────────────────────────────────────┐
│ 微信容器：官方 Linux 微信 + 网页桌面（浏览器里扫码、查看）                     │
│                                                                             │
│   connector/bridge.py（连接器主程序）                                        │
│     ├─ 收：每秒看一眼消息库 → 解密新写入的部分（明文只在内存里）               │
│     │       → 白名单里的新文字 → OneBot 消息事件                             │
│     ├─ OneBot v11 反向 WS 客户端 ──────────────→ ws://astrbot:6200/ws        │
│     └─ 回：send_private_msg / send_group_msg → 发送队列（文字、图片、语音按顺序）│
│            → wxui.py：按名字切换会话 → 核对 → 粘贴 → 回车 → 回查数据库确认   │
└─────────────────────────────────────────────────────────────────────────────┘
```

| 文件 | 作用 |
|---|---|
| `connector/wxkey.py` | 扫码登录时用 GDB 断点抓数据库密钥原料（passphrase），校验后存进 `keys.json` |
| `connector/wxdb.py` | 解密数据库（SQLCipher 4）：主库加 WAL 增量叠加，用 `sqlite3.deserialize` 在内存里打开，读新消息和通讯录 |
| `connector/wxui.py` | 界面驱动：通过无障碍接口（AT-SPI）读微信界面，按名字找会话，用 xdotool 点击和粘贴；发语音条时点「发语音」、把音频播进虚拟麦克风、再点「发送语音」 |
| `connector/onebot.py` | OneBot v11 反向 WebSocket 客户端，以及消息事件的格式 |
| `connector/media.py` | 把 AstrBot 发来的图片、语音（base64）落成临时文件：WEBP 转 PNG；语音条用 wav（mp3 用 lame 解码，超过 55 秒切段），退回文件时用 lame 转 mp3；估算音频时长 |
| `connector/bridge.py` | 主程序：轮询、白名单、发送队列、限速、确认 |

## 环境要求

- Docker。只在 **Apple 芯片的 Mac（arm64）** 上验证过。
- 微信容器：[`nickrunning/wechat-selkies:0.0.17-minimal`](https://github.com/nickrunning/wechat-selkies)，里面是官方 Linux 微信 **4.1.13**。
- 取密钥用的 `wxkey.py` **只支持 arm64 版微信**。x86_64 的机器可以用 [wcdb-key-tool](https://github.com/TANGandXUE/wcdb-key-tool) 取到 passphrase，写进 `keys.json` 的 `passphrase` 字段（十六进制）。
- 微信升级以后，数据库格式、密钥派生函数的位置、界面结构都可能变化，所以**容器镜像要钉住版本**，不要让微信自动更新。

## 部署

微信容器的镜像用本目录的 [`docker/Dockerfile`](docker/Dockerfile) 构建，在 `nickrunning/wechat-selkies` 的基础上：

- 装好 gdb、xdotool、xclip、无障碍组件（at-spi）、lame；
- 设好 `PULSE_SERVER`（原镜像只给 `PULSE_RUNTIME_PATH=/defaults`，PulseAudio 要求这个目录归自己所有，微信就连不上声音服务，一点「发语音」就提示「未检测到麦克风设备」）；
- `init-wxvmic`：PulseAudio 起来之后、桌面（微信）起来之前，建一个虚拟麦克风并设成默认输入。微信只在启动时找麦克风，而且不把 PulseAudio 的「监听源」当麦克风，所以必须在它启动前建好；
- `svc-wechat-connector`：等配置、密钥、微信都就绪后自动启动连接器，挂了会被重新拉起。

下面的命令都在 AstrBot 的 compose 目录里执行。

### 1. 构建并启动微信容器

把本目录放在 compose 目录下（或者改 `context` 指过来），在 compose 里加：

```yaml
  wechat:
    build:
      context: ./dafeiyu-astrbot/wechat-connector
      dockerfile: docker/Dockerfile
    image: dafeiyu/wechat-connector:local
    container_name: wechat
    hostname: wechat            # 固定主机名：换了的话微信可能当成新设备，要重新登录
    profiles: ["wechat"]        # 只能点名启停，不会被 docker compose up -d 顺手带起来
    restart: unless-stopped
    ports:
      - "127.0.0.1:3001:3001"   # 网页桌面，只绑本机
    volumes:
      - ./wechat:/config        # 微信登录态、聊天数据库、连接器的配置和密钥都在这里
    cap_add:
      - SYS_PTRACE              # 第 2 步取密钥要用
    environment:
      - PUID=1000
      - PGID=100
      - TZ=Asia/Shanghai
      - LC_ALL=zh_CN.UTF-8
      - AUTO_START_WECHAT=true
      - AUTO_START_QQ=false
      - CUSTOM_USER=${WECHAT_WEB_USER}       # 网页桌面的账号密码，写在同目录 .env 里（权限 600）
      - PASSWORD=${WECHAT_WEB_PASSWORD}
    shm_size: 1gb
```

```bash
docker compose --profile wechat up -d --build wechat
```

它和 AstrBot 在同一个 compose 里，所以自动在同一个网络，能用 `astrbot` 这个名字连过去。

### 2. 扫码登录，同时抓密钥

**先挂断点，再扫码。** 新版微信只在登录那一刻派生数据库密钥。

```bash
# 先运行，等它提示「现在扫码登录」
docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py capture
```

然后在浏览器打开 `https://localhost:3001`，用**小号**扫码登录。抓到之后运行：

```bash
docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py verify
```

看到「校验通过 N/N 个库」就说明成功了，结果存在 `/config/wechat-connector/keys.json`（权限 600）。以后容器重启、微信重启（会自动登录）都不用再抓，只有换号或者在手机上退出、重新扫码时才要重来一遍。

### 3. 写配置

把 [`config.example.json`](config.example.json) 复制成 `./wechat/wechat-connector/config.json`（容器里是 `/config/wechat-connector/config.json`），权限设成 600。白名单要填 wxid，可以按备注名或昵称查（必须完全一致）：

```bash
docker exec -u abc wechat /lsiopy/bin/python3 /opt/wechat-connector/bridge.py lookup "<备注名或群名>"
```

它会打印出 wxid、类型（联系人或群），以及连接器给这个号的 **OneBot 号**（一个整数）。AstrBot 那边看到的 QQ 号、群号就是这个数。

| 配置项 | 说明 |
|---|---|
| `astrbot_ws_url` | AstrBot 的反向 WS 地址，例如 `ws://astrbot:6200/ws` |
| `access_token` | 反向 WS 口令，要和 AstrBot 平台里填的一样 |
| `whitelist` | 允许私聊的人（wxid 列表）。不在里面的人发来的消息不转发、不回复 |
| `group_whitelist` | 允许的群（`xxx@chatroom` 列表） |
| `bot_names` | 群里 @ 小号时可能用的名字（比如小号的群昵称）。小号自己的微信昵称会自动加上 |
| `receive_only` | `true` 时只读消息、写日志，不连 AstrBot。第一次部署建议先开着试 |
| `face_map` | 可选，指向 QQ 表情插件的 `face_map.json`（复制到 `./wechat/wechat-connector/` 里）。AstrBot 回复里的 QQ 表情会翻回 `[旺柴]` 这种文字，微信会显示成表情 |
| `max_sends_per_minute` | 每分钟最多发几条，默认 20，多出来的排队 |
| `max_message_age` | 超过这么多秒的旧消息不转发，默认 600。连接器停了一段时间再启动时，不会回一堆过期消息 |
| `group_voice_min_seconds` | 群里只发不短于这么多秒的语音，默认 20：点歌（翻唱）会发，随口的短语音不发，免得群里语音太多。设成 0 群里所有语音都发，设成 -1 群里一律不发。私聊不受影响。不发时会告诉 AstrBot「发送失败」，插件会改用文字回复 |
| `media_dir` | 待发图片、语音的临时目录，默认 `/tmp/wechat-connector-media`，超过 10 分钟的自动清掉 |
| `voice_mode` | `native`（默认）发原生语音条，失败时退回 mp3 文件；`file` 一律发 mp3 文件 |

### 4. AstrBot 里加一个平台

在 AstrBot 面板「消息平台」里新增 **OneBot v11（aiocqhttp）**：

- ID 随便起，比如 `wechat`
- 反向 WebSocket 主机 `0.0.0.0`，端口 `6200`（不要和 QQ 用的 6199 冲突）
- 口令填 `access_token`

端口只在 Docker 网络内部用，**不需要**映射到宿主机。

如果装了「只在指定群回复」这类插件，记得把群的 **OneBot 号**（`lookup` 打出来的那个整数）加进它的白名单。

### 5. 启动和检查

有了 `config.json` 和 `keys.json` 以后，连接器会在一分钟内自己启动（或者 `docker restart wechat`）。AstrBot 日志里多出一条「aiocqhttp(OneBot v11) 适配器已连接」，就说明连上了。

```bash
# 检查：密钥、数据库、通讯录、界面驱动是不是都就绪
docker exec -u abc wechat /lsiopy/bin/python3 /opt/wechat-connector/bridge.py check

# 自测发送：给「文件传输助手」发一段文字、一张图、一个动图、一段语音条（不经过 AstrBot）。
# 先停常驻服务，免得两边同时操作界面，测完再开
docker exec -u 0 wechat s6-svc -d /run/service/svc-wechat-connector
docker exec -u abc wechat /lsiopy/bin/python3 /opt/wechat-connector/bridge.py test-media --config /config/wechat-connector/config.json
docker exec -u 0 wechat s6-svc -u /run/service/svc-wechat-connector
```

日志在 `./wechat/wechat-connector/connector.log`，也可以看 `docker logs wechat`。

## 安全设计

| 机制 | 做法 |
|---|---|
| 白名单 | 只处理白名单里的人和群。发送时也只能发给白名单里的目标，AstrBot 想发给别人会被拒绝 |
| 发前核对 | 切到会话后，核对底部输入框的名字是否等于目标；粘贴前、回车前各核对一次，对不上就放弃，一个字都不发 |
| 不点错 | 搜索结果只认对应分组（私聊只认「联系人」，群只认「群聊」）里名字完全相同的那一项，「搜索网络结果」「聊天记录」永远不点；同名的不止一个就放弃 |
| 不标已读 | 会话列表里点不开时，直接改用搜索，不靠「先点别的会话再点回来」，免得把别人的消息标成已读 |
| 限速 | 每分钟最多 `max_sends_per_minute` 条；打字前按字数停顿 0.5～3 秒 |
| 急停 | 在 `./wechat/wechat-connector/` 里放一个名为 `STOP` 的文件，就只收不发，删掉恢复 |
| 发后确认 | 发完回查数据库，确认真的多了一条自己发出的消息 |
| 隐私 | 日志只记 OneBot 号、时间和字数，不记内容；解密后的明文只在内存里，不写盘 |

## 已知限制

- **收**：只收文字。别人发的图片、语音、表情包会被跳过（日志里记一行类型）。
- **语音条按实际时长录**：一段 20 秒的语音要录 20 秒，这期间界面被占着，后面的消息排队。超过 55 秒的切成几条（微信语音条最长 60 秒）。
- **视频、文件**：AstrBot 发视频、文件时给的是它自己容器里的路径，连接器拿不到，会跳过。
- **回复会慢几秒**：会话已经开着时大约 1 秒；要切换会话时 2～6 秒，再加上打字停顿。
- **同名的联系人或群会被跳过**：给它们设不同的备注名就行。
- **@ 只是文字**：回复里的 @ 是「@名字」这样的文字，对方不会收到提醒。
- **主窗口被关到托盘没关系**：在网页桌面里点了微信右上角的 ×，微信只是缩到托盘；连接器发消息前会点托盘图标把它叫回来，窗口太窄（会切成单栏、藏起搜索框）时调到 1760×1280。
- **界面同时只能一个人用**：连接器在发消息时，别在网页桌面里同时点微信，会打断它（它会因为核对失败而放弃，不会发错）。
- **每秒轮询一次**：消息库变化时，要在内存里重建一份明文库，容器大约占 1.5～2GB 内存。

## 致谢

- [jessiongod/wechat-onebot-bridge](https://github.com/jessiongod/wechat-onebot-bridge)（MIT）：OneBot v11 反向 WS 客户端的连接方式。
- [TANGandXUE/wcdb-key-tool](https://github.com/TANGandXUE/wcdb-key-tool)（MIT）：「在登录时断在 WCDB 密钥函数上取 passphrase」的思路。这里改写成了 arm64 版本：用 ADRP+ADD 寻址，参数从 x1、x2 读。
- [nickrunning/wechat-selkies](https://github.com/nickrunning/wechat-selkies)（MIT）：把官方 Linux 微信装进容器、在浏览器里使用。

本目录随仓库一起以 [MIT](../LICENSE) 协议开源。
