# 微信容器 + 微信连接器：官方 Linux 微信（wechat-selkies，网页桌面）+ 连接器自启
# 构建上下文是 wechat-connector/ 目录：
#   docker build -f docker/Dockerfile -t wechat-connector .
FROM nickrunning/wechat-selkies:0.0.17-minimal

# gdb：扫码时抓数据库密钥；xdotool/xclip：点击和粘贴；at-spi：读微信界面；lame：mp3 编解码；
# ffmpeg：把微信 wxgf 格式（HEVC）的照片原图转成 JPEG
RUN apt-get update \
 && apt-get install -y --no-install-recommends gdb xdotool xclip at-spi2-core gir1.2-atspi-2.0 lame ffmpeg \
 && rm -rf /var/lib/apt/lists/*

# 镜像只给微信 PULSE_RUNTIME_PATH=/defaults，PulseAudio 要求这个目录归自己所有，微信就连不上声音服务
# （一点「发语音」就提示「未检测到麦克风设备」）。直接告诉所有程序服务端在哪。
ENV PULSE_SERVER=unix:/defaults/native \
    QT_LINUX_ACCESSIBILITY_ALWAYS_ON=1

# 状态页：网页桌面（nginx）的 /wxc/ 转给连接器在容器里开的 127.0.0.1:6299，沿用网页桌面的密码。
# 改的是模板，nginx 每次启动都从它生成配置；两个 server 块（3000、3001）各插一条，插不进去就让构建失败
RUN sed -i 's|^  location SUBFOLDER {$|  location = SUBFOLDERwxc { return 301 SUBFOLDERwxc/; }\n  location SUBFOLDERwxc/ {\n    proxy_pass http://127.0.0.1:6299/;\n  }\n&|' /defaults/default.conf \
 && test "$(grep -c 'proxy_pass http://127.0.0.1:6299/' /defaults/default.conf)" = 2

COPY connector/ /opt/wechat-connector/
# init-wxvmic：PulseAudio 之后、桌面之前建虚拟麦克风；svc-wechat-connector：微信起来后启动连接器
COPY docker/root/ /
