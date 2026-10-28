# 微信容器 + 微信连接器：官方 Linux 微信（wechat-selkies，网页桌面）+ 连接器自启
# 构建上下文是 wechat-connector/ 目录：
#   docker build -f docker/Dockerfile -t wechat-connector .
FROM nickrunning/wechat-selkies:0.0.17-minimal

# gdb：扫码时抓数据库密钥；xdotool/xclip：点击和粘贴；at-spi：读微信界面；lame：mp3 编解码
RUN apt-get update \
 && apt-get install -y --no-install-recommends gdb xdotool xclip at-spi2-core gir1.2-atspi-2.0 lame \
 && rm -rf /var/lib/apt/lists/*

# 镜像只给微信 PULSE_RUNTIME_PATH=/defaults，PulseAudio 要求这个目录归自己所有，微信就连不上声音服务
# （一点「发语音」就提示「未检测到麦克风设备」）。直接告诉所有程序服务端在哪。
ENV PULSE_SERVER=unix:/defaults/native \
    QT_LINUX_ACCESSIBILITY_ALWAYS_ON=1

COPY connector/ /opt/wechat-connector/
# init-wxvmic：PulseAudio 之后、桌面之前建虚拟麦克风；svc-wechat-connector：微信起来后启动连接器
COPY docker/root/ /
