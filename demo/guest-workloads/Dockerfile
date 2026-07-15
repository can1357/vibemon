FROM alpine:3.22

ARG VMON_ARCH

RUN apk add --no-cache \
      bash \
      ca-certificates \
      cpio \
      chromium \
      docker \
      font-dejavu \
      fontconfig \
      shadow \
      tar \
      util-linux \
    && addgroup -g 1000 vmon \
    && adduser -D -u 1000 -G vmon -h /home/vmon vmon \
    && chmod 1777 /tmp

COPY target/test-assets/vmon-agent-${VMON_ARCH} /usr/bin/vmon-agent
COPY demo/guest-workloads/init /init
COPY demo/guest-workloads/runc-no-pivot /usr/local/bin/runc-no-pivot

RUN chmod 0755 /init /usr/bin/vmon-agent /usr/local/bin/runc-no-pivot
