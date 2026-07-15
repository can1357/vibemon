#!/bin/sh
set -eu

skip() {
  printf '%s\n' "VMON_WORKLOAD_SKIP: browser: $*"
  exit 77
}

fail() {
  printf '%s\n' "VMON_WORKLOAD_FAIL: browser: $*" >&2
  exit 1
}

if [ "$(id -u)" -eq 0 ]; then
  id vmon >/dev/null 2>&1 || fail "workload user vmon not found"
  exec su -s /bin/sh vmon -c \
    "HOME=/home/vmon /opt/vmon-workloads/browser.sh"
fi

browser=
for candidate in chromium chromium-browser google-chrome; do
  if command -v "$candidate" >/dev/null 2>&1; then
    browser=$candidate
    break
  fi
done
[ -n "$browser" ] || skip "Chromium binary not installed"

[ -r /proc/sys/user/max_user_namespaces ] || skip "user namespaces unavailable"
[ "$(cat /proc/sys/user/max_user_namespaces)" -gt 0 ] || skip "user namespaces disabled"
[ -d /dev/shm ] || skip "/dev/shm unavailable"
mountpoint -q /dev/shm 2>/dev/null || skip "/dev/shm is not mounted"
shm_kib=$(df -Pk /dev/shm 2>/dev/null | awk 'NR == 2 { print $2 }')
[ "${shm_kib:-0}" -ge 65536 ] || skip "/dev/shm is smaller than 64 MiB"
fc-list 2>/dev/null | grep -q . || skip "fonts unavailable"

page=/opt/vmon-workloads/browser-page.html
[ -r "$page" ] || fail "fixture page missing"
profile=/tmp/chromium-profile
mkdir -p "$profile"
output=$("$browser" --headless --disable-gpu --user-data-dir="$profile" --dump-dom "file://$page" 2>&1) \
  || fail "Chromium navigation failed: $output"
printf '%s\n' "$output" | grep -Fq 'VMON_BROWSER_PAGE_OK' || fail "page marker missing"
printf '%s\n' 'VMON_BROWSER_OK'
exit 0
