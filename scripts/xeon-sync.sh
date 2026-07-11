#!/bin/sh
# Publish the committed checkout to the dedicated Linux/KVM test checkout.
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
host=${VMON_TEST_HOST:-xeon.internal}

if [ -z "${VMON_TEST_REPO:-}" ] || [ -z "${VMON_TEST_GIT:-}" ] || [ -z "${VMON_TEST_TARGET:-}" ]; then
	remote_user=$(ssh -G "$host" 2>/dev/null | awk '/^user / {print $2}' || true)
	remote_user=${remote_user:-$USER}
	if [ "$remote_user" = "root" ]; then
		remote_home="/root"
	else
		remote_home="/home/$remote_user"
	fi
else
	remote_home=""
fi

remote_repo=${VMON_TEST_REPO:-$remote_home/vibevmm-e2e}
remote_git=${VMON_TEST_GIT:-$remote_home/vibevmm-sync.git}
remote_target=${VMON_TEST_TARGET:-$remote_home/vibevmm/target}
sync_ref=${VMON_TEST_REF:-refs/heads/vmon-e2e-sync}

case "$remote_repo" in
	/*) ;;
	*) echo "xeon-sync: remote checkout must be an absolute path: $remote_repo" >&2; exit 2 ;;
esac
case "$remote_git" in
	/*) ;;
	*) echo "xeon-sync: remote Git repository must be an absolute path: $remote_git" >&2; exit 2 ;;
esac
if [ "$remote_repo" = "/" ] || [ "$remote_git" = "/" ]; then
	echo "xeon-sync: refusing an unsafe remote path" >&2
	exit 2
fi
if ! git check-ref-format "$sync_ref"; then
	echo "xeon-sync: invalid sync ref: $sync_ref" >&2
	exit 2
fi

# A Git transport deliberately publishes one reproducible commit, not a partial
# working tree. Ignored workstation files such as AGENTS.local.md do not matter.
if ! git -C "$repo_root" diff --quiet --no-ext-diff -- ||
	! git -C "$repo_root" diff --cached --quiet --no-ext-diff --
then
	echo "xeon-sync: tracked changes are not committed; commit them before testing" >&2
	exit 2
fi
untracked=$(git -C "$repo_root" ls-files --others --exclude-standard)
if [ -n "$untracked" ]; then
	echo "xeon-sync: untracked files are not committed:" >&2
	printf '%s\n' "$untracked" >&2
	exit 2
fi
commit=$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')

# The private bare repository is only a transport between this checkout and the
# disposable remote checkout. Nothing is pushed to the public origin.
ssh "$host" sh -s -- "$remote_git" <<'REMOTE'
set -eu
git_dir=$1
if [ ! -e "$git_dir" ]; then
	mkdir -p "$(dirname "$git_dir")"
	git init --bare "$git_dir"
elif [ "$(git --git-dir="$git_dir" rev-parse --is-bare-repository 2>/dev/null || true)" != "true" ]; then
	echo "xeon-sync: $git_dir exists but is not a bare Git repository" >&2
	exit 2
fi
REMOTE

git -C "$repo_root" push --force "$host:$remote_git" "$commit:$sync_ref"

ssh "$host" sh -s -- "$remote_repo" "$remote_git" "$remote_target" "$sync_ref" "$commit" <<'REMOTE'
set -eu
repo=$1
git_dir=$2
target=$3
sync_ref=$4
commit=$5

mkdir -p "$repo"
if ! git -C "$repo" rev-parse --git-dir >/dev/null 2>&1; then
	git -C "$repo" init
fi
if git -C "$repo" remote get-url vmon-sync >/dev/null 2>&1; then
	git -C "$repo" remote set-url vmon-sync "$git_dir"
else
	git -C "$repo" remote add vmon-sync "$git_dir"
fi
git -C "$repo" fetch --force vmon-sync "$sync_ref:refs/remotes/vmon-sync/current"
git -C "$repo" checkout --detach --force "$commit"
git -C "$repo" clean -fd

if [ ! -e "$repo/target" ] && [ ! -L "$repo/target" ]; then
	ln -s "$target" "$repo/target"
fi
actual=$(git -C "$repo" rev-parse HEAD)
if [ "$actual" != "$commit" ]; then
	echo "xeon-sync: remote checkout is $actual, expected $commit" >&2
	exit 1
fi
REMOTE

printf 'xeon-sync: %s -> %s:%s at %s\n' "$repo_root" "$host" "$remote_repo" "$commit"
