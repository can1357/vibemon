#!/usr/bin/env bash
# Build and atomically roll out vmon to every live AWS worker and scheduler.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-}}
TARGET=${VMON_TARGET:-x86_64-unknown-linux-gnu.2.34}
BUILD=1
BINARY=
ROLES=all
S3_URI=${VMON_BINARY_S3_URI:-}

usage() {
	cat <<'EOF'
Usage: deploy/aws/rollout.sh [OPTIONS]

Builds vmon, discovers current instances by their vibevmm:role tag, opens SSH
only to this host for the duration of the rollout, and atomically updates each
worker and scheduler. No instance IDs or IP addresses are stored in the script.

Options:
  --binary PATH       Deploy an existing binary and skip the build
  --no-build          Use the default target binary without rebuilding
  --region REGION     AWS region (defaults to AWS_REGION/AWS config)
  --roles ROLES       all, worker, or scheduler (default: all)
  --s3-uri URI        Also update the ASG launch artifact, e.g. s3://bucket/vmon
  -h, --help          Show this help

Environment:
  VMON_TARGET         cargo-zigbuild target (default: x86_64 Linux glibc 2.34)
  VMON_BINARY_S3_URI  Same as --s3-uri
  VMON_SOURCE_CIDR    SSH source CIDR (default: this host's public IPv4 /32)
EOF
}

while (($#)); do
	case "$1" in
		--binary)
			[[ $# -ge 2 ]] || { echo "--binary requires a path" >&2; exit 2; }
			BINARY=$2
			BUILD=0
			shift 2
			;;
		--no-build)
			BUILD=0
			shift
			;;
		--region)
			[[ $# -ge 2 ]] || { echo "--region requires a value" >&2; exit 2; }
			REGION=$2
			shift 2
			;;
		--roles)
			[[ $# -ge 2 ]] || { echo "--roles requires a value" >&2; exit 2; }
			ROLES=$2
			shift 2
			;;
		--s3-uri)
			[[ $# -ge 2 ]] || { echo "--s3-uri requires a value" >&2; exit 2; }
			S3_URI=$2
			shift 2
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			echo "unknown option: $1" >&2
			usage >&2
			exit 2
			;;
	esac
done

case "$ROLES" in
	all|worker|scheduler) ;;
	*) echo "--roles must be all, worker, or scheduler" >&2; exit 2 ;;
esac

for command in aws curl scp ssh ssh-keygen; do
	command -v "$command" >/dev/null || { echo "required command not found: $command" >&2; exit 1; }
done

if [[ -z "$REGION" ]]; then
	REGION=$(aws configure get region 2>/dev/null || true)
fi
[[ -n "$REGION" ]] || { echo "set AWS_REGION or pass --region" >&2; exit 1; }

if [[ -z "$BINARY" ]]; then
	OUTPUT_TARGET=${TARGET%.2.34}
	BINARY="$ROOT/target/$OUTPUT_TARGET/release/vmon"
fi
if ((BUILD)); then
	command -v cargo-zigbuild >/dev/null || {
		echo "cargo-zigbuild is required; install it or pass --binary" >&2
		exit 1
	}
	(
		cd "$ROOT"
		cargo zigbuild --locked --release --target "$TARGET" -p vmon
	)
fi
[[ -x "$BINARY" ]] || { echo "vmon binary is missing or not executable: $BINARY" >&2; exit 1; }

if [[ -n "$S3_URI" ]]; then
	echo "Updating launch artifact: $S3_URI"
	aws s3 cp --only-show-errors "$BINARY" "$S3_URI"
else
	echo "Warning: current instances will be updated, but the ASG launch artifact is unchanged." >&2
	echo "Pass --s3-uri so replacement workers boot this binary." >&2
fi

TMP=$(mktemp -d "${TMPDIR:-/tmp}/vmon-rollout.XXXXXX")
KEY="$TMP/key"
INVENTORY="$TMP/instances.tsv"
RULE_IDS=()
RULE_GROUPS=()
OPENED_GROUPS=""

cleanup() {
	status=$?
	trap - EXIT INT TERM
	for index in "${!RULE_IDS[@]}"; do
		aws ec2 revoke-security-group-ingress \
			--region "$REGION" \
			--group-id "${RULE_GROUPS[$index]}" \
			--security-group-rule-ids "${RULE_IDS[$index]}" >/dev/null 2>&1 || true
	done
	rm -rf "$TMP"
	exit "$status"
}
trap cleanup EXIT INT TERM

ssh-keygen -q -t ed25519 -N '' -f "$KEY"

aws ec2 describe-instances \
	--region "$REGION" \
	--filters \
		"Name=instance-state-name,Values=running" \
		"Name=tag:vibevmm:role,Values=worker,scheduler" \
	--query "Reservations[].Instances[].[Tags[?Key=='vibevmm:role']|[0].Value,InstanceId,Placement.AvailabilityZone,PublicIpAddress,SecurityGroups[0].GroupId]" \
	--output text > "$INVENTORY"

[[ -s "$INVENTORY" ]] || { echo "no running vibevmm workers or schedulers found in $REGION" >&2; exit 1; }

SOURCE_CIDR=${VMON_SOURCE_CIDR:-}
if [[ -z "$SOURCE_CIDR" ]]; then
	SOURCE_IP=$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')
	[[ "$SOURCE_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
		echo "could not determine this host's public IPv4" >&2
		exit 1
	}
	SOURCE_CIDR="$SOURCE_IP/32"
fi

open_ssh_ingress() {
	local group=$1
	local error_file="$TMP/authorize-$group.err"
	local rule_id status

	case " $OPENED_GROUPS " in
		*" $group "*) return ;;
	esac
	OPENED_GROUPS="$OPENED_GROUPS $group"

	set +e
	rule_id=$(aws ec2 authorize-security-group-ingress \
		--region "$REGION" \
		--group-id "$group" \
		--protocol tcp \
		--port 22 \
		--cidr "$SOURCE_CIDR" \
		--query 'SecurityGroupRules[0].SecurityGroupRuleId' \
		--output text 2>"$error_file")
	status=$?
	set -e
	if ((status == 0)); then
		RULE_IDS+=("$rule_id")
		RULE_GROUPS+=("$group")
	elif grep -q 'InvalidPermission.Duplicate' "$error_file"; then
		echo "SSH ingress already permits $SOURCE_CIDR on $group"
	else
		cat "$error_file" >&2
		return "$status"
	fi
}

role_selected() {
	[[ "$ROLES" == all || "$ROLES" == "$1" ]]
}

SSH_OPTIONS=(
	-i "$KEY"
	-o BatchMode=yes
	-o ConnectTimeout=10
	-o StrictHostKeyChecking=no
	-o UserKnownHostsFile=/dev/null
)

rollout_instance() {
	local role=$1 instance_id=$2 zone=$3 public_ip=$4 group=$5
	local remote="/tmp/vmon-rollout-$$"
	local uploaded=0

	[[ "$public_ip" != None && -n "$public_ip" ]] || {
		echo "$role $instance_id has no public IPv4; cannot deploy without a bastion" >&2
		return 1
	}
	open_ssh_ingress "$group"
	echo "Deploying $role $instance_id ($public_ip, $zone)"

	for attempt in 1 2 3 4 5; do
		aws ec2-instance-connect send-ssh-public-key \
			--region "$REGION" \
			--instance-id "$instance_id" \
			--availability-zone "$zone" \
			--instance-os-user ec2-user \
			--ssh-public-key "file://$KEY.pub" >/dev/null
		if scp "${SSH_OPTIONS[@]}" "$BINARY" "ec2-user@$public_ip:$remote"; then
			uploaded=1
			break
		fi
		sleep 2
	done
	((uploaded)) || { echo "upload failed for $instance_id" >&2; return 1; }

	ssh "${SSH_OPTIONS[@]}" "ec2-user@$public_ip" \
		"sudo bash -s -- '$role' '$remote'" <<'REMOTE'
set -euo pipefail
role=$1
uploaded=$2
next=/usr/local/bin/vmon.next
previous=/usr/local/bin/vmon.previous
case "$role" in
	worker)
		services=(vmon-netbroker vmon-worker)
		port=8000
		;;
	scheduler)
		services=(vmon-sched)
		port=8100
		;;
	*)
		echo "unsupported role: $role" >&2
		exit 2
		;;
esac

install -o root -g root -m 0755 "$uploaded" "$next"
cp -p /usr/local/bin/vmon "$previous"
mv -f "$next" /usr/local/bin/vmon
rollback() {
	trap - ERR
	if [[ -f "$previous" ]]; then
		mv -f "$previous" /usr/local/bin/vmon
		systemctl restart "${services[@]}" || true
	fi
}
trap rollback ERR
systemctl restart "${services[@]}"
for service in "${services[@]}"; do
	systemctl is-active --quiet "$service"
done
for _ in {1..30}; do
	if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null; then
		rm -f "$previous" "$uploaded"
		trap - ERR
		/usr/local/bin/vmon --version
		exit 0
	fi
	sleep 1
done
echo "$role health check timed out on port $port" >&2
exit 1
REMOTE
}

DEPLOYED=0
for wanted_role in worker scheduler; do
	role_selected "$wanted_role" || continue
	while IFS=$'\t' read -r role instance_id zone public_ip group; do
		[[ "$role" == "$wanted_role" ]] || continue
		rollout_instance "$role" "$instance_id" "$zone" "$public_ip" "$group"
		DEPLOYED=$((DEPLOYED + 1))
	done < "$INVENTORY"
done

((DEPLOYED > 0)) || { echo "no running instances matched --roles $ROLES" >&2; exit 1; }

echo "Rollout complete: $DEPLOYED instance(s)"
while IFS=$'\t' read -r role _instance_id _zone public_ip _group; do
	if [[ "$role" == scheduler ]]; then
		echo "Scheduler: http://$public_ip:8100"
	fi
done < "$INVENTORY"
