#!/bin/bash
# EC2 user data for tools/aws_acceptance.py. Upper-case placeholders between double at-signs are
# filled by the driver. The instance terminates itself (shutdown behavior
# "terminate") when the run ends or when the hard time cap expires.
set -u
shutdown -h +@@MAX_MINUTES@@
export AWS_ACCESS_KEY_ID='@@KEY@@' AWS_SECRET_ACCESS_KEY='@@SECRET@@' AWS_SESSION_TOKEN='@@TOKEN@@'
export AWS_DEFAULT_REGION=@@REGION@@ AWS_REGION=@@REGION@@
export GLIDER_S3_BUCKET=@@BUCKET@@ GLIDER_S3_REGION=@@REGION@@ GLIDER_M24_ROWS=@@ROWS@@
export HOME=/root CARGO_TERM_COLOR=never
RESULTS=s3://@@BUCKET@@/@@PREFIX@@/results/@@RUN@@
NAMESPACE=@@PREFIX@@/@@RUN@@
LOG=/var/log/glider-run.log
exec >"$LOG" 2>&1
upload_log() { aws s3 cp --only-show-errors "$LOG" "$RESULTS/run.log" || true; }
finish() {
    status=$?
    [ "$status" -eq 0 ] || echo "FAILED with status $status" >"/tmp/FAILED"
    [ -f /tmp/FAILED ] && aws s3 cp --only-show-errors /tmp/FAILED "$RESULTS/FAILED"
    # Remove the test namespace and its backup whatever happened.
    aws s3 rm --only-show-errors --recursive "s3://@@BUCKET@@/$NAMESPACE/" || true
    aws s3 rm --only-show-errors --recursive "s3://@@BUCKET@@/$NAMESPACE-backup/" || true
    upload_log
    shutdown -h now
}
trap finish EXIT
set -e
(while sleep 60; do upload_log; done) &

dnf install -y -q git gcc
curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. /root/.cargo/env
git clone -q https://github.com/omerfeyzioglu/glider.git /opt/glider
cd /opt/glider
git checkout -q @@REVISION@@
cargo build -q --locked --release --features s3 --example m24_acceptance
echo "built $(git rev-parse HEAD)"

mkdir -p /opt/data/sift && cd /opt/data/sift
aws s3 cp --only-show-errors "s3://@@BUCKET@@/@@PREFIX@@/datasets/sift_base.fvecs" .
aws s3 cp --only-show-errors "s3://@@BUCKET@@/@@PREFIX@@/datasets/sift_query.fvecs" .
sha256sum -c - <<'SUMS'
21f66e2975057b5728ba56de1c825bac4f4d89d596609ae985741c6242631816  sift_base.fvecs
f7fc9be140accdfd64116c2fa2365ecdb69b8f084970c6b0532db5ff79ac8fdc  sift_query.fvecs
SUMS
cd /opt/glider
BIN=target/release/examples/m24_acceptance
BASE=/opt/data/sift/sift_base.fvecs QUERY=/opt/data/sift/sift_query.fvecs
CACHE=/opt/cache && mkdir -p "$CACHE"
ORACLE=$(python3 -c "import sys; sys.path.insert(0, 'tools'); from m24_acceptance import ENVELOPES; print(ENVELOPES[@@ROWS@@]['oracle'])")

echo "load $(date -u +%T)"
$BIN load "$BASE" "$NAMESPACE" >/opt/load.json
echo "serve $(date -u +%T)"
$BIN serve "$QUERY" "$BASE" "$NAMESPACE" "$CACHE" "$ORACLE" @@ROUNDS@@ >/opt/serve.json
VISIBLE=$(aws s3 ls --recursive --summarize "s3://@@BUCKET@@/$NAMESPACE/" | awk '/Total Size/ {print $3}')
echo "verify $(date -u +%T)"
$BIN verify "$QUERY" "$BASE" "$NAMESPACE" "$CACHE" /opt/serve.json "$NAMESPACE-backup" >/opt/verify.json
python3 tools/aws_result.py /opt/load.json /opt/serve.json /opt/verify.json "$VISIBLE" @@ROWS@@ "$ORACLE" >/opt/run.json
aws s3 cp --only-show-errors /opt/run.json "$RESULTS/run.json"
echo "done $(date -u +%T)"
