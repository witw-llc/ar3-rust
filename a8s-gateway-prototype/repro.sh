#!/usr/bin/env bash
# Reproduce known defects 1, 2, 3 and 5 (see KNOWN-DEFECTS.md) on loopback, with the synthetic test identities.
# Usage: repro.sh <path to built ar3-server-prototype> <scratch dir>
set -u
B=$1; D=$2; PORT=18090; U=http://127.0.0.1:$PORT
start() { rm -rf "$D"; mkdir -m 700 "$D" "$D/objects"; "$B" init "$D/state.sqlite" 00000000000000000000000000 >/dev/null
  ("$B" serve "$D/state.sqlite" "$D/objects" 127.0.0.1:$PORT >"$D/serve.log" 2>&1 &); /bin/sleep 1; }
stop() { pkill -f "ar3-server-prototype serve $D" ; /bin/sleep 1; }
post() { # who key path body -> prints "code body"
  curl -s -w '\n%{http_code}' "$U$3" -H "Authorization: Bearer $1" -H "Idempotency-Key: $2" -H 'Content-Type: application/json' --data "$4" | tr '\n' ' '; echo; }
field() { python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2], {'d': d}))" "$1" "$2"; }

echo "=== defect 3: every claim or poll leaves a permanent record; the cap then refuses all new work"
start
before=$(post analyst-test f3-before /v1/messages '{"recipient":"builder","content":"before","attachment_ids":[]}' | awk '{print $NF}')
for i in $(seq 1 5000); do curl -s -o /dev/null "$U/v1/inboxes/builder/claims" -H 'Authorization: Bearer builder-test' -H "Idempotency-Key: f3-claim-$i" -H 'Content-Type: application/json' --data '{"consumer_id":"builder-device","max_messages":1,"wait_seconds":0}'; done
after=$(post analyst-test f3-after /v1/messages '{"recipient":"builder","content":"after","attachment_ids":[]}')
echo "send before 5000 empty claims: $before | send after: $after"
stop

echo "=== defect 1: uploads fill the database page cap long before the record cap"
start
head -c 1048576 /dev/zero > "$D/one-mib.bin"
for i in $(seq 1 34); do code=$(curl -s -o "$D/up.out" -w '%{http_code}' "$U/v1/attachments" -H 'Authorization: Bearer analyst-test' -H "Idempotency-Key: f1-up-$i" -H "X-Attachment-Filename: f$i.bin" -H 'Content-Type: application/octet-stream' --data-binary @"$D/one-mib.bin"); [ "$code" != "201" ] && [ "$code" != "200" ] && { echo "upload $i -> $code $(cut -c1-120 "$D/up.out")"; break; }; done
echo "send after the uploads: $(post analyst-test f1-send /v1/messages '{"recipient":"builder","content":"after uploads","attachment_ids":[]}')"
echo "claim after the uploads: $(post builder-test f1-claim /v1/inboxes/builder/claims '{"consumer_id":"builder-device","max_messages":1,"wait_seconds":0}' | cut -c1-160)"
stop

echo "=== defect 5: a repeated disposition after a re-claim returns 409, not the first result"
start
r=$(curl -s "$U/v1/messages" -H 'Authorization: Bearer analyst-test' -H 'Idempotency-Key: f5-send' -H 'Content-Type: application/json' --data '{"recipient":"builder","content":"task","attachment_ids":[]}')
op=$(field "$r" "d['operation_id']")
echo "publish: $(post analyst-test f5-pub /v1/operations/$op/publish '{}' | cut -c1-120)"
echo "poll: $(post builder-test f5-poll /v1/inboxes/builder/poll '{}' | cut -c1-120)"
c=$(curl -s "$U/v1/inboxes/builder/claims" -H 'Authorization: Bearer builder-test' -H 'Idempotency-Key: f5-claim-1' -H 'Content-Type: application/json' --data '{"consumer_id":"builder-device","max_messages":1,"wait_seconds":0}')
echo "claim 1: $(echo "$c" | cut -c1-200)"
gen=$(field "$c" "d['claims'][0]['lease_generation']"); proof=$(field "$c" "d['claims'][0]['lease_proof']"); did=$(field "$c" "d['claims'][0]['delivery_id']")
body="{\"lease_generation\":$gen,\"lease_proof\":\"$proof\",\"outcome\":\"retry\",\"handling_reference\":\"first-try\"}"
echo "disposition retry, key K (first time): $(post builder-test f5-K /v1/deliveries/$did/disposition "$body" | cut -c1-160)"
echo "same request, key K, before any re-claim: $(post builder-test f5-K /v1/deliveries/$did/disposition "$body" | cut -c1-160)"
/bin/sleep 8
echo "claim 2 (after backoff): $(post builder-test f5-claim-2 /v1/inboxes/builder/claims '{"consumer_id":"builder-device","max_messages":1,"wait_seconds":0}' | cut -c1-120)"
echo "same request, key K, after the re-claim: $(post builder-test f5-K /v1/deliveries/$did/disposition "$body" | cut -c1-160)"
stop

echo "=== defect 2: after restore and cutover, mail published before the cutover is never delivered"
start
r=$(curl -s "$U/v1/messages" -H 'Authorization: Bearer analyst-test' -H 'Idempotency-Key: f2-send' -H 'Content-Type: application/json' --data '{"recipient":"builder","content":"published before the stop","attachment_ids":[]}')
op=$(field "$r" "d['operation_id']")
echo "publish: $(post analyst-test f2-pub /v1/operations/$op/publish '{}' | cut -c1-120)"
stop
"$B" backup "$D/state.sqlite" "$D/snap.sqlite" >/dev/null; "$B" restore "$D/snap.sqlite" "$D/restored.sqlite" >/dev/null; "$B" cutover "$D/restored.sqlite" >/dev/null
("$B" serve "$D/restored.sqlite" "$D/objects" 127.0.0.1:$PORT >"$D/serve2.log" 2>&1 &); /bin/sleep 1
echo "poll after cutover: $(post builder-test f2-poll /v1/inboxes/builder/poll '{}' | cut -c1-120)"
echo "claim after cutover: $(post builder-test f2-claim /v1/inboxes/builder/claims '{"consumer_id":"builder-device","max_messages":1,"wait_seconds":0}' | cut -c1-120)"
pkill -f "ar3-server-prototype serve $D"
