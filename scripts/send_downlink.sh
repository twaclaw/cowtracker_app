#!/bin/bash
DEV=$1
PORT=$2
PAYLOAD=$3

echo $LNS_KEY

payload=$(echo $PAYLOAD|xxd -r -p|base64)
message="{\"downlinks\":[{\"f_port\":${PORT}, \"priority\": \"HIGH\", \"frm_payload\":\"${payload}\"}]}"

echo "Sending ${message}"
MESSAGE""message
mosquitto_pub -h nam1.cloud.thethings.network -t "v3/${APP_ID}@ttn/devices/${DEV}/down/replace" -u "${APP_ID}" -P $LNS_KEY  -m "${message}" -d

exit 0