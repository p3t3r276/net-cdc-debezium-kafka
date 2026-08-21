#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="appdb-connector"
CONFIG_FILE="${CONFIG_FILE:-/connector-config.json}"

echo "Cho Kafka Connect san sang tai ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null; do
  echo "  ...chua san sang, thu lai sau 3s"
  sleep 3
done
echo "Connect da san sang."

# PUT /connectors/<ten>/config = upsert: tao moi neu chua co, cap nhat neu da co.
# Chay lai bao nhieu lan cung ra cung ket qua -> an toan cho CI/CD.
echo "Register/Update connector '${CONNECTOR_NAME}' ..."
curl -sf -X PUT \
  "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
  -H "Content-Type: application/json" \
  -d "@${CONFIG_FILE}" \
  | tee /tmp/connector-response.json

echo ""
echo "Connector state:"
curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || true
echo ""
echo "Finished."
