#!/bin/sh
set -eu

: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"

mkdir -p /run/urbanflow
cat > /run/urbanflow/s3.json <<EOF
{
  "identities": [
    {
      "name": "urbanflow-local",
      "credentials": [
        {
          "accessKey": "${AWS_ACCESS_KEY_ID}",
          "secretKey": "${AWS_SECRET_ACCESS_KEY}"
        }
      ],
      "actions": ["Admin", "Read", "List", "Tagging", "Write"]
    }
  ]
}
EOF
chown seaweed:seaweed /run/urbanflow/s3.json
chmod 600 /run/urbanflow/s3.json

exec /entrypoint.sh "$@"
