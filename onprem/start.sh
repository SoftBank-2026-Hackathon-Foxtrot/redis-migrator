#!/bin/sh
# First boot only: from then on users.acl belongs to Valkey, and ACL SAVE keeps the fence in it.
set -eu

acl=/data/users.acl
if [ ! -f "$acl" ]; then
    hash() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }
    umask 077
    {
        echo "user default on #$(hash "$VALKEY_ADMIN_PASSWORD") ~* &* +@all"
        echo "user app on #$(hash "$VALKEY_APP_PASSWORD") ~* &* +@all"
    } > "$acl"
fi

exec docker-entrypoint.sh valkey-server /etc/valkey-onprem/valkey.conf --maxmemory "$VALKEY_MAXMEMORY"
