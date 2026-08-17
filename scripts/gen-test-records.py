#!/usr/bin/env python3
"""Generate synthetic records with valid CIDv1 keys and submit them to a
registryd write API in batches. For sandbox testing only — see
docs/testing-guide.md, "Local sandbox".

    gen-test-records.py <count> <api-base> <token> [--offset N]

Keys are deterministic in (offset+i), so re-running the same range is an
idempotent no-op on the registry (same key, same value, same hashes) —
use --offset to submit genuinely new records. Each submitted key is
derivable later: cidv1(raw, sha2-256) over b"sandbox-record-<n>".
"""
import base64
import hashlib
import json
import sys
import urllib.request

BATCH = 500  # registryd's default batch_max_records


def cidv1_raw(payload: bytes) -> str:
    digest = hashlib.sha256(payload).digest()
    # multibase 'b' + (version 1, codec raw 0x55, sha2-256 multihash)
    cid_bytes = bytes([0x01, 0x55, 0x12, 0x20]) + digest
    return "b" + base64.b32encode(cid_bytes).decode().lower().rstrip("=")


def main() -> None:
    if len(sys.argv) < 4:
        sys.exit(__doc__)
    count, api, token = int(sys.argv[1]), sys.argv[2].rstrip("/"), sys.argv[3]
    offset = 0
    if "--offset" in sys.argv:
        offset = int(sys.argv[sys.argv.index("--offset") + 1])

    batch, sent, first_key = [], 0, None
    for n in range(offset, offset + count):
        key = cidv1_raw(f"sandbox-record-{n}".encode())
        first_key = first_key or key
        value = json.dumps(
            {
                "location": f"sandbox://record/{n}",
                "iscc": "ISCC:TESTTESTTESTTEST",
                "seq": n,
                "timestamp": "2026-08-05T00:00:00Z",
            }
        )
        batch.append({"key": key, "value": value})
        if len(batch) == BATCH or n == offset + count - 1:
            req = urllib.request.Request(
                f"{api}/v1/records/batch",
                data=json.dumps({"records": batch}).encode(),
                headers={
                    "content-type": "application/json",
                    "authorization": f"Bearer {token}",
                },
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                resp.read()
            sent += len(batch)
            batch = []
            if sent % 5000 == 0:
                print(f"submitted {sent}/{count}", flush=True)

    print(f"done: {sent} records submitted")
    print(f"first key (for storectl get / viewer lookup): {first_key}")


if __name__ == "__main__":
    main()
