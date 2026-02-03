#!/usr/bin/env python3
import json
import sys


def main() -> int:
    try:
        req = json.load(sys.stdin)
        params = ((req.get("input") or {}).get("params") or {})
        message = params.get("message", "")
        resp = {
            "type": "success",
            "exports": {
                "result": f"external:{message}",
            },
        }
    except Exception as exc:
        resp = {
            "type": "error",
            "message": f"plugin failed: {exc}",
        }
    json.dump(resp, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
