#!/usr/bin/env python3
"""Refresh the bundled Google model limits; never run as part of CLI startup."""

import datetime
import json
import pathlib
import urllib.request


def main():
    source = "https://models.dev/api.json"
    request = urllib.request.Request(
        source, headers={"User-Agent": "orchestral-model-metadata/1"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        catalog = json.load(response)
    snapshot = {
        "_meta": {
            "source": source,
            "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    }
    for provider in ("google", "google-vertex"):
        models = {}
        for name, model in catalog[provider]["models"].items():
            limits = {
                key: value
                for key, value in model.get("limit", {}).items()
                if key in ("context", "input", "output")
                and type(value) is int
                and value > 0
            }
            if "context" in limits or "input" in limits:
                models[name] = {"limit": limits}
        if not models:
            raise ValueError(f"No valid model capacities returned for {provider}")
        snapshot[provider] = {"models": models}
    destination = (
        pathlib.Path(__file__).resolve().parents[1]
        / "plugins/orchestral-model-gemini/src/metadata/catalog.json"
    )
    destination.write_text(
        json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"Updated {destination}")


if __name__ == "__main__":
    main()
