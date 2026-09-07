#!/usr/bin/env python3
"""
main.py — the real Airbyte low-code CDK connector entrypoint.

*** NOT VERIFIED IN THIS ENVIRONMENT ***
No network access was available to `pip install airbyte-cdk` or execute this
file when it was written. It's built from the documented low-code CDK
pattern (YamlDeclarativeSource + AirbyteEntrypoint.launch), the same
structure Airbyte's connector generator produces — but it has not been run.
Before trusting it, at minimum run `python main.py spec` first (the cheapest
command — it doesn't need network or the mock server) and confirm it prints
the JSON Schema from manifest.yaml's spec: block without error.

If YamlDeclarativeSource's constructor keyword has changed in the
airbyte-cdk version you install, that's the first thing to check — this
script tries the two most likely names and reports clearly if neither works,
rather than failing with an unrelated-looking traceback.
"""
import sys
from pathlib import Path

MANIFEST_PATH = Path(__file__).parent / "manifest.yaml"


def _build_source():
    from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource

    last_error = None
    for kwarg_name in ("path_to_yaml_file", "path_to_yaml"):
        try:
            return YamlDeclarativeSource(**{kwarg_name: str(MANIFEST_PATH)})
        except TypeError as exc:
            last_error = exc
    raise RuntimeError(
        "Could not construct YamlDeclarativeSource with either "
        "'path_to_yaml_file' or 'path_to_yaml' — the installed airbyte-cdk "
        "version likely renamed this parameter. Check "
        "`import airbyte_cdk; help(airbyte_cdk.sources.declarative."
        "yaml_declarative_source.YamlDeclarativeSource.__init__)` "
        f"to find the current signature. Last error: {last_error}"
    ) from last_error


def run():
    from airbyte_cdk.entrypoint import launch

    source = _build_source()
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    run()
