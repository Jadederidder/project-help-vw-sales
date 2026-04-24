# Fixtures

Drop the real cp1252-encoded sample CSV in here as:

    scripts/fixtures/sample_cancellations.csv

It must be the raw file from the VW cancellation email (do not re-save it
through a text editor — Preview, VS Code, etc. will silently convert it
to UTF-8 and destroy the 0x96 byte that represents the en-dash).

`tests/test_encoding.py` asserts:
- fixture exists
- fixture contains byte `0x96` (the cp1252 en-dash)
- decoding the fixture as cp1252 yields `U+2013` (en-dash)

Run `pytest tests/test_encoding.py` after dropping it in.
