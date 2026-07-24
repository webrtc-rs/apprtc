#!/bin/bash

# 5. Stop all three services.
kill $(pgrep -f "target/debug/(appweb|signaling|sfu)") || true