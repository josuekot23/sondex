#!/bin/bash

DOSSIER="$(cd "$(dirname "$0")" && pwd)"
PYTHON_VENV="$DOSSIER/venv/bin/python"

if ps -aux | grep "$DOSSIER/mesures.py" | grep -v grep > /dev/null
then
    echo "En cours..."
else
    "$PYTHON_VENV" "$DOSSIER/mesures.py" &
fi