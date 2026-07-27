DOSSIER="$(cd "$(dirname "$0")" && pwd)"

if ps -aux | grep "$DOSSIER/temperature.py" | grep -v grep
then
    echo "En cours..."
else
    "$DOSSIER/venv/bin/python" "$DOSSIER/temperature.py" &
fi