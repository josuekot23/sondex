if ps -aux | grep mesures.py | grep -v grep
then 
    echo "En cours..."
else
    python /home/logger/mesures.py . &
fi