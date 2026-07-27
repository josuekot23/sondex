if ps -aux | grep temperatures.py | grep -v grep
then 
    echo "En cours..."
else
    python /home/logger/temperature.py . &
fi