if ps -aux | grep /usr/sbin/mosquitto | grep -v grep
then 
    echo "En cours..."
else
    sudo service mosquitto start
fi