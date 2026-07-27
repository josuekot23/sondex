#!/bin/bash

echo "=== Configuration de l'environnement virtuel ==="

# Vérifier si venv existe
if [ ! -d "venv" ]; then
    echo "Création de l'environnement virtuel..."
    python3 -m venv venv
else
    echo "Environnement virtuel existant"
fi

# Activer l'environnement virtuel
source venv/bin/activate

# Mettre à jour pip
pip install --upgrade pip

# Installer les dépendances
echo "Installation des dépendances dans l'environnement virtuel..."
pip install -r requirements.txt

echo "=== Installation terminée ! ==="


# Remplissage de la crontab 
echo "Configuration du lancement automatique au démarrage..."

# Chemin absolu du dossier sondex
DOSSIER="$(cd "$(dirname "$0")" && pwd)"
# Chemin vers le Python de l'environnement virtuel
PYTHON_VENV="$DOSSIER/venv/bin/python"

# MESURE 

# Chemin vers le script Python : mesures
SCRIPT="$DOSSIER/mesures.py"
# Ajout de la tâche cron au démarrage (si elle n'existe pas déjà)
CRON_JOB="@reboot $PYTHON_VENV $SCRIPT >> $DOSSIER/mesures.log 2>&1"
(crontab -l 2>/dev/null | grep -v "$SCRIPT"; echo "$CRON_JOB") | crontab -
echo "Crontab configurée : mesures.py sera lancé au démarrage."

# Chemin vers le script Python : mesures
SCRIPT="$DOSSIER/temperature.py"
# Ajout de la tâche cron au démarrage (si elle n'existe pas déjà)
CRON_JOB="@reboot $PYTHON_VENV $SCRIPT >> $DOSSIER/temperature.log 2>&1"
(crontab -l 2>/dev/null | grep -v "$SCRIPT"; echo "$CRON_JOB") | crontab -
echo "Crontab configurée : temperature.py sera lancé au démarrage."


# Remplissage de la crontab : check mesures
SCRIPT_SH="$DOSSIER/check_mesures.sh"
# Rendre le script exécutable
chmod +x "$SCRIPT_SH"
# Ajout dans la crontab au démarrage
CRON_JOB="@reboot sleep 30 && $SCRIPT_SH >> $DOSSIER/check_mesures.log 2>&1"
(crontab -l 2>/dev/null | grep -v "$SCRIPT_SH"; echo "$CRON_JOB") | crontab -
echo "Crontab configurée : check_mesures.sh sera lancé au démarrage."

# Remplissage de la crontab : check mosquitto
SCRIPT_SH="$DOSSIER/check_mosquitto.sh"
# Rendre le script exécutable
chmod +x "$SCRIPT_SH"
# Ajout dans la crontab au démarrage
CRON_JOB="@reboot sleep 30 && $SCRIPT_SH >> $DOSSIER/check_mosquitto.log 2>&1"
(crontab -l 2>/dev/null | grep -v "$SCRIPT_SH"; echo "$CRON_JOB") | crontab -
echo "Crontab configurée : check_mosquitto.sh sera lancé au démarrage."

# Remplissage de la crontab : check mesures
SCRIPT_SH="$DOSSIER/check_temperature.sh"
# Rendre le script exécutable
chmod +x "$SCRIPT_SH"
# Ajout dans la crontab au démarrage
CRON_JOB="@reboot sleep 30 && $SCRIPT_SH >> $DOSSIER/check_temperature.log 2>&1"
(crontab -l 2>/dev/null | grep -v "$SCRIPT_SH"; echo "$CRON_JOB") | crontab -
echo "Crontab configurée : check_temperature.sh sera lancé au démarrage."