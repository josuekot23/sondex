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
echo "Pour activer l'environnement virtuel : source venv/bin/activate"
echo "Pour exécuter votre script : python votre_script.py"