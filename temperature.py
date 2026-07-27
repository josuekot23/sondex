########################################################################
# import des librairies
########################################################################
import subprocess, os, sys, json
import paho.mqtt.client as mqtt
import binascii
import logging
import signal
import psutil
import time
import socket


########################################################################
# Definition des variables
########################################################################

#Recupération de l'IP du serveur
with open("target.txt","r") as f:
	config = f.read().strip()


#Configuration MQTT
MQTT_HOST = config
MQTT_PORT = 1883
MQTT_KEEPALIVE_INTERVAL = 45
MQTT_TOPIC = "logger"
SITE=socket.gethostname()
########################################################################
# configuration des messages de logs
########################################################################
logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

########################################################################
# buffer local a rotation en cas de broker injoignable
########################################################################
import glob
DOSSIER = os.path.dirname(os.path.abspath(__file__))

BUFFER_DIR = os.path.join(DOSSIER, "mqtt_buffer_mesures")
MAX_LIGNES_PAR_FICHIER = 5000
PREFIXE_FICHIER = "buffer_"

os.makedirs(BUFFER_DIR, exist_ok=True)

def _fichiers_buffer_tries():
        fichiers = glob.glob(os.path.join(BUFFER_DIR, PREFIXE_FICHIER + "*.jsonl"))
        return sorted(fichiers)

def _nouveau_nom_fichier():
        existants = _fichiers_buffer_tries()
        if not existants:
                n = 1
        else:
                dernier = os.path.basename(existants[-1])
                num = int(dernier.replace(PREFIXE_FICHIER, "").replace(".jsonl", ""))
                n = num + 1
        return os.path.join(BUFFER_DIR, PREFIXE_FICHIER + str(n).zfill(8) + ".jsonl")

def _mettre_en_buffer(topic, data):
        fichiers = _fichiers_buffer_tries()

        if fichiers:
                fichier_courant = fichiers[-1]
                with open(fichier_courant, "r") as f:
                        nb_lignes = sum(1 for _ in f)
        else:
                fichier_courant = None
                nb_lignes = MAX_LIGNES_PAR_FICHIER

        if nb_lignes >= MAX_LIGNES_PAR_FICHIER:
                fichier_courant = _nouveau_nom_fichier()
                logging.warning('buffer plein, nouveau fichier cree : ' + fichier_courant)

        with open(fichier_courant, "a") as f:
                f.write(json.dumps({"topic": topic, "data": data}) + "\n")

        total = sum(1 for fic in _fichiers_buffer_tries() for _ in open(fic))
        logging.warning('broker injoignable, message mis en buffer local (' + str(total) + ' en attente au total)')

def _vider_buffer(mqttc):
        fichiers = _fichiers_buffer_tries()
        if not fichiers:
                return

        total_rejoues = 0
        for fichier in fichiers:
                with open(fichier, "r") as f:
                        lignes = [l for l in f.read().splitlines() if l.strip()]

                restantes = []
                echec = False
                for i, l in enumerate(lignes):
                        if echec:
                                restantes.append(l)
                                continue
                        try:
                                msg = json.loads(l)
                                mqttc.publish(msg["topic"], msg["data"])
                                total_rejoues += 1
                        except Exception:
                                echec = True
                                restantes.append(l)

                if restantes:
                        with open(fichier, "w") as f:
                                f.write("\n".join(restantes) + "\n")
                else:
                        os.remove(fichier)

                if echec:
                        break

        if total_rejoues:
                logging.info('buffer local : ' + str(total_rejoues) + ' messages rejoués en rafale')

########################################################################
# fonction d'envoi des messages MQTT
########################################################################
def envoyer_message(topic,data):
        try:
                # initialisation du client MQTT
                mqttc = mqtt.Client()

                # connexion au broker MQTT
                mqttc.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)

                # on rejoue d'abord les messages en attente depuis une eventuelle coupure
                _vider_buffer(mqttc)

                # publication du message
                mqttc.publish(topic,data)

                # ajout d'un message de log sur la console
                logging.info('message envoyé:' + data)
                # deconnexion du broker MQTT
                mqttc.disconnect()
        except Exception as e:
                logging.error('échec envoi MQTT (broker injoignable ?) : ' + str(e))
                _mettre_en_buffer(topic, data)



###################################################################################
# Début du programme
###################################################################################
logging.info('Début du programme de mesure')

while(True):

        # temporisation
        time.sleep(3)

        # ligne de commande du programme de mesure
        cmd ="vcgencmd measure_temp"

        # execution de la commande au niveau du systeme d exploitation
        p = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,encoding='utf8')

        # lecture de la sortie standard 
        for line in iter(p.stdout.readline, ''):
             # message de log
                logging.info('ligne en cours de traitement:' + line.rstrip())

                # recuperation de la temperature
                line = line.strip()
                line = line.replace("temp=","")
                line = line.replace("'C","")

                # timestamp de la mesure (nanosecondes)
                sts = str(time.time_ns())

                # envoi de l information
                mqtt_msg="{\"site\":\""+SITE+"\", \"temperature\":"+line+", \"timestamp\":\""+sts+"\"}"
                envoyer_message("temperature",mqtt_msg)
