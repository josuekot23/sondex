# !/usr/bin/python

########################################################################
# import des librairies
########################################################################
import subprocess, os, sys, json
import paho.mqtt.client as mqtt
import binascii
import logging
import signal
import psutil
from datetime import datetime, timedelta
import time
import socket
os.chdir(os.path.dirname(os.path.abspath(__file__)))

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
print(MQTT_HOST)
#SITE="Brest"
SITE=socket.gethostname()
frequences = ["474000000","482000000","490000000","498000000","506000000","514000000","522000000","530000000","538000000","546000000","554000000","562000000","570000000","578000000","586000000","594000000","602000000","610000000","618000000","626000000","634000000","642000000","650000000","658000000","666000000","674000000","682000000","690000000"]
#frequences = ["474000000"]
dictionnaireFrequences = {}
dictionnaireFrequences["474000000"] = ""
dictionnaireFrequences["482000000"] = ""
dictionnaireFrequences["490000000"] = ""
dictionnaireFrequences["498000000"] = ""
dictionnaireFrequences["506000000"] = ""
dictionnaireFrequences["514000000"] = ""
dictionnaireFrequences["522000000"] = ""
dictionnaireFrequences["530000000"] = ""
dictionnaireFrequences["538000000"] = ""
dictionnaireFrequences["546000000"] = ""
dictionnaireFrequences["554000000"] = ""
dictionnaireFrequences["562000000"] = ""
dictionnaireFrequences["570000000"] = ""
dictionnaireFrequences["578000000"] = ""
dictionnaireFrequences["586000000"] = ""
dictionnaireFrequences["594000000"] = ""
dictionnaireFrequences["602000000"] = ""
dictionnaireFrequences["610000000"] = ""
dictionnaireFrequences["618000000"] = ""
dictionnaireFrequences["626000000"] = ""
dictionnaireFrequences["634000000"] = ""
dictionnaireFrequences["642000000"] = ""
dictionnaireFrequences["650000000"] = ""
dictionnaireFrequences["658000000"] = ""
dictionnaireFrequences["666000000"] = ""
dictionnaireFrequences["674000000"] = ""
dictionnaireFrequences["682000000"] = ""
dictionnaireFrequences["690000000"] = ""


########################################################################
# configuration des messages de logs
########################################################################
logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        #filename='/tmp/mesures.log',
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
        # liste les fichiers de buffer dans l'ordre chronologique (le plus ancien d'abord)
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
                nb_lignes = MAX_LIGNES_PAR_FICHIER  # force la creation du 1er fichier

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
                                # le broker a re-coupe en cours de vidage : on arrete
                                # et on garde le reste de ce fichier + tous les suivants intacts
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


        dictionnaireFrequences = {}
        dictionnaireFrequences["474000000"] = ""
        dictionnaireFrequences["482000000"] = ""
        dictionnaireFrequences["490000000"] = ""
        dictionnaireFrequences["498000000"] = ""
        dictionnaireFrequences["506000000"] = ""
        dictionnaireFrequences["514000000"] = ""
        dictionnaireFrequences["522000000"] = ""
        dictionnaireFrequences["530000000"] = ""
        dictionnaireFrequences["538000000"] = ""
        dictionnaireFrequences["546000000"] = ""
        dictionnaireFrequences["554000000"] = ""
        dictionnaireFrequences["562000000"] = ""
        dictionnaireFrequences["570000000"] = ""
        dictionnaireFrequences["578000000"] = ""
        dictionnaireFrequences["586000000"] = ""
        dictionnaireFrequences["594000000"] = ""
        dictionnaireFrequences["602000000"] = ""
        dictionnaireFrequences["610000000"] = ""
        dictionnaireFrequences["618000000"] = ""
        dictionnaireFrequences["626000000"] = ""
        dictionnaireFrequences["634000000"] = ""
        dictionnaireFrequences["642000000"] = ""
        dictionnaireFrequences["650000000"] = ""
        dictionnaireFrequences["658000000"] = ""
        dictionnaireFrequences["666000000"] = ""
        dictionnaireFrequences["674000000"] = ""
        dictionnaireFrequences["682000000"] = ""
        dictionnaireFrequences["690000000"] = ""



        # ligne de commande du programme de mesure
        cmd ="sudo dvbv5-scan DVB_ALL --output=output.scan"

        # execution de la commande au niveau du systeme d exploitation
        p = subprocess.call(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,encoding='utf8')

        cmd = "more output.scan | grep -e '\[' -e 'FREQUENCY'  | paste - - | sed 's/[[:blank:]]//g' | sed 's/\[//g' | sed 's/\]FREQUENCY=/;/g' > output.chaine"
        # execution de la commande au niveau du systeme d exploitation
        p = subprocess.call(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,encoding='utf8')


        with open("output.chaine") as file:
                while (line := file.readline().rstrip()):
                        tmp = line.split(";")
                        dictionnaireFrequences[tmp[1]] = dictionnaireFrequences[tmp[1]] + ' '+ tmp[0]

        for i in range(5):

                # pour chaque frequence de liste definie 
                for freq in frequences:


                        # temporisation pour limiter le nombre de mesures
        #               time.sleep(600)

                        # message de log        
                        logging.info('Fréquence sélectionnée : ' + freq )

                        # compteur  
                        cntNbLignesNonLock = 0
                         # au départ la fréquence n'est pas verouillee
                        freqIsLocked = False

                        # ligne de commande du programme de mesure
                        cmd ="sudo dvbv5-zap -c DVB_ALL -t 5 " + freq 

                        # execution de la commande au niveau du systeme d exploitation
                        p = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,encoding='utf8')

                        # lecture de la sortie standard 
                        for line in iter(p.stdout.readline, ''):

                                # message de log
                                logging.info('ligne en cours de traitement:' + line.rstrip())

                                # si le programme renvoie une ligne avec les mots Quality et Signal
                                if "Quality" in line and "Signal" in line:

                                        # la frequence est verouillee   
                                        freqIsLocked = True
                                        logging.info('la fréquence est vérouillée')

                                        line = line.replace("= ",":")
                                        line = line.replace("Lock   (0x1f) ","")
                                        line = line.replace("Lock   (0x0f) ","")
                                        mesures = line.split()

                                        quality = mesures[0]
                                        qualityTab = quality.split(":")
                                        logging.info(qualityTab[0])
                                        qualityData = qualityTab[1] 
                                        #logging.info('Qualité du signal : ' + qualityData )

                                        signal = mesures[1]
                                        signalTab = signal.split(":")
                                        signalDataTmp = signalTab[1].replace("dBm","")
                                        signalDataTmp = signalDataTmp.replace(",",".")
                                        signalData =  float(signalDataTmp)
                                        extrapolationData = float(signalDataTmp)
                                        extrapolationData = extrapolationData + 123.75
                                        #logging.info('Mesure du signal : ' + signalDataTmp )
                                        
                                        cn = mesures[2]
                                        cnTab = cn.split(":")
                                        cnDataTmp = cnTab[1].replace("dB","")
                                        cnDataTmp = cnDataTmp.replace(",",".")
                                        #cnData = float(cnDataTmp)
                                        cnData = cnDataTmp

                                        ucb = mesures[3]
                                        ucbTab = ucb.split(":")
                                        ucbData = ucbTab[1]

                                        postber = mesures[4]
                                        postberTab = postber.split(":")
                                        postberData = postberTab[1]
                                        postberData = postberData.replace("x10^","E")

                                        preber = mesures[5]
                                        preberTab = preber.split(":")
                                        preberData = preberTab[1]
                                        preberData = preberData.replace("x10^","E")
                                        
                                        per = mesures[6]
                                        perTab = per.split(":")
                                        perData = perTab[1]


                                        # Getting the current date and time
                                        dt = datetime.now()- timedelta(hours=1, minutes=30)
                                        #dt = datetime.now()

                                        # getting the timestamp
                                        ts = datetime.timestamp(dt)
                                        sts = str(int( ts * 1000000000 ))
                                        #sts=str(ts)

                                        sts = str(time.time_ns() )

                                        mqtt_msg="{\"site\":\""+SITE+"\",\"frequence\":\""+freq+":"+dictionnaireFrequences[freq]+"\", \"signal\":"+signalDataTmp+", \"timestamp\":\""+sts+"\"}"
                                        envoyer_message("signal",mqtt_msg)

                                        mqtt_msg="{\"site\":\""+SITE+"\",\"frequence\":"+freq+", \"cn\":"+cnData+", \"timestamp\":\""+sts+"\"}"
                                        envoyer_message("cn",mqtt_msg)

                                        mqtt_msg="{\"site\":\""+SITE+"\",\"frequence\":"+freq+", \"postBER\":"+postberData+", \"timestamp\":\""+sts+"\"}"
                                        envoyer_message("postber",mqtt_msg)

                                        mqtt_msg="{\"site\":\""+SITE+"\",\"frequence\":"+freq+", \"preBER\":"+preberData+", \"timestamp\":\""+sts+"\"}"
                                        envoyer_message("preber",mqtt_msg)

                                        mqtt_msg="{\"site\":\""+SITE+"\",\"frequence\":"+freq+", \"extrapolation\":"+str(extrapolationData)+", \"timestamp\":\""+sts+"\"}"
                                        envoyer_message("extrapolation",mqtt_msg)

                                else:
                                        if "C/N" not in line and "Signal" in line and freqIsLocked == False:

                                                logging.info('fréquence non vérouillée')

                                                cntNbLignesNonLock = cntNbLignesNonLock +1
                                                # Getting the current date and time
                                                dt = datetime.now()- timedelta(hours=1, minutes=30)
                                                #dt = datetime.now()

                                                # getting the timestamp
                                                ts = datetime.timestamp(dt)
                                                sts = str(int( ts * 1000000000 ))
                                                #sts=str(ts)
                                                
                                                sts = str(time.time_ns() )

                                                line = line.replace("= ",":")
                                                line = line.replace("(0x00)","")
                                                line = line.replace("(0x01)","")
                                                mesures = line.split()

                                                signal = mesures[0]
                                                signalTab = signal.split(":")
                                                signalDataTmp = signalTab[1].replace("dBm","")
                                                signalDataTmp = signalDataTmp.replace(",",".")
                                                signalData = float(signalDataTmp)
                                                extrapolationData = float(signalDataTmp)
                                                extrapolationData = extrapolationData + 123.75

                                                #mqtt_msg="{\"site\":\""+SITE+"\",\"frequence\":"+freq+", \"signal\":"+signalDataTmp+"}"
                                                mqtt_msg="{\"site\":\""+SITE+"\",\"frequence\":\""+freq+":"+dictionnaireFrequences[freq]+"\", \"signal\":"+signalDataTmp+", \"timestamp\":\""+sts+"\"}"
                                                envoyer_message("signal",mqtt_msg)
                                                mqtt_msg="{\"site\":\""+SITE+"\",\"frequence\":"+freq+", \"extrapolation\":"+str(extrapolationData)+", \"timestamp\":\""+sts+"\"}"
                                                envoyer_message("extrapolation",mqtt_msg)


                                        if "C/N" not in line and "Signal" in line and freqIsLocked == False and cntNbLignesNonLock == 2:
                                            logging.info('Fréquence non vérouillée -> fin du process')
                                            current_process = psutil.Process()
                                            children = current_process.children(recursive=True)
                                            for child in reversed(children):
                                                    logging.info('kill du process :  {}'.format(child.pid)) 
                                                    os.system("sudo pkill -9 -P " + format(child.pid))
