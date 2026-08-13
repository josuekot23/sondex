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
import threading
from datetime import datetime, timedelta
import time
import socket
os.chdir(os.path.dirname(os.path.abspath(__file__)))

########################################################################
# Definition des variables
########################################################################

# Recupération de l'IP du serveur
with open("target.txt", "r") as f:
	config = f.read().strip()

# Configuration MQTT
MQTT_HOST = config
MQTT_PORT = 1883
MQTT_KEEPALIVE_INTERVAL = 45
SITE = socket.gethostname()
frequences = ["474000000", "482000000", "490000000", "498000000", "506000000", "514000000", "522000000",
              "530000000", "538000000", "546000000", "554000000", "562000000", "570000000", "578000000",
              "586000000", "594000000", "602000000", "610000000", "618000000", "626000000", "634000000",
              "642000000", "650000000", "658000000", "666000000", "674000000", "682000000", "690000000"]

########################################################################
# configuration des messages de logs
########################################################################
logging.basicConfig(
	format='%(asctime)s %(levelname)-8s %(message)s',
	level=logging.INFO,
	datefmt='%Y-%m-%d %H:%M:%S')

########################################################################
# buffer local a rotation en cas de broker injoignable
#
# Optimisations par rapport à la version d'origine :
#  - compteur de lignes tenu en mémoire (plus de relecture complète du
#    fichier courant à chaque mise en buffer)
#  - un verrou (_buffer_lock) protège l'accès concurrent aux fichiers
#    de buffer, car le vidage tourne désormais dans un thread séparé
#    déclenché par la reconnexion, en parallèle du thread principal
#    qui peut continuer à écrire si la connexion retombe aussitôt
########################################################################
import glob
DOSSIER = os.path.dirname(os.path.abspath(__file__))

BUFFER_DIR = os.path.join(DOSSIER, "mqtt_buffer_mesures")
MAX_LIGNES_PAR_FICHIER = 5000
PREFIXE_FICHIER = "buffer_"

os.makedirs(BUFFER_DIR, exist_ok=True)

_buffer_lock = threading.Lock()
_fichier_courant = None
_lignes_fichier_courant = 0


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


def _init_compteur_buffer():
	"""Lecture unique au démarrage pour initialiser le compteur en mémoire."""
	global _fichier_courant, _lignes_fichier_courant
	fichiers = _fichiers_buffer_tries()
	if fichiers:
		_fichier_courant = fichiers[-1]
		with open(_fichier_courant, "r") as f:
			_lignes_fichier_courant = sum(1 for _ in f)
	else:
		_fichier_courant = None
		_lignes_fichier_courant = 0


def _mettre_en_buffer(topic, data):
	global _fichier_courant, _lignes_fichier_courant
	with _buffer_lock:
		if _fichier_courant is None or _lignes_fichier_courant >= MAX_LIGNES_PAR_FICHIER:
			_fichier_courant = _nouveau_nom_fichier()
			_lignes_fichier_courant = 0
			logging.warning('buffer plein ou inexistant, nouveau fichier créé : ' + _fichier_courant)

		with open(_fichier_courant, "a") as f:
			f.write(json.dumps({"topic": topic, "data": data}) + "\n")
		_lignes_fichier_courant += 1

	total = sum(1 for fic in _fichiers_buffer_tries() for _ in open(fic))
	logging.warning('broker injoignable, message mis en buffer local (' + str(total) + ' en attente au total)')


def _vider_buffer():
	"""Rejoue les messages en attente. Appelé uniquement depuis le callback on_connect
	(thread dédié), plus à chaque envoi de message."""
	global _fichier_courant, _lignes_fichier_courant
	with _buffer_lock:
		fichiers = _fichiers_buffer_tries()
		if not fichiers:
			return

		total_rejoues = 0
		total_corrompues = 0
		fichier_corrompues = os.path.join(BUFFER_DIR, "corrompues.jsonl")

		for fichier in fichiers:
			with open(fichier, "r") as f:
				lignes = [l for l in f.read().splitlines() if l.strip()]

			restantes = []
			echec = False
			for l in lignes:
				if echec or not mqtt_connected:
					restantes.append(l)
					continue

				# 1) parsing : une ligne illisible (JSON invalide, écriture
				# coupée par un kill -9 ou un redémarrage en plein milieu...)
				# est écartée définitivement et archivée à part. Elle ne doit
				# JAMAIS empêcher les lignes valides qui suivent de partir.
				try:
					msg = json.loads(l)
				except Exception as e:
					total_corrompues += 1
					logging.error('ligne de buffer corrompue, ignorée : ' + str(e))
					try:
						with open(fichier_corrompues, "a") as fc:
							fc.write(l + "\n")
					except Exception:
						pass
					continue

				# 2) envoi : là un échec est un vrai problème réseau/broker,
				# on arrête le vidage et on préserve cette ligne + tout le reste
				try:
					info = mqttc.publish(msg["topic"], msg["data"], qos=1)
					if info.rc != mqtt.MQTT_ERR_SUCCESS:
						raise Exception("publish rc=" + str(info.rc))
					total_rejoues += 1
				except Exception:
					echec = True
					restantes.append(l)

			if restantes:
				with open(fichier, "w") as f:
					f.write("\n".join(restantes) + "\n")
			else:
				os.remove(fichier)

			if fichier == _fichier_courant:
				_lignes_fichier_courant = len(restantes)

			if echec:
				break

		if total_rejoues:
			logging.info('buffer local : ' + str(total_rejoues) + ' messages rejoués en rafale')
		if total_corrompues:
			logging.warning('buffer local : ' + str(total_corrompues) + ' ligne(s) corrompue(s) ignorée(s), archivées dans corrompues.jsonl')


########################################################################
# connexion MQTT persistante
#
# Un seul client, connecté une fois au démarrage, avec un thread réseau
# géré par paho (loop_start) qui prend en charge la reconnexion
# automatique (reconnect_delay_set). Le vidage du buffer n'est déclenché
# qu'au moment où la connexion est (re)établie, via on_connect.
########################################################################
mqttc = None
mqtt_connected = False


def on_connect(client, userdata, flags, rc):
	global mqtt_connected
	if rc == 0:
		mqtt_connected = True
		logging.info('connecté au broker MQTT (' + MQTT_HOST + ')')
		threading.Thread(target=_vider_buffer, daemon=True).start()
	else:
		mqtt_connected = False
		logging.error('échec de connexion au broker MQTT, code ' + str(rc))


def on_disconnect(client, userdata, rc):
	global mqtt_connected
	mqtt_connected = False
	if rc != 0:
		logging.warning('déconnexion inattendue du broker MQTT (rc=' + str(rc) + '), reconnexion automatique en cours')


def init_mqtt():
	global mqttc
	_init_compteur_buffer()
	mqttc = mqtt.Client()
	mqttc.on_connect = on_connect
	mqttc.on_disconnect = on_disconnect
	mqttc.reconnect_delay_set(min_delay=1, max_delay=30)
	# connect_async() (et non connect()) : ne bloque pas et ne lève pas
	# d'exception si le broker est injoignable. La tentative (et les
	# retentatives suivantes, avec le backoff de reconnect_delay_set)
	# sont déléguées au thread réseau démarré par loop_start(), y compris
	# pour le tout premier essai. Avec connect() simple, si la première
	# tentative échouait (broker down au boot du Pi, réseau pas encore up...),
	# le thread de loop_start() ne retentait jamais rien : on_connect n'était
	# plus jamais appelé, et le buffer local n'était donc plus jamais vidé.
	try:
		mqttc.connect_async(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)
	except Exception as e:
		logging.error('échec de préparation de la connexion MQTT (' + str(e) + ')')
	mqttc.loop_start()


def envoyer_message(topic, data):
	"""Publie directement si connecté (QoS 1), sinon met en buffer local.
	Ne tente plus une connexion par message et ne rejoue plus le buffer
	à chaque appel : c'est on_connect qui s'en charge."""
	if mqtt_connected:
		try:
			info = mqttc.publish(topic, data, qos=1)
			if info.rc != mqtt.MQTT_ERR_SUCCESS:
				raise Exception("publish rc=" + str(info.rc))
		except Exception as e:
			logging.error('échec publication MQTT (' + str(e) + '), mise en buffer')
			_mettre_en_buffer(topic, data)
	else:
		_mettre_en_buffer(topic, data)


###################################################################################
# Début du programme
###################################################################################
logging.info('Début du programme de mesure')
init_mqtt()

while (True):

	dictionnaireFrequences = {}
	for freq in frequences:
		dictionnaireFrequences[freq] = ""

	# ligne de commande du programme de mesure
	cmd = "sudo dvbv5-scan DVB_ALL --output=output.scan"
	p = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf8')

	cmd = "more output.scan | grep -e '\[' -e 'FREQUENCY'  | paste - - | sed 's/[[:blank:]]//g' | sed 's/\[//g' | sed 's/\]FREQUENCY=/;/g' > output.chaine"
	p = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf8')

	with open("output.chaine") as file:
		while (line := file.readline().rstrip()):
			tmp = line.split(";")
			dictionnaireFrequences[tmp[1]] = dictionnaireFrequences[tmp[1]] + ' ' + tmp[0]

	for i in range(5):

		# pour chaque frequence de liste definie
		for freq in frequences:

			cntNbLignesNonLock = 0
			freqIsLocked = False

			cmd = "sudo dvbv5-zap -c DVB_ALL -t 5 " + freq

			p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf8')

			for line in iter(p.stdout.readline, ''):

				if "Quality" in line and "Signal" in line:

					freqIsLocked = True

					line = line.replace("= ", ":")
					line = line.replace("Lock   (0x1f) ", "")
					line = line.replace("Lock   (0x0f) ", "")
					mesures = line.split()

					quality = mesures[0]
					qualityTab = quality.split(":")
					qualityData = qualityTab[1]

					signal = mesures[1]
					signalTab = signal.split(":")
					signalDataTmp = signalTab[1].replace("dBm", "")
					signalDataTmp = signalDataTmp.replace(",", ".")
					signalData = float(signalDataTmp)
					extrapolationData = float(signalDataTmp)
					extrapolationData = extrapolationData + 123.75

					cn = mesures[2]
					cnTab = cn.split(":")
					cnDataTmp = cnTab[1].replace("dB", "")
					cnDataTmp = cnDataTmp.replace(",", ".")
					cnData = cnDataTmp

					ucb = mesures[3]
					ucbTab = ucb.split(":")
					ucbData = ucbTab[1]

					postber = mesures[4]
					postberTab = postber.split(":")
					postberData = postberTab[1]
					postberData = postberData.replace("x10^", "E")

					preber = mesures[5]
					preberTab = preber.split(":")
					preberData = preberTab[1]
					preberData = preberData.replace("x10^", "E")

					per = mesures[6]
					perTab = per.split(":")
					perData = perTab[1]

					sts = str(time.time_ns())

					mqtt_msg = "{\"site\":\"" + SITE + "\",\"frequence\":\"" + freq + ":" + dictionnaireFrequences[freq] + "\", \"signal\":" + signalDataTmp + ", \"timestamp\":\"" + sts + "\"}"
					envoyer_message("signal", mqtt_msg)

					mqtt_msg = "{\"site\":\"" + SITE + "\",\"frequence\":" + freq + ", \"cn\":" + cnData + ", \"timestamp\":\"" + sts + "\"}"
					envoyer_message("cn", mqtt_msg)

					mqtt_msg = "{\"site\":\"" + SITE + "\",\"frequence\":" + freq + ", \"postBER\":" + postberData + ", \"timestamp\":\"" + sts + "\"}"
					envoyer_message("postber", mqtt_msg)

					mqtt_msg = "{\"site\":\"" + SITE + "\",\"frequence\":" + freq + ", \"preBER\":" + preberData + ", \"timestamp\":\"" + sts + "\"}"
					envoyer_message("preber", mqtt_msg)

					mqtt_msg = "{\"site\":\"" + SITE + "\",\"frequence\":" + freq + ", \"extrapolation\":" + str(extrapolationData) + ", \"timestamp\":\"" + sts + "\"}"
					envoyer_message("extrapolation", mqtt_msg)

				else:
					if "C/N" not in line and "Signal" in line and freqIsLocked == False:

						cntNbLignesNonLock = cntNbLignesNonLock + 1
						sts = str(time.time_ns())

						line = line.replace("= ", ":")
						line = line.replace("(0x00)", "")
						line = line.replace("(0x01)", "")
						mesures = line.split()

						signal = mesures[0]
						signalTab = signal.split(":")
						signalDataTmp = signalTab[1].replace("dBm", "")
						signalDataTmp = signalDataTmp.replace(",", ".")
						signalData = float(signalDataTmp)
						extrapolationData = float(signalDataTmp)
						extrapolationData = extrapolationData + 123.75

						mqtt_msg = "{\"site\":\"" + SITE + "\",\"frequence\":\"" + freq + ":" + dictionnaireFrequences[freq] + "\", \"signal\":" + signalDataTmp + ", \"timestamp\":\"" + sts + "\"}"
						envoyer_message("signal", mqtt_msg)
						mqtt_msg = "{\"site\":\"" + SITE + "\",\"frequence\":" + freq + ", \"extrapolation\":" + str(extrapolationData) + ", \"timestamp\":\"" + sts + "\"}"
						envoyer_message("extrapolation", mqtt_msg)

					if "C/N" not in line and "Signal" in line and freqIsLocked == False and cntNbLignesNonLock == 2:
						current_process = psutil.Process()
						children = current_process.children(recursive=True)
						for child in reversed(children):
							os.system("sudo pkill -9 -P " + format(child.pid))