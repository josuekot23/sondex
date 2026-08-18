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
import random
import time
import socket
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Lissage du vidage du buffer : utile quand plusieurs Raspberry Pi se
# reconnectent en même temps après une panne serveur commune (ex: 15
# sondes qui vident chacune un backlog de plusieurs milliers de messages
# à la même seconde).
VIDAGE_PAUSE_SECONDES = 0.02        # pause "lente" (petits backlogs, ou fin de purge) : ~50 msg/s
VIDAGE_PAUSE_RAPIDE_SECONDES = 0.002 # pause "rapide" (gros backlogs, ex: panne de plusieurs semaines) : ~500 msg/s
VIDAGE_SEUIL_ACCELERATION = 2000    # en dessous de ce nombre de messages restants, on repasse en pause lente
VIDAGE_JITTER_MAX_SECONDES = 30   # délai aléatoire avant de démarrer le vidage, différent par Pi

########################################################################
# Definition des variables
########################################################################

# Recupération de l'IP du serveur
with open("target.txt", "r") as f:
	config = f.read().strip()


def _charger_env(fichier):
	"""Charge les variables KEY=VALUE d'un fichier .env dans os.environ.
	Utilisé pour ne jamais avoir le mot de passe MQTT en dur dans le code
	ni commité dans Git."""
	if os.path.exists(fichier):
		with open(fichier) as f:
			for ligne in f:
				ligne = ligne.strip()
				if "=" in ligne and not ligne.startswith("#"):
					k, v = ligne.split("=", 1)
					os.environ[k.strip()] = v.strip()


_charger_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), "mqtt_credentials.env"))

# Configuration MQTT
MQTT_HOST = config
MQTT_PORT = 8883
MQTT_KEEPALIVE_INTERVAL = 45
MQTT_TOPIC = "logger"
SITE = socket.gethostname()
MQTT_USERNAME = "sonde-" + SITE
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_CA_CERT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ca.crt")

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
#  - verrou (_buffer_lock) protégeant l'accès concurrent aux fichiers
#    de buffer, car le vidage tourne maintenant dans un thread séparé
#    déclenché par la reconnexion
#  - une ligne corrompue (JSON invalide) est écartée et archivée, elle
#    ne bloque plus jamais les lignes valides qui suivent
########################################################################
import glob
DOSSIER = os.path.dirname(os.path.abspath(__file__))

BUFFER_DIR = os.path.join(DOSSIER, "mqtt_buffer_temperature")
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


def _compter_total_buffer(fichiers):
	total = 0
	for fic in fichiers:
		with open(fic) as f:
			total += sum(1 for _ in f)
	return total


def _vider_buffer():
	"""Rejoue les messages en attente. Appelé uniquement depuis le callback on_connect
	(thread dédié), plus à chaque envoi de message."""
	global _fichier_courant, _lignes_fichier_courant
	with _buffer_lock:
		fichiers = _fichiers_buffer_tries()
		if not fichiers:
			return

		total_restant = _compter_total_buffer(fichiers)
		if total_restant > VIDAGE_SEUIL_ACCELERATION:
			logging.info('vidage : ' + str(total_restant) + ' message(s) en attente, mode rapide activé')

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
					total_restant -= 1
					pause = VIDAGE_PAUSE_SECONDES if total_restant <= VIDAGE_SEUIL_ACCELERATION else VIDAGE_PAUSE_RAPIDE_SECONDES
					time.sleep(pause)
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


def _vider_buffer_differe():
	"""Attend un délai aléatoire avant de vider le buffer. Utile quand
	plusieurs Pi se reconnectent au même instant (panne serveur commune) :
	ça étale les vidages sur une fenêtre de temps au lieu de tout envoyer
	en même temps sur le broker et sur la chaîne d'écriture derrière
	(Node-RED -> InfluxDB)."""
	delai = random.uniform(0, VIDAGE_JITTER_MAX_SECONDES)
	logging.info('vidage du buffer programmé dans ' + str(round(delai, 1)) + 's')
	time.sleep(delai)
	_vider_buffer()


def on_connect(client, userdata, flags, rc):
	global mqtt_connected
	if rc == 0:
		mqtt_connected = True
		logging.info('connecté au broker MQTT (' + MQTT_HOST + ')')
		threading.Thread(target=_vider_buffer_differe, daemon=True).start()
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
	if not MQTT_PASSWORD:
		logging.error('MQTT_PASSWORD manquant (mqtt_credentials.env introuvable ou vide) : la connexion va échouer')
	mqttc.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
	mqttc.tls_set(ca_certs=MQTT_CA_CERT)
	mqttc.on_connect = on_connect
	mqttc.on_disconnect = on_disconnect
	mqttc.reconnect_delay_set(min_delay=1, max_delay=30)
	# connect_async() (et non connect()) : ne bloque pas et ne lève pas
	# d'exception si le broker est injoignable. La tentative (et les
	# retentatives suivantes, avec le backoff de reconnect_delay_set)
	# sont déléguées au thread réseau démarré par loop_start(), y compris
	# pour le tout premier essai — important au boot du Pi si le réseau
	# n'est pas encore up.
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
			logging.info('message envoyé:' + data)
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

	# temporisation
	time.sleep(3)

	# ligne de commande du programme de mesure
	cmd = "vcgencmd measure_temp"

	# execution de la commande au niveau du systeme d exploitation
	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf8')

	# lecture de la sortie standard
	for line in iter(p.stdout.readline, ''):
		# message de log
		logging.info('ligne en cours de traitement:' + line.rstrip())

		# recuperation de la temperature
		line = line.strip()
		line = line.replace("temp=", "")
		line = line.replace("'C", "")

		# timestamp de la mesure (nanosecondes)
		sts = str(time.time_ns())

		# envoi de l information
		mqtt_msg = "{\"site\":\"" + SITE + "\", \"temperature\":" + line + ", \"timestamp\":\"" + sts + "\"}"
		envoyer_message("temperature", mqtt_msg)