from flask import Flask, jsonify, request
from confluent_kafka import Producer
import requests
import logging
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

PRODUCER_CONF = {
    'bootstrap.servers': 'cvqocn6qn6pkj5g2nf5g.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'GioJCG',
    'sasl.password': 'FEovgbUIbtjaaCsV2tSzbvyRYZbBPh',
}
producer = Producer(PRODUCER_CONF)

TOPIC = "movies_mongo"  # Cambiado el nombre del tópico

JSONL_URL = "https://raw.githubusercontent.com/GioJCG/spark_movies/refs/heads/master/results/movies_without_desc/data.jsonl"

def delivery_report(err, msg):
    if err:
        logging.error(f"Error al enviar mensaje: {err}")
    else:
        logging.info(f"Mensaje enviado a {msg.topic()}")

def create_mongo_document(movie_data):
    """Crea un documento básico para MongoDB con solo el título"""
    return {
        '_id': f"movie_{movie_data.get('title', 'unknown').lower().replace(' ', '_')}",
        'title': movie_data.get('title', ''),
        'created_at': datetime.utcnow().isoformat()
    }

@app.route('/send-movies', methods=['POST'])
def send_movies():
    try:
        logging.info(f"Descargando datos desde: {JSONL_URL}")
        response = requests.get(JSONL_URL)
        response.raise_for_status()

        lines = response.text.strip().splitlines()
        logging.info(f"Total de registros a enviar: {len(lines)}")

        for line in lines:
            try:
                movie_data = json.loads(line)
                mongo_doc = create_mongo_document(movie_data)
                producer.produce(
                    TOPIC, 
                    json.dumps(mongo_doc).encode('utf-8'), 
                    callback=delivery_report
                )
            except json.JSONDecodeError:
                logging.warning(f"Línea inválida omitida: {line}")
                continue

        producer.flush()
        logging.info("Todos los datos fueron enviados correctamente.")

        return jsonify({
            "status": "success", 
            "message": f"Todos los datos fueron enviados al tópico '{TOPIC}'"
        }), 200

    except Exception as e:
        logging.error(f"Error al enviar los datos: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health')
def health_check():
    return "ok", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)