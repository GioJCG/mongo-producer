from confluent_kafka import Producer
from flask import Flask, jsonify
from dotenv import load_dotenv
from datetime import datetime
import requests
import logging
import json
import os

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

TOPIC = "movies_mongo"

JSONL_URL = "https://raw.githubusercontent.com/GioJCG/spark_movies/refs/heads/master/results/movies_without_desc/data.jsonl"

producer_conf = {
    'bootstrap.servers': 'cvqocn6qn6pkj5g2nf5g.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'GioJCG',
    'sasl.password': 'FEovgbUIbtjaaCsV2tSzbvyRYZbBPh'
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err:
        logging.error(f'Error al enviar: {err}')
    else:
        logging.info(f'Enviado al tópico {msg.topic()}: {msg.value().decode("utf-8")}')

def transform_for_mongodb(data):
    try:
        return {
            'title': data['title'],
        }
    except Exception as e:
        logging.warning(f"Error transformando datos: {e}")
        return None

def fetch_and_send_data():
    response = requests.get(JSONL_URL)
    response.raise_for_status()

    records = response.text.strip().splitlines()
    logging.info(f"Registros recibidos: {len(records)}")

    success, failed = 0, 0

    for line in records:
        try:
            data = json.loads(line)
            message = transform_for_mongodb(data)

            if message:
                producer.produce(
                    topic=TOPIC,
                    value=json.dumps(message, default=str).encode('utf-8'),
                    callback=delivery_report
                )
                success += 1
            else:
                failed += 1
        except Exception as e:
            logging.warning(f"Error procesando línea: {e}")
            failed += 1

    producer.flush()
    return success, failed, len(records)

@app.route('/send-movies', methods=['POST'])
def send_area_stats():
    try:
        success, failed, total = fetch_and_send_data()
        logging.info(f"Éxitos: {success} | Fallos: {failed}")

        return jsonify({
            "status": "success",
            "message": f"Datos enviados al tópico '{TOPIC}'",
            "stats": {
                "total": total,
                "success": success,
                "failed": failed
            }
        }), 200

    except Exception as e:
        logging.error(f"Error general: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)