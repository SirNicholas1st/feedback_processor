from flask import Flask, render_template, request
from kafka import KafkaProducer
from datetime import datetime
import sys
import json

KAFKA_HOST = "localhost:9092"
KAFKA_TOPIC = "customer_feedback"

try:
    producer = KafkaProducer(bootstrap_servers = KAFKA_HOST,
                             value_serializer = lambda v: json.dumps(v).encode("utf-8"))
except Exception as e:
    print(f"Error --> {e}")
    sys.exit()

app = Flask(__name__, static_folder="static")

@app.route("/")
def form():
    return render_template("form.html")

@app.route("/submit", methods = ["POST", "GET"])
def submit():
    timestamp = datetime.utcnow().isoformat().encode("utf-8")

    value_dict = {
    "service_rating" : request.form.get("serviceRating"),
    "product_rating" : request.form.get("productRating"),
    "comments" : request.form.get("comments")
    }

    producer.send(topic = KAFKA_TOPIC, key = timestamp, value = value_dict)
    
    return render_template("submitted.html")


if __name__ == "__main__":
    app.run(debug = True)