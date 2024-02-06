from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from confluent_kafka import Producer, Consumer

app = Flask(__name__)
CORS(app)

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',
}

# Kafka producer
producer = Producer(producer_config)

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Kafka consumer
consumer = Consumer(consumer_config)
consumer.subscribe(['messageapp'])

# Configure logging
logging.basicConfig(level=logging.DEBUG)

@app.route('/send_message', methods=['POST'])
def send_message():
    message = request.data.decode('utf-8')

    if not message:
        return jsonify({"status": "error", "message": "Message not provided"}), 400

    # Log received message
    logging.info(f"Received message: {message}")

    try:
        # Send message to Kafka topic
        producer.produce('messageapp', value=message.encode('utf-8'))
        producer.flush()  # Ensure the message is sent immediately
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/receive_message', methods=['GET'])
def receive_message():
    messages = []
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                break
            messages.append(message.value().decode('utf-8'))
        
        return jsonify({"status": "success", "messages": messages}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(port=8000, debug=True)
