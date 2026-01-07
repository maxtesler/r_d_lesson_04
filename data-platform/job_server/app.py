from flask import Flask, request, jsonify
app = Flask(__name__)

@app.post("/run-dbt")
def run_dbt():
    return jsonify({"status": "created", "job": "dbt", "payload": (request.get_json(silent=True) or {})}), 201

@app.post("/train")
def train():
    return jsonify({"status": "created", "job": "train", "payload": (request.get_json(silent=True) or {})}), 201

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
