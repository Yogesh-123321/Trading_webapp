from flask import Flask, render_template, request, redirect, url_for, session
from flask_socketio import SocketIO, emit
from dotenv import load_dotenv
import subprocess
import threading
import sys
import os
import signal

# Load credentials from .env
load_dotenv()
USER_ID = os.getenv("USER_ID")
PASSWORD = os.getenv("PASSWORD")

# Create new random secret key each run so old sessions are invalid
app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['SESSION_PERMANENT'] = False

socketio = SocketIO(app)
process = None  # Will hold the running algo.py process


@app.route("/")
def home():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_template("index.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        if username == USER_ID and password == PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("home"))
        else:
            return "Invalid credentials"
    return render_template("login.html")


@socketio.on("run_algo")
def run_algo():
    global process
    if process is None or process.poll() is not None:
        emit("log", {"data": "[Started running...]\n"})

        def run_script():
            global process
            process = subprocess.Popen(
                [sys.executable, "-u", "algo.py"],  # -u = unbuffered output
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            for line in iter(process.stdout.readline, ''):
                socketio.emit("log", {"data": line})
            process.stdout.close()
            process.wait()
            socketio.emit("log", {"data": "\n[Process Finished]\n"})

        threading.Thread(target=run_script).start()
    else:
        emit("log", {"data": "[Process already running]\n"})


@socketio.on("stop_algo")
def stop_algo():
    global process
    if process and process.poll() is None:
        os.kill(process.pid, signal.SIGTERM)
        emit("log", {"data": "[Process Stopped]\n"})
    else:
        emit("log", {"data": "[No process running]\n"})


if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=True)
