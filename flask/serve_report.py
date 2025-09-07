from flask import Flask, send_from_directory
import pathlib, webbrowser

REPORT_DIR = pathlib.Path("/home/pew_pew/github_projects/stokc-price-prediciton-pipeline/Stock-price-prediction-pipeline/nifty_data/html/")   # change to your path
REPORT_NAME = "nifty_report_20250907T083851Z.html"  # change to your filename

app = Flask(__name__, static_folder=str(REPORT_DIR))

@app.route("/")
def index():
    return send_from_directory(REPORT_DIR, REPORT_NAME)

@app.route("/<path:filename>")
def any_file(filename):
    return send_from_directory(REPORT_DIR, filename)

if __name__ == "__main__":
    url = "http://127.0.0.1:5000/"
    print("Serving at", url)
    webbrowser.open(url)   # optional: opens default browser
    app.run(debug=True, host="0.0.0.0", port=5000)
