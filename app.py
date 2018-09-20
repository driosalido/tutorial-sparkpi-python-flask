import os

from flask import Flask
from flask import request
from pyspark.sql import SparkSession


app = Flask(__name__)


def cuenta(palabra):
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()

    count = palabra.flatMap(lambda line: line.split("")).map(lambda char: (char, 1)).reduceByKey(lambda a, b: a+b)
    spark.stop()
    return count


@app.route("/")
def index():
    return "Python Flask SparkPi server running. Add the 'sparkpi' route to this URL to invoke the app."


@app.route("/sparkpi")
def sparkpi():
    palabra = int(request.args.get('palabra'))
    response = "Letras".format(cuenta(palabra))
    return response


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
