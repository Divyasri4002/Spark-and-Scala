import os

class Config:
    SPARK_MASTER = "local[*]"
    DATA_PATH = "data/raw.csv"
    SPARK_UI_PORT = 4040
    FLASK_PORT = 5000