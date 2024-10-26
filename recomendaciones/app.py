import os
import pandas as pd
import io
from sqlalchemy import text
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError
from flask import Flask, request, jsonify
from google.cloud.sql.connector import Connector
import vertexai
from vertexai.generative_models import GenerativeModel
import logging
import signal
import sys
from types import FrameType

app = Flask(__name__)
connector = Connector()

# Configuración de la base de datos
def getconn():
    user = "postgres"
    password = "1234567"
    db = "hack"
    conn = connector.connect(
        "hackmx-1:us-central1:mind",
        "pg8000",
        user=user,
        password=password,
        db=db,
        ip_type="public"
    )
    return conn

app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql+pg8000://"
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {"creator": getconn}

dbp = SQLAlchemy(app)

# Inicialización del modelo de Vertex AI
vertexai.init(project="hackmx-1", location="us-central1")
model = GenerativeModel("gemini-1.5-flash-002")

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Función para generar recomendaciones a partir de un prompt
def gen_text(prompt):
    response = model.generate_content([prompt])
    return response.text

# Consulta a la base de datos y generación de recomendaciones
def vectorize_data_to_csv():
    producto_query = text("""
        SELECT sku, fecha, producto, precio_unitario, cantidad
        FROM Producto
    """)
    det_com_query = text("""
        SELECT *
        FROM Detalle_Compra
    """)
    try:
        producto_result = dbp.session.execute(producto_query).fetchall()
        det_com_result = dbp.session.execute(det_com_query).fetchall()

        producto_df = pd.DataFrame(producto_result, columns=['sku', 'fecha', 'producto', 'precio_unitario', 'cantidad'])
        det_com_df = pd.DataFrame(det_com_result, columns=['id_detalle', 'sku', 'id_compra', 'cantidad', 'precio_unitario', 'precio_total'])

        return {
            'producto_csv': io.StringIO(producto_df.to_csv(index=False)).getvalue(),
            'detalle_compra_csv': io.StringIO(det_com_df.to_csv(index=False)).getvalue()
        }
    except SQLAlchemyError as e:
        logger.error(f"Error al ejecutar la consulta: {e}")
        return {'producto_csv': '', 'detalle_compra_csv': ''}

@app.route('/agent_report/', methods=['GET'])
def show_csv():
    csv_data = vectorize_data_to_csv()

    prompt = (
        f"Analiza el inventario disponible y determina estadísticas y probabilidades a partir de los detalles de las compras. "
        f"Proporciona recomendaciones de compra y reabastecimiento para una tienda de abarrotes o miscelanea utilizando lenguaje natural. "
        f"Evita usar lenguaje de estadística y probabilidad. Genera recomendaciones concretas como: 'Compra X cantidad de Y producto esta semana'. se breve, claro y consiso "
        f"Inventario:\n{csv_data['producto_csv']}\n Detalles de compra:\n{csv_data['detalle_compra_csv']}"
    )

    response = gen_text(prompt)

    return jsonify({"response": response})

@app.route("/")
def hello() -> str:
    logger.info("Hello endpoint accessed.")
    return "Hello, World!"

def shutdown_handler(signal_int: int, frame: FrameType) -> None:
    logger.info(f"Caught Signal {signal.strsignal(signal_int)}")
    sys.exit(0)

if __name__ == "__main__":
    # Running application locally, outside of a Google Cloud Environment
    signal.signal(signal.SIGINT, shutdown_handler)
    app.run(host="localhost", port=8080, debug=True)
else:
    # handles Cloud Run container termination
    signal.signal(signal.SIGTERM, shutdown_handler)