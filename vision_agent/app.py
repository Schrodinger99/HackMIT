import os
import signal
import sys
import re
import logging
import requests
from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from google.cloud.sql.connector import Connector
import vertexai
from sqlalchemy.sql import text
import tempfile
from vertexai.generative_models import GenerativeModel, Image
from PIL import Image as PilImage
from io import BytesIO
from datetime import datetime

# Configura el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
connector = Connector()


def extraer_numero(cadena):
    # Utiliza una expresión regular para encontrar todos los números en la cadena
    numeros = re.findall(r"\d+(?:\.\d+)?", cadena)
    # Encuentra todos los dígitos y puntos
    if numeros:  # Si hay números encontrados
        return float(numeros[0])  # Convierte el primer número encontrado a float
    return 0.0  # Devuelve 0.0 si no se encuentran números


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
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "creator": getconn
}

dbp = SQLAlchemy()
dbp.init_app(app)

# Inicializa el modelo de Vertex AI
vertexai.init(project="hackmx-1", location="us-central1")
generative_multimodal_model = GenerativeModel("gemini-1.5-pro-002")


@app.route("/genInventario", methods=["GET"])
def gen_inventario():
    try:
        image_url = request.args.get("image_url")
        if not image_url:
            return jsonify({"error": "Se requiere un parámetro 'image_url' en la solicitud"}), 400

        # Descargar la imagen desde la URL
        response = requests.get(image_url)
        if response.status_code != 200:
            return jsonify({"error": "No se pudo descargar la imagen."}), 400

        # Verifica el tipo de contenido
        if 'image' not in response.headers.get('Content-Type', ''):
            return jsonify({"error": "La URL proporcionada no es una imagen."}), 400

        # Cargar la imagen en memoria
        image = PilImage.open(BytesIO(response.content))

        # Guardar la imagen en un archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp_file:
            image.convert('RGB').save(tmp_file, format='JPEG')
            tmp_file_path = tmp_file.name

        # Cargar la imagen desde el archivo temporal
        image_for_model = Image.load_from_file(tmp_file_path)

        prompt = ("A partir de la siguientes notas haz un inventario de una tienda de abarrotes, "
                  "devuelve los datos de la siguiente manera, respetando la notación "
                  "<name>nombre del producto</name> <cantidad>cantidad de producto adquirido</cantidad> "
                  "<fecha>fecha del resurtido</fecha> <precio>precio total de la adquisicion</precio>")

        response = generative_multimodal_model.generate_content([prompt, image_for_model])
        response_text = response.text if hasattr(response, 'text') else str(response)

        # Extraer información usando expresiones regulares
        nombre_pattern = r"<name>(.*?)</name>"
        cantidad_pattern = r"<cantidad>(.*?)</cantidad>"
        fecha_pattern = r"<fecha>(.*?)</fecha>"
        precio_pattern = r"<precio>(.*?)</precio>"

        nombres = re.findall(nombre_pattern, response_text)
        cantidades = re.findall(cantidad_pattern, response_text)
        fechas = re.findall(fecha_pattern, response_text)
        precios = re.findall(precio_pattern, response_text)

        inventario = []
        for i in range(len(nombres)):
            inventario.append({
                "nombre": nombres[i],
                "cantidad": cantidades[i],
                "fecha": fechas[i],
                "precio": precios[i]
            })

            # Ejecutar la consulta de inserción
            try:
                query4 = text("""
                    INSERT INTO producto (fecha, producto, precio_unitario, cantidad, precio_total)
                    VALUES (:fecha, :producto, :precio, :cantidad, :tot)
                """)
                dbp.session.execute(query4, {
                    'fecha': fechas[i],
                    'producto': nombres[i],
                    'precio': (int(extraer_numero(precios[i])) / int(extraer_numero(cantidades[i]))),
                    'cantidad': int(extraer_numero(cantidades[i])),
                    'tot': (int(extraer_numero(precios[i])) )
                })
                dbp.session.commit()  # Confirmar los cambios
            except Exception as e:
                logger.error(f"Error al insertar en la base de datos: {e}")
                dbp.session.rollback()  # Revertir en caso de error

        # Eliminar el archivo temporal
        os.remove(tmp_file_path)

        return jsonify({"inventario": inventario})

    except Exception as e:
        logger.error(f"Error en gen_inventario: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/registrar_venta", methods=["GET"])
def registrar_venta():
    try:
        image_url = request.args.get("image_url")
        if not image_url:
            return jsonify({"error": "Se requiere un parámetro 'image_url' en la solicitud"}), 400

        # Descargar la imagen desde la URL
        response = requests.get(image_url)
        if response.status_code != 200:
            return jsonify({"error": "No se pudo descargar la imagen."}), 400

        # Verifica el tipo de contenido
        if 'image' not in response.headers.get('Content-Type', ''):
            return jsonify({"error": "La URL proporcionada no es una imagen."}), 400

        # Cargar la imagen en memoria
        image = PilImage.open(BytesIO(response.content))

        # Guardar la imagen en un archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp_file:
            image.convert('RGB').save(tmp_file, format='JPEG')
            tmp_file_path = tmp_file.name

        # Cargar la imagen desde el archivo temporal
        image_for_model = Image.load_from_file(tmp_file_path)

        prompt = ("A partir de la siguiente nota, extrae la información de los productos vendidos en la tienda, "
                  "y devuelve los datos en el siguiente formato: "
                  "<producto>nombre del producto</producto> <cantidad>Cantidad vendida</cantidad>")

        response = generative_multimodal_model.generate_content([prompt, image_for_model])
        response_text = response.text if hasattr(response, 'text') else str(response)

        producto_pattern = r"<producto>(.*?)</producto>"
        cantidad_pattern = r"<cantidad>(.*?)</cantidad>"

        productos = re.findall(producto_pattern, response_text)
        cantidades = re.findall(cantidad_pattern, response_text)

        fecha = datetime.now().strftime("%Y-%m-%d")
        total = 0  # Inicializa el total de la compra

        # Almacenar precios unitarios y calcular el total de la compra
        precios_unitarios = {}
        for i in range(len(productos)):
            query_precio = text("SELECT precio_unitario FROM producto WHERE producto = :nombre_producto")
            result_precio = dbp.session.execute(query_precio, {'nombre_producto': productos[i]}).fetchone()
            if result_precio:
                precio_unitario = result_precio[0]
                precios_unitarios[productos[i]] = precio_unitario
                total += int(cantidades[i]) * precio_unitario
            else:
                logger.error(f"Producto {productos[i]} no encontrado en la base de datos.")
                return jsonify({"error": f"Producto {productos[i]} no encontrado en la base de datos."}), 400

        # Insertar la compra en la tabla compra
        query_compra = text("INSERT INTO compra (fecha, total) VALUES (:fecha, :total) RETURNING id_compra")
        venta_id = dbp.session.execute(query_compra, {'fecha': fecha, 'total': total}).scalar()
        dbp.session.commit()

        # Insertar los detalles de la venta en la tabla detalle_compra y actualizar el inventario
        for i in range(len(productos)):
            query_sku = text("SELECT sku, cantidad FROM producto WHERE producto = :nombre_producto")
            result_sku = dbp.session.execute(query_sku, {'nombre_producto': productos[i]}).fetchone()
            if result_sku:
                sku = result_sku[0]
                inventario_actual = result_sku[1]
                precio_unitario = precios_unitarios[productos[i]]
                cantidad_vendida = int(cantidades[i])
                precio_total = cantidad_vendida * precio_unitario

                # Verificar si hay suficiente inventario
                if inventario_actual < cantidad_vendida:
                    logger.error(f"No hay suficiente inventario para el producto {productos[i]}")
                    return jsonify({"error": f"No hay suficiente inventario para el producto {productos[i]}"}), 400

                # Inserta el detalle de la venta
                query_detalle = text("""
                    INSERT INTO detalle_compra (sku, id_compra, cantidad, precio_total)
                    VALUES (:sku, :id_compra, :cantidad, :precio_total)
                """)
                dbp.session.execute(query_detalle, {
                    'sku': sku,
                    'id_compra': venta_id,
                    'cantidad': cantidad_vendida,
                    'precio_total': precio_total
                })

                # Actualizar el inventario
                nuevo_inventario = inventario_actual - cantidad_vendida
                query_update_inventario = text("""
                    UPDATE producto SET cantidad = :nuevo_inventario WHERE sku = :sku
                """)
                dbp.session.execute(query_update_inventario, {'nuevo_inventario': nuevo_inventario, 'sku': sku})
                dbp.session.commit()
            else:
                logger.error(f"SKU no encontrado para el producto {productos[i]}")
                return jsonify({"error": f"SKU no encontrado para el producto {productos[i]}"}), 400

        # Eliminar el archivo temporal
        os.remove(tmp_file_path)

        return jsonify({"id_compra": venta_id, "productos_vendidos": productos, "cantidades_vendidas": cantidades, "total": total})

    except Exception as e:
        logger.error(f"Error en registrar_venta: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/status")
def status():
    return jsonify({"status": "running"})


if __name__ == "__main__":
    def sigterm_handler(_signo, _stack_frame):
        sys.exit(0)
    signal.signal(signal.SIGTERM, sigterm_handler)
    app.run(host="0.0.0.0", port=8080)

