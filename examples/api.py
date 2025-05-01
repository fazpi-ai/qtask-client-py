import asyncio
import logging
import os
import sys # Usado para salir limpiamente en caso de error de configuración

from fastapi import FastAPI, HTTPException

# Import necessary components from your library
# Asegúrate de que QTaskClient está importado correctamente
from qtask_client import QTaskClient, BrokerApiException

import config # Tu archivo de configuración (config.py)

# --- Configuración del Logging ---
# Configura el logging una sola vez al inicio
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger(__name__) # Usa un logger específico para tu app

# --- Instanciación Global del Cliente (Necesario para Decoradores) ---
# Crea una única instancia del cliente para tu aplicación
qtask_client = None # Inicializa como None
try:
    qtask_client = QTaskClient(
        broker_api_url=f"http://{config.QTASK_BROKER_HOST}:{config.QTASK_BROKER_PORT}",
        redis_host=config.REDIS_HOST,
        redis_port=config.REDIS_PORT,
        redis_username=config.REDIS_USERNAME,
        redis_password=config.REDIS_PASSWORD,
    )
    logger.info("QTaskClient instanciado globalmente para decoradores.")
except ValueError as e:
    logger.error(f"Error de configuración al crear QTaskClient globalmente: {e}")
    sys.exit(1) # Salir si el cliente no se puede configurar
except Exception as e:
    logger.error(f"Error inesperado al crear QTaskClient globalmente: {e}", exc_info=True)
    sys.exit(1)

# --- Instancia de FastAPI ---
app = FastAPI(
    title="Mi Aplicación con QTask y FastAPI",
    description="Un ejemplo de integración de qtask_client en FastAPI.",
    version="1.0.0"
)

# --- Definición y Registro de Handlers ---
# Usa el decorador proporcionado por la instancia del cliente (definida globalmente)
# ¡Importante! Asegúrate de que qtask_client no sea None antes de usar el decorador
if qtask_client:
    @qtask_client.handler(topic="whatsapp_messages", group="whatsapp_message_sender")
    async def handle_whatsapp_message(data: dict, message_id: str, partition_index: int):
        """
        Handler para procesar nuevos mensajes de WhatsApp.
        (Ejemplo: Simula el envío de un mensaje)
        """
        to = data.get("to")
        content = data.get("content")
        logger.info(
            f"[HANDLER whatsapp_messages P{partition_index}] Mensaje recibido para {to} ({content}). ID: {message_id}"
        )
        # Aquí iría la lógica real para enviar el mensaje de WhatsApp
        await asyncio.sleep(1) # Simula trabajo asíncrono
        logger.info(f"[HANDLER whatsapp_messages P{partition_index}] Mensaje procesado ID: {message_id}")
        # No necesitas retornar nada especial a menos que quieras indicar un error específico
        # al propio sistema QTask (lo cual requeriría manejo de excepciones aquí).
else:
    logger.error("QTaskClient no se pudo inicializar. Los handlers no serán registrados.")
    # Puedes decidir si quieres que la aplicación falle aquí también
    # sys.exit(1)

# --- Eventos de Ciclo de Vida de FastAPI ---

@app.on_event("startup")
async def startup_event():
    """
    Evento que se ejecuta cuando FastAPI inicia.
    Aquí es donde iniciamos los consumidores de QTask.
    """
    if qtask_client:
        logger.info("Iniciando consumidores de QTask...")
        try:
            # Inicia todos los consumidores registrados con @qtask_client.handler
            await qtask_client.start_all_consumers()
            logger.info("Consumidores de QTask iniciados correctamente.")
        except Exception as e:
            logger.error(f"Error al iniciar los consumidores de QTask: {e}", exc_info=True)
            # Podrías decidir cerrar la aplicación si los consumidores son críticos
            # raise HTTPException(status_code=500, detail="Failed to start QTask consumers")
    else:
        logger.warning("QTaskClient no está disponible, no se iniciarán consumidores.")

@app.on_event("shutdown")
async def shutdown_event():
    """
    Evento que se ejecuta cuando FastAPI se detiene (ej. Ctrl+C).
    Aquí es donde cerramos limpiamente el cliente QTask.
    """
    if qtask_client:
        logger.info("Deteniendo consumidores y cerrando cliente QTask...")
        try:
            await qtask_client.close()
            logger.info("Cliente QTask cerrado correctamente.")
        except Exception as e:
            logger.error(f"Error al cerrar el cliente QTask: {e}", exc_info=True)
    else:
        logger.info("QTaskClient no estaba inicializado, no hay nada que cerrar.")

# --- Endpoints de la API FastAPI ---

@app.get("/")
async def read_root():
    """Endpoint raíz de ejemplo."""
    return {"message": "Servidor FastAPI con QTask funcionando"}

@app.post("/webhook")
async def receive_webhook(data: dict):
    """
    Endpoint para recibir webhooks (ejemplo).
    Podría, por ejemplo, publicar un mensaje en QTask.
    """
    logger.info(f"Webhook recibido: {data}")
    # Ejemplo: Publicar un mensaje en QTask basado en el webhook
    # if qtask_client:
    #     try:
    #         # Asume que el webhook contiene datos para un mensaje de WhatsApp
    #         message_data = {
    #             "to": data.get("user_id"),
    #             "content": data.get("message_text")
    #         }
    #         if message_data["to"] and message_data["content"]:
    #              await qtask_client.publish("whatsapp_messages", message_data)
    #              logger.info(f"Mensaje publicado en QTask topic 'whatsapp_messages' desde webhook.")
    #         else:
    #              logger.warning("Datos incompletos en webhook para publicar en QTask.")
    #     except BrokerApiException as e:
    #         logger.error(f"Error al publicar mensaje desde webhook en QTask: {e}")
    #         raise HTTPException(status_code=500, detail=f"Error al interactuar con QTask Broker: {e}")
    #     except Exception as e:
    #         logger.error(f"Error inesperado al procesar webhook para QTask: {e}", exc_info=True)
    #         raise HTTPException(status_code=500, detail="Error interno al procesar webhook.")
    # else:
    #      logger.error("QTaskClient no disponible para publicar desde webhook.")
    #      raise HTTPException(status_code=503, detail="Servicio de mensajería no disponible.")

    return {"status": "webhook recibido"}

# --- Punto de Entrada (para ejecutar con Uvicorn) ---
# No necesitas el bloque if __name__ == "__main__": asyncio.run(main())
# porque FastAPI se ejecuta con un servidor ASGI como Uvicorn.

# Para ejecutar esta aplicación:
# 1. Guarda el código como `main.py` (o el nombre que prefieras).
# 2. Asegúrate de tener `fastapi`, `uvicorn`, y tu `qtask_client` instalados.
#    pip install fastapi uvicorn[standard] aiohttp redis # y las dependencias de qtask_client
# 3. Ejecuta desde la terminal:
#    uvicorn main:app --reload --host 0.0.0.0 --port 8000
#    (usa --reload para desarrollo, quítalo para producción)