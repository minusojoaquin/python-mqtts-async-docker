import asyncio
import os
import logging
import ssl
import signal
import aiomqtt

# Configuración obligatoria: %(taskName)s requiere Python 3.12
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(taskName)s - %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)

class MqttApp:
    def __init__(self):
        self.broker = os.environ['SERVIDOR']
        self.sub_1 = os.environ['TOPICO_SUB_1']
        self.sub_2 = os.environ['TOPICO_SUB_2']
        self.pub = os.environ['TOPICO_PUB']
        self.counter = 0
        
        # MQTTS: Puerto 8883 con TLS
        self.tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.tls_context.load_default_certs()
        # Nota: Si el certificado del servidor es auto-firmado, 
        # podrías necesitar: self.tls_context.check_hostname = False

    async def increment_task(self):
        """Incrementa el contador cada 3 segundos."""
        while True:
            await asyncio.sleep(3)
            self.counter += 1

    async def publish_task(self, client):
        """Publica el estado cada 5 segundos."""
        while True:
            await asyncio.sleep(5)
            await client.publish(self.pub, payload=f"Contador: {self.counter}")
            logging.info(f"Publicación enviada a {self.pub}")

    async def sub_1_handler(self, payload):
        logging.info(f"Corrutina T1 procesando: {payload}")

    async def sub_2_handler(self, payload):
        logging.info(f"Corrutina T2 procesando: {payload}")

    async def listen_task(self, client):
        """Maneja suscripciones y deriva a corrutinas correspondientes."""
        await client.subscribe(self.sub_1)
        await client.subscribe(self.sub_2)
        async for message in client.messages:
            payload = message.payload.decode()
            if message.topic.matches(self.sub_1):
                await self.sub_1_handler(payload)
            elif message.topic.matches(self.sub_2):
                await self.sub_2_handler(payload)

    async def run(self):
        try:
            async with aiomqtt.Client(
                hostname=self.broker,
                port=8883,
                tls_context=self.tls_context
            ) as client:
                # Creación de tareas con nombre para el logging
                await asyncio.gather(
                    asyncio.create_task(self.increment_task(), name="Task-Incremento"),
                    asyncio.create_task(self.publish_task(client), name="Task-Publicacion"),
                    asyncio.create_task(self.listen_task(client), name="Task-Suscripcion")
                )
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    app = MqttApp()
    loop = asyncio.new_event_loop()
    
    # Captura de señales para cierre limpio
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, loop.stop)

    try:
        loop.run_until_complete(app.run())
    except Exception:
        pass
    finally:
        print("mqtt final")
        loop.close()