import asyncio
import os
import logging
import ssl
import signal
import aiomqtt

# Configuración de logging para Python 3.12+
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(taskName)s - %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)

class MqttApp:
    def __init__(self):
        # Carga de variables de entorno
        self.broker = os.environ['SERVIDOR']
        self.sub_1 = os.environ['TOPICO_SUB_1']
        self.sub_2 = os.environ['TOPICO_SUB_2']
        self.pub = os.environ['TOPICO_PUB']
        self.counter = 0
        
        # Configuración TLS para MQTTS
        self.tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.tls_context.load_default_certs()

    async def counter_task(self):
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

    async def handle_sub1(self, payload):
        logging.info(f"Corrutina T1 procesando: {payload}")

    async def handle_sub2(self, payload):
        logging.info(f"Corrutina T2 procesando: {payload}")

    async def listen_task(self, client):
        """Suscribe y deriva mensajes a corrutinas con nombre."""
        await client.subscribe(self.sub_1)
        await client.subscribe(self.sub_2)
        async for message in client.messages:
            payload = message.payload.decode()
            if message.topic.matches(self.sub_1):
                asyncio.create_task(self.handle_sub1(payload), name="Task-Handler1")
            elif message.topic.matches(self.sub_2):
                asyncio.create_task(self.handle_sub2(payload), name="Task-Handler2")

    async def run(self):
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        # Manejo de señales para cierre limpio
        def shutdown():
            stop_event.set()

        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, shutdown)

        try:
            async with aiomqtt.Client(
                hostname=self.broker,
                port=8883,
                tls_context=self.tls_context
            ) as client:
                # Ejecución de tareas concurrentes con nombres asignados
                tasks = [
                    asyncio.create_task(self.counter_task(), name="Task-Contador"),
                    asyncio.create_task(self.publish_task(client), name="Task-Publicacion"),
                    asyncio.create_task(self.listen_task(client), name="Task-Suscripcion")
                ]
                
                await stop_event.wait()
                
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    app = MqttApp()
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass
    finally:
        print("mqtt final")