import asyncio
import os
import logging
import ssl
import signal
import aiomqtt

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(taskName)s - %(levelname)s: %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)

class MqttApp:
    def __init__(self):
        self.broker = os.environ['SERVIDOR']
        self.sub_topic_1 = os.environ['TOPICO_SUB_1']
        self.sub_topic_2 = os.environ['TOPICO_SUB_2']
        self.pub_topic = os.environ['TOPICO_PUB']
        self.counter = 0
        self.tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.tls_context.load_default_certs()

    async def increment_counter(self):
        while True:
            await asyncio.sleep(3)
            self.counter += 1

    async def publish_status(self, client):
        while True:
            await asyncio.sleep(5)
            payload = f"Contador actual: {self.counter}"
            await client.publish(self.pub_topic, payload=payload)
            logging.info(f"Publicado en {self.pub_topic}: {payload}")

    async def listener(self, client):
        await client.subscribe(self.sub_topic_1)
        await client.subscribe(self.sub_topic_2)
        # aiomqtt 2.x: client.messages es un iterador, no una función
        async for message in client.messages:
            if message.topic.matches(self.sub_topic_1):
                logging.info(f"Recibido T1: {message.payload.decode()}")
            elif message.topic.matches(self.sub_topic_2):
                logging.info(f"Recibido T2: {message.payload.decode()}")

    async def run(self):
        try:
            async with aiomqtt.Client(
                hostname=self.broker,
                port=8883,
                tls_context=self.tls_context
            ) as client:
                await asyncio.gather(
                    asyncio.create_task(self.increment_counter(), name="Task-Incremento"),
                    asyncio.create_task(self.publish_status(client), name="Task-Publicador"),
                    asyncio.create_task(self.listener(client), name="Task-Suscripcion")
                )
        except asyncio.CancelledError:
            pass

async def main():
    app = MqttApp()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, stop_event.set)

    run_task = asyncio.create_task(app.run())
    await stop_event.wait()
    
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass
    print("mqtt final")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass