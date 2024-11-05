from process_brokerage_data.user_data import UserData
import websockets
import asyncio
import json
import ssl
import certifi
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchwabStreaming:
    def __init__(self):
        self.socket_address = None
        self.customer_id = None
        self.core_rel_id = None
        self.client_channel_id = None
        self.client_function = None
        self.access_token = None
        self.running = True
        self.websocket = None
        self.subscriptions = {}
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 30
        self.callbacks = {}

    def get_streamer_info(self):
        """Get websocket and account info"""
        response, access_token = UserData().get_user_prefs()
        streamer_data = response['streamerInfo'][0]
        if streamer_data:
            self.access_token = access_token
            self.socket_address = streamer_data["streamerSocketUrl"]
            self.customer_id = streamer_data["schwabClientCustomerId"]
            self.core_rel_id = streamer_data["schwabClientCorrelId"]
            self.client_channel_id = streamer_data["schwabClientChannel"]
            self.client_function = streamer_data["schwabClientFunctionId"]

    async def connect(self):
        """Establish connection with websockets"""
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        try:
            self.websocket = await websockets.connect(self.socket_address, ssl=ssl_context)
            login_request = {
                "requestid": str(int(round(time.time() * 1000))),
                "service": "ADMIN",
                "command": "LOGIN",
                "SchwabClientCustomerId": self.customer_id,
                "SchwabClientCorrelId": self.core_rel_id,
                "parameters": {
                    "Authorization": self.access_token,
                    "SchwabClientChannel": self.client_channel_id,
                    "SchwabClientFunctionId": self.client_function,
                }
            }
            # Send login request
            await self.websocket.send(json.dumps(login_request))

            # Start the message handler and heartbeat in separate tasks
            asyncio.create_task(self.message_handler())
            asyncio.create_task(self.heartbeat_monitor())

        except Exception as e:
            print(f"Error: {e}")

    async def subscribe(self, service, keys, fields, callback=None):
        """Method to subscribe to specific service with given keys and fields"""
        if not self.websocket:
            raise Exception("Websocket not connected")
        request = {
            "requestid": str(int(time.time() * 1000)),
            "service": service,
            "command": "SUBS",
            "SchwabClientCustomerId": self.customer_id,
            "SchwabClientCorrelId": self.core_rel_id,
            "parameters": {
                "keys": keys,
                "fields": fields,
            }
        }
        await self.websocket.send(json.dumps(request))
        self.subscriptions[f"{service}_{keys}"] = request

        if callback:
            for symbol in keys.split(","):
                self.callbacks[symbol.strip()] = callback

        logger.info(f"Subscribed to {service} for {keys}")


    async def unsubscribe(self, service, keys):
        """Method to unsubscribe from specific service with given keys"""
        pass

    async def message_handler(self):
        """Method to handle incoming data"""
        while self.running:
            try:
                message = await self.websocket.recv()
                data = json.loads(message)

                # Update heartbeat time if this is a heartbeat message
                if data.get('service') == "ADMIN" and data.get('command') == "HEARTBEAT":
                    self.last_heartbeat = time.time()
                    await self.send_heartbeat_response()
                else:
                    await self.handle_data(data)

            except websockets.exceptions.ConnectionClosed:
                logger.error("Connection closed. Attempting to reconnect...")
                await self.reconnect()
            except Exception as e:
                logger.error(f"Error: {e}")

    async def heartbeat_monitor(self):
        """Monitors heartbeat and reconnects if needed"""
        while self.running:
            if time.time() - self.last_heartbeat > self.heartbeat_interval * 2:
                logger.warning(f"Heartbeat timeout. Attempting to reconnect...")
                await self.reconnect()
            await asyncio.sleep(self.heartbeat_interval)

    async def send_heartbeat_response(self):
        """Send heartbeat response"""
        heartbeat_response = {
            "requestid": str(int(round(time.time() * 1000))),
            "service": "ADMIN",
            "command": "HEARTBEAT",
            "SchwabClientCustomerId": self.customer_id,
            "SchwabClientCorrelId": self.core_rel_id,
        }
        await self.websocket.send(json.dumps(heartbeat_response))

    async def reconnect(self):
        """Reconnect and reestablish subscriptions"""
        try:
            await self.connect()
            # Resubscribe to all active subscriptions
            for subscription in self.subscriptions.values():
                await self.websocket.send(json.dumps(subscription))
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            # Wait before trying again
            await asyncio.sleep(5)

    async def handle_data(self, data):
        """Process incoming data"""
        try:
            service = data.get('service')
            if service == "ADMIN":
                return
            services = ["LEVELONE_EQUITIES", "LEVELONE_OPTIONS", "LEVELONE_FUTURES", "LEVELONE_FUTURES_OPTIONS",
                        "LEVELONE_FOREX", "NYSE_BOOK", "NASDAQ_BOOK", "OPTIONS_BOOK", "CHART_EQUITY",
                        "CHART_FUTURES", "SCREENER_EQUITY", "SCREENER_OPTION", "ACCT_ACTIVITY"]
            if service in services:
                content = data.get('content', [])
                for item in content:
                    symbol = item.get('key')
                    if symbol in self.callbacks:
                        await self.callbacks[symbol](item)
                        logger.info(f"Processed data for {symbol}: {item} received")
        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            logger.error(f"Problematic data: {data}")

    def run(self):
        """Method to run the streaming connection"""

        async def main():
            await self.connect()
            # Keep main task running
            while self.running:
                await asyncio.sleep(1)

        asyncio.run(main())


stream = SchwabStreaming()
stream.get_streamer_info()
stream.run()
# LEVELONE_FUTURES
