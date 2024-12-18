from process_brokerage_data.user_data import UserData
import websockets
import asyncio
import json
import ssl
import certifi
import logging
import time
from threading import Thread, Event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchwabStreaming:
    """To use, we call subscribe/add then wait_forever()—e.g.:
    def my_data(data):
        print("MY_DATA:", data)
    streamer = SchwabStreaming()
    streamer.subscribe(service="LEVELONE_FUTURES", keys="/ESZ24,/GCZ24", fields="0,1,2,3,8,10,12,13", callback=my_data)
    streamer.wait_forever()
    """

    def __init__(self):
        self.socket_address = None
        self.customer_id = None
        self.core_rel_id = None
        self.client_channel_id = None
        self.client_function = None
        self.access_token = None
        self.websocket = None
        self.stop = False
        self.loop = None
        self.callbacks = {}
        self._running = Event()
        self._initialize_event_loop()

    def _initialize_event_loop(self):
        """Initialize asyncio event loop in a separate thread"""
        self.loop = asyncio.new_event_loop()
        thread = Thread(target=self._run_event_loop, daemon=True)
        self._running.set()
        thread.start()

    def _run_event_loop(self):
        """Run the event loop in the background"""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _ensure_connection(self):
        """Ensure websocket connection is established"""
        if not self.websocket:
            await self.get_streamer_info()
            await self.open_connection()

    def subscribe(self, service, keys, fields, callback=None):
        """Synchronous method to subscribe to a service"""
        if not self.websocket:
            # If not connected, establish connection first
            future = asyncio.run_coroutine_threadsafe(self._ensure_connection(), self.loop)
            future.result()  # Wait for connection to be established

        # Store callback location
        # if we're subscribing to multiple securities we assign them to the same function
        subscription_list = [f"{service}_{k}" for k in keys.split(',')]
        for sl in subscription_list:
            self.callbacks[sl] = callback

        # Now subscribe
        future = asyncio.run_coroutine_threadsafe(
            self._subscribe(service, keys, fields),
            self.loop
        )
        return future.result()

    async def _subscribe(self, service, keys, fields):
        """Async implementation of subscribe"""
        request = {
            "service": service,
            "requestid": str(int(round(time.time() * 1000))),
            "command": "SUBS",
            "SchwabClientCustomerId": self.customer_id,
            "SchwabClientCorrelId": self.core_rel_id,
            "parameters": {
                "keys": keys,
                "fields": fields
            }
        }
        json_request = json.dumps(request)
        try:
            await self.websocket.send(json_request)
            return True
        except Exception as e:
            logger.error(f"Error during subscription: {e}")
            return False

    def add_screener_equity(self, index, sort_field, frequency):
        """Synchronous method to screen advancers and decliners"""
        if not self.websocket:
            # If not connected, establish connection first
            future = asyncio.run_coroutine_threadsafe(self._ensure_connection(), self.loop)
            future.result()  # Wait for connection to be established

        # Now screen
        future = asyncio.run_coroutine_threadsafe(
            self._add_screener_equity(index, sort_field, frequency),
            self.loop
        )
        return future.result()

    async def _add_screener_equity(self, index, sort_field, frequency):
        """Get advancers and decliners.
        index can be: $COMPX $DJI, $SPX, INDEX_ALL, NYSE, NASDAQ, OTCBB, EQUITY_ALL
        sort_field can be:VOLUME, TRADES, PERCENT_CHANGE_UP, PERCENT_CHANGE_DOWN, AVERAGE_PERCENT_VOLUME
        frequency can be: 0, 1, 5, 10, 30 60 minutes (0 is for all day)
        """
        request = {
            "service": "SCREENER_EQUITY",
            "requestid": str(int(round(time.time() * 1000))),
            "command": "SUBS",
            "SchwabClientCustomerId": self.customer_id,
            "SchwabClientCorrelId": self.core_rel_id,
            "parameters": {
                "keys": f"{index}_{sort_field}_{frequency}",
                "fields": "0,1,2,3,4"
            }
        }
        json_request = json.dumps(request)
        try:
            await self.websocket.send(json_request)
            return True
        except Exception as e:
            logger.error(f"Error during screening: {e}")
            return False

    async def get_streamer_info(self):
        """Get websocket and account info"""
        response = UserData().get_user_prefs()
        streamer_info = response['streamerInfo'][0]

        if response:
            self.access_token = response["access_token"]
            self.socket_address = streamer_info["streamerSocketUrl"]
            self.customer_id = streamer_info["schwabClientCustomerId"]
            self.core_rel_id = streamer_info["schwabClientCorrelId"]
            self.client_channel_id = streamer_info["schwabClientChannel"]
            self.client_function = streamer_info["schwabClientFunctionId"]

    async def open_connection(self):
        """Open websocket connection"""
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        request = {
            "requestid": str(int(round(time.time() * 1000))),
            "service": "ADMIN",
            "command": "LOGIN",
            "SchwabClientCustomerId": self.customer_id,
            "SchwabClientCorrelId": self.core_rel_id,
            "parameters": {
                "Authorization": self.access_token,
                "SchwabClientChannel": self.client_channel_id,
                "SchwabClientFunctionId": self.client_function
            }
        }

        try:
            self.websocket = await websockets.connect(self.socket_address, ssl=ssl_context)
            await self.websocket.send(json.dumps(request))
            response = await self.websocket.recv()
            logger.info(f"Login response: {response}")

            # Start listening for messages
            asyncio.run_coroutine_threadsafe(self.listen_for_messages(), self.loop)

        except Exception as e:
            logger.error(f"Error in WebSocket connection: {e}")
            raise

    async def listen_for_messages(self):
        """Listen for messages from WebSocket and route based on content"""
        while self._running.is_set():
            try:
                message = await self.websocket.recv()
                json_message = json.loads(message)

                if "notify" in json_message:
                    logger.info(f"NOTIFY: {json_message['notify'][0]}")
                elif "notify" not in json_message and 'response' in json_message:
                    logger.info(f"RESPONSE: {json_message['response'][0]}")
                elif "notify" not in json_message and 'data' in json_message:
                    logger.info(f"DATA: {json_message['data'][0]}")
                    json_message = json_message['data'][0]
                    service = json_message.get('service')
                    content = json_message.get('content', [])
                    # Process each item
                    for item in content:
                        key = item.get('key')
                        if key:
                            subscription_key = f"{service}_{key}"
                            callback = self.callbacks.get(subscription_key)
                            if callback:
                                try:
                                    callback(item)
                                except Exception as e:
                                    logger.error(f"Error in callback: {e}")

            except websockets.exceptions.ConnectionClosedError as e:
                logger.error(f"Connection closed with error: {e}")
                if self._running.is_set():
                    # Try to reconnect if we're still supposed to be running
                    await self._ensure_connection()
            except Exception as e:
                logger.error(f"Error in WebSocket connection: {e}")
                if self._running.is_set():
                    await asyncio.sleep(5)  # Wait before retry

    def close(self):
        """Synchronous method to close the connection"""
        self._running.clear()
        future = asyncio.run_coroutine_threadsafe(self.close_connection(), self.loop)
        future.result()
        self.loop.call_soon_threadsafe(self.loop.stop)

    async def close_connection(self):
        """Close websocket connection"""
        if self.websocket:
            request = {
                "requestid": str(int(round(time.time() * 1000))),
                "service": "ADMIN",
                "command": "LOGOUT",
                "SchwabClientCustomerId": self.customer_id,
                "SchwabClientCorrelId": self.core_rel_id,
                "parameters": {}
            }
            try:
                await self.websocket.send(json.dumps(request))
                await self.websocket.close()
                logger.info("WebSocket connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

    def wait_forever(self):
        """Method to keep the main thread alive"""
        try:
            while self._running.is_set():
                time.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.close()
