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
        self.websocket = None
        self.stop = False  # Stop stream flag

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
            async with websockets.connect(self.socket_address, ssl=ssl_context) as self.websocket:
                await self.websocket.send(json.dumps(request))
                response = await self.websocket.recv()  # Wait till login response
                print("Login response:", response)

                # Once logged in - proceed
                await self.subscribe(service="LEVELONE_FUTURES", keys="/ESZ24", fields="0,1,2,3,4,5,6,7,8,9,10")
                await self.listen_for_messages()

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Connection closed with error: {e}")
        except Exception as e:
            print(f"Error in WebSocket connection: {e}")

    async def listen_for_messages(self):
        """Listen for messages from WebSocket and route based on content"""
        while not self.stop:
            try:
                message = await self.websocket.recv()
                json_message = json.loads(message)
                # print(f"Received message: {message}")
                if "notify" not in json_message and 'response' in json_message:
                    print("RESPONSE:", json_message["response"][0])
                if "notify" not in json_message and 'data' in json_message:
                    print("DATA:", json_message["data"][0])

            except websockets.exceptions.ConnectionClosedError as e:
                print(f"Connection closed with error: {e}")
            except Exception as e:
                print(f"Error in WebSocket connection: {e}")

    async def close_connection(self):
        """Close websocket connection"""
        request = {
            "requestid": str(int(round(time.time() * 1000))),
            "service": "ADMIN",
            "command": "LOGOUT",
            "SchwabClientCustomerId": self.customer_id,
            "SchwabClientCorrelId": self.core_rel_id,
            "parameters": {}
        }
        if self.websocket:
            json_req = json.dumps(request)
            try:
                await self.websocket.send(json_req)
                await self.websocket.close()
                print("WebSocket connection closed:")
            except websockets.exceptions.ConnectionClosedError as e:
                print(f"Connection closed with error: {e}")

    async def check_for_exit(self):
        """Check for a 'q' press to stop and logout"""
        print("Press 'q' and Enter to quit")
        while not self.stop:
            user_input = await asyncio.to_thread(input)
            if user_input.lower() == 'q':
                print("Logout command received. Closing connection...")
                self.stop = True
                await self.close_connection()

    async def subscribe(self, service, keys, fields):
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
            # response = await self.websocket.recv()
            # print("Subscription response:", response)
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Connection closed while subscribing: {e}")
        except Exception as e:
            print(f"Error during subscription: {e}")

    async def main(self):
        await self.get_streamer_info()

        await asyncio.gather(
            self.open_connection(),
            self.check_for_exit(),
        )


streamer = SchwabStreaming()
asyncio.run(streamer.main())

