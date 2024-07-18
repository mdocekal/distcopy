"""
This package contains code for communication between nodes.
"""
import asyncio
import json
import shutil
import subprocess
from asyncio import StreamReader, StreamWriter
from typing import Optional

from classconfig import ConfigurableValue


class Server:
    """
    Server class for communication between nodes using asyncio.
    """

    host: str = ConfigurableValue("Host address", user_default="localhost")
    port: int = ConfigurableValue("Port number", user_default=8888)
    token: Optional[str] = ConfigurableValue("Token for authentication", voluntary=True)

    def __init__(self, host: str, port: int, token: Optional[str] = None):
        """
        Initialize server with host and port.

        :param host: host address
        :param port: port number
        :param token: token for authentication
        """
        self.host = host
        self.port = port
        self.token = token
        self.server = None

    async def handle_client(self, reader: StreamReader, writer: StreamWriter):
        """
        Handle client connection.

        :param reader: reader object
        :param writer: writer object
        """
        data = await reader.read()
        message = json.loads(data.decode())

        if self.token is not None and ("token" not in message or message["token"] != self.token):
            writer.write(json.dumps({
                "error": "Invalid token"
            }).encode())
            await writer.drain()
            writer.close()
            return

        if message["command"] == "free_disk_space":
            response = self.free_disk_space(message["folder"])
        elif message["command"] == "copy_rsync":
            response = self.copy_rsync(message["src"], message["dst"])
        elif message["command"] == "ping":
            response = json.dumps({
                "message": "Pong"
            })
        elif message["command"] == "kill":
            response = json.dumps({
                "message": "Server killed"
            })
            self.server.close()
        else:
            response = json.dumps({
                "error": "Unknown command"
            })

        writer.write(response.encode())
        await writer.drain()
        writer.close()

    async def start(self):
        """
        Start server.
        """
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        await self.server.serve_forever()

    @staticmethod
    def free_disk_space(folder: str) -> str:
        """
        Calculate free disk space in folder.

        :param folder: folder path
        :return: message with free disk space in bytes
        """

        return json.dumps({
            "free": shutil.disk_usage(folder).free
        })

    @staticmethod
    def copy_rsync(src: str, dst: str) -> str:
        """
        Copy file or folder from source to destination using rsync command.

        :param src: source path
        :param dst: destination path
        :return: message with success or error
        """

        r = subprocess.run(["rsync", "-a", src, dst])

        if r.returncode == 0:
            return json.dumps({
                "message": "Success"
            })
        else:
            return json.dumps({
                "error": "Error during copy"
            })





