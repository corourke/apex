# Simple test program to send messages to web monitor
# pip install websockets
# python send-message.py <command> <arguments>
# Help: python send-message.py <command> --help

import argparse
import asyncio
import websockets
import json

# Load message formats
with open("../message_formats.json", "r") as f:
    MESSAGE_FORMATS = json.load(f)

async def main():
    # Create the main parser
    parser = argparse.ArgumentParser(description="WebSocket Test Client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Dynamically create subparsers based on message formats
    for cmd, fields in MESSAGE_FORMATS.items():
        cmd_parser = subparsers.add_parser(cmd, help=f"Send {cmd} message")
        for field, details in fields.items():
            if field != "type":  # Skip the 'type' field as it’s set by the command
                arg_type = str if details["type"] == "string" else int
                cmd_parser.add_argument(field, type=arg_type, help=details["help"])

    # Parse the arguments
    args = parser.parse_args()

    # Construct the message
    message = {"type": args.command}
    for field in MESSAGE_FORMATS[args.command]:
        if field != "type":
            message[field] = getattr(args, field)

    # Send the message via WebSocket
    async with websockets.connect("ws://localhost:8080") as ws:
        await ws.send(json.dumps(message))
        print(f"Sent: {message}")

if __name__ == "__main__":
    asyncio.run(main())