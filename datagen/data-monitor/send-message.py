# Simple test program to send messages to web monitor
# pip install websockets
# python send-message.py <command> <arguments>

import argparse
import asyncio
import websockets
import json

async def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="WebSocket Test Client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Start command
    start_parser = subparsers.add_parser("start", help="Send start message")
    start_parser.add_argument("timezone", type=str, help="Timezone (e.g., America/New_York)")
    start_parser.add_argument("localTime", type=str, help="Local time in the timezone (e.g., 2023-10-01 12:00:00)")

    # Stop command
    stop_parser = subparsers.add_parser("stop", help="Send stop message")
    stop_parser.add_argument("timezone", type=str, help="Timezone (e.g., America/New_York)")
    stop_parser.add_argument("localTime", type=str, help="Local time in the timezone (e.g., 2023-10-01 13:00:00)")

    # Transactions command
    transactions_parser = subparsers.add_parser("scans", help="Send transactions message")
    transactions_parser.add_argument("timezone", type=str, help="Timezone (e.g., America/New_York)")
    transactions_parser.add_argument("batchCount", type=int, help="Number of transactions in the batch")
    transactions_parser.add_argument("timestamp", type=str, help="Timestamp of the batch (e.g., 2023-10-01 12:34:56)")
    transactions_parser.add_argument("dataVolume", type=int, help="Data volume in bytes")
    transactions_parser.add_argument("localTime", type=str, help="Local time in the timezone (e.g., 2023-10-01 12:35:00)")

    # Parse arguments
    args = parser.parse_args()

    # Construct the message based on the command
    message = {}
    if args.command == "start":
        message = {
            "type": "start",
            "timezone": args.timezone,
            "localTime": args.localTime
        }
    elif args.command == "stop":
        message = {
            "type": "stop",
            "timezone": args.timezone,
            "localTime": args.localTime
        }
    elif args.command == "scans":
        message = {
            "type": "scans",
            "timezone": args.timezone,
            "batchCount": args.batchCount,
            "timestamp": args.timestamp,
            "dataVolume": args.dataVolume,
            "localTime": args.localTime
        }

    # Connect to WebSocket server and send the message
    async with websockets.connect("ws://localhost:8080") as ws:
        await ws.send(json.dumps(message))
        print(f"Sent: {message}")

if __name__ == "__main__":
    asyncio.run(main())
