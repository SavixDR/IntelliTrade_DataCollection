import asyncio
from websockets import connect
import aiofiles
import json
import httpx
import time
import os
from datetime import datetime

recorded_data_path = r'E:\CampusAcedemics2\SEM7\FYP\Data Recordings\Order Book Data\level-2-orderbooks-2022\Recorded_Data'

os.makedirs(recorded_data_path, exist_ok=True)

async def orderbook_download(pair):

    async def fetch_snapshot(pair):
        rest_url = f"https://api.binance.com/api/v3/depth"
        params = {
            "symbol": pair.upper(),
            "limit": 1000,
        }
        async with httpx.AsyncClient() as client:
            snapshot_response = await client.get(rest_url, params=params)
        snapshot = snapshot_response.json()
        last_update_id = snapshot["lastUpdateId"]
        return snapshot, last_update_id

    async def apply_event(order_book, event):
        # Apply bids
        for price, qty in event["b"]:
            if float(qty) == 0:
                order_book["bids"].pop(price, None)  # Remove level if qty is 0
            else:
                order_book["bids"][price] = float(qty)  # Update or add new level

        # Apply asks
        for price, qty in event["a"]:
            if float(qty) == 0:
                order_book["asks"].pop(price, None)  # Remove level if qty is 0
            else:
                order_book["asks"][price] = float(qty)  # Update or add new level

    async def process_websocket(websocket, order_book, last_update_id):
        while True:
            data = await websocket.recv()
            event = json.loads(data)

            if event["u"] <= last_update_id:
                continue  # Skip this event because it's out of date
            
            # The first event processed should have U <= lastUpdateId + 1 AND u >= lastUpdateId + 1
            if event["U"] <= last_update_id + 1 and event["u"] >= last_update_id + 1:
                apply_event(order_book, event) # Order book is updated
                last_update_id = event["u"]
            else:
                print("Out of order event detected, reinitializing from snapshot.")
                return False, last_update_id  # Indicate the need to reinitialize

            # Save the event to file
            async with aiofiles.open(os.path.join(recorded_data_path, f"{pair_lower}-updates-{today}.txt"), mode="a") as f:
                await f.write(data + "\n")

            print("Updated order book:", order_book)

            # Save the updated order book to file
            async with aiofiles.open(os.path.join(recorded_data_path, f"{pair_lower}-orderbook-updated-{today}.txt"), mode="a") as f:
                await f.write(f'{order_book}' + "\n")
        
        return True, last_update_id

    pair_lower = pair.lower()
    websocket_url = f"wss://stream.binance.com:9443/ws/{pair_lower}@depth"
    today = datetime.now().date()

    while True:
        # Step 1: Fetch the initial snapshot
        snapshot, last_update_id = await fetch_snapshot(pair)
        # Initialize the local order book

        order_book = {
            "bids": {price: float(qty) for price, qty in snapshot["bids"]},
            "asks": {price: float(qty) for price, qty in snapshot["asks"]},
        }

        # Save the snapshot with timestamp
        snapshot["time"] = time.time()
        async with aiofiles.open(os.path.join(recorded_data_path, f"{pair_lower}-snapshots-{today}.txt"), mode="a") as f:
            await f.write(json.dumps(snapshot) + "\n")

        async with connect(websocket_url) as websocket:
            success, last_update_id = await process_websocket(websocket, order_book, last_update_id)

            if not success:
                continue  # Reinitialize from snapshot if out-of-order event is detected


def main() :
    asyncio.run(orderbook_download("BTCUSDT"))
    return

if __name__ == "__main__":
    main()
