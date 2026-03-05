"""
Helper utility to discover Monday.com column IDs for a board.

Usage:
    python get_column_ids.py <MONDAY_API_KEY> <BOARD_ID>
"""

import sys

import httpx

MONDAY_API_URL = "https://api.monday.com/v2"


def get_columns(api_key: str, board_id: str) -> None:
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }

    # Query parent columns
    query = """
    query ($boardId: [ID!]) {
        boards(ids: $boardId) {
            name
            columns {
                id
                title
                type
            }
        }
    }
    """
    resp = httpx.post(
        MONDAY_API_URL,
        headers=headers,
        json={"query": query, "variables": {"boardId": [board_id]}},
        timeout=30,
    )
    data = resp.json()

    if "errors" in data:
        print(f"API Error: {data['errors']}")
        sys.exit(1)

    boards = data.get("data", {}).get("boards", [])
    if not boards:
        print(f"Board {board_id} not found.")
        sys.exit(1)

    board = boards[0]
    print(f"\nBoard: {board['name']} (ID: {board_id})")
    print(f"\n{'='*60}")
    print(f"{'PARENT COLUMNS':^60}")
    print(f"{'='*60}")
    print(f"{'Title':<30} {'ID':<20} {'Type':<15}")
    print(f"{'-'*30} {'-'*20} {'-'*15}")
    for col in board["columns"]:
        print(f"{col['title']:<30} {col['id']:<20} {col['type']:<15}")

    # Query subitem columns
    sub_query = """
    query ($boardId: [ID!]) {
        boards(ids: $boardId) {
            columns(ids: "subitems") {
                settings_str
            }
        }
    }
    """
    resp2 = httpx.post(
        MONDAY_API_URL,
        headers=headers,
        json={"query": sub_query, "variables": {"boardId": [board_id]}},
        timeout=30,
    )
    data2 = resp2.json()

    # Try to find the subitem board ID from settings
    try:
        import json
        boards2 = data2["data"]["boards"]
        for b in boards2:
            for col in b["columns"]:
                settings = json.loads(col["settings_str"])
                sub_board_id = settings.get("boardIds", [None])[0]
                if sub_board_id:
                    sub_resp = httpx.post(
                        MONDAY_API_URL,
                        headers=headers,
                        json={
                            "query": query,
                            "variables": {"boardId": [str(sub_board_id)]},
                        },
                        timeout=30,
                    )
                    sub_data = sub_resp.json()
                    sub_boards = sub_data.get("data", {}).get("boards", [])
                    if sub_boards:
                        print(f"\n{'='*60}")
                        print(f"{'SUBITEM COLUMNS':^60}")
                        print(f"{'='*60}")
                        print(f"{'Title':<30} {'ID':<20} {'Type':<15}")
                        print(f"{'-'*30} {'-'*20} {'-'*15}")
                        for sc in sub_boards[0]["columns"]:
                            print(f"{sc['title']:<30} {sc['id']:<20} {sc['type']:<15}")
    except (KeyError, IndexError, TypeError):
        print("\nCould not retrieve subitem columns. Check the board has subitems enabled.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python get_column_ids.py <MONDAY_API_KEY> <BOARD_ID>")
        sys.exit(1)

    get_columns(sys.argv[1], sys.argv[2])
