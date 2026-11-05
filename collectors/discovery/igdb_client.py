import requests

IGDB_BASE_URL = "https://api.igdb.com/v4"


def get_access_token(client_id: str, client_secret: str) -> str:
    response = requests.post(
        "https://id.twitch.tv/oauth2/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
    )
    return response.json()["access_token"]


def fetch_platform_id(name: str, client_id: str, access_token: str) -> int | None:
    response = requests.post(
        f"{IGDB_BASE_URL}/platforms",
        headers={"Client-ID": client_id, "Authorization": f"Bearer {access_token}"},
        data=f'search "{name}"; fields name; limit 5;',
    )
    for entry in response.json():
        if entry["name"].lower() == name.lower():
            return entry["id"]
    return None


def fetch_games_by_platform(
    platform_id: int, client_id: str, access_token: str, max_pages: int = 20
) -> list[dict]:
    headers = {"Client-ID": client_id, "Authorization": f"Bearer {access_token}"}
    games: list[dict] = []
    offset = 0
    for _ in range(max_pages):
        response = requests.post(
            f"{IGDB_BASE_URL}/games",
            headers=headers,
            data=(
                f"fields name,platforms,websites.url; "
                f"where platforms = ({platform_id}); limit 500; offset {offset};"
            ),
        )
        page = response.json()
        for entry in page:
            games.append(
                {
                    "id": entry["id"],
                    "name": entry["name"],
                    "platforms": entry.get("platforms", []),
                    "websites": [w["url"] for w in entry.get("websites", [])],
                }
            )
        if len(page) < 500:
            break
        offset += 500
    return games
