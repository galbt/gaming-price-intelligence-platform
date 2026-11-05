import requests

EDITION_SUFFIXES = [
    ": deluxe edition", ": complete edition", ": remastered",
    ": director's cut", ": game of the year edition",
]


def normalize_name(name: str) -> str:
    result = name.strip().lower()
    for suffix in EDITION_SUFFIXES:
        if result.endswith(suffix):
            result = result[: -len(suffix)]
            break
    return result.strip()


def search_steam(name: str) -> list[dict]:
    response = requests.get(
        "https://store.steampowered.com/api/storesearch/",
        params={"term": name, "cc": "us", "l": "en"},
    )
    return response.json()["items"]


def best_match(target_name: str, steam_results: list[dict]) -> tuple[dict | None, bool]:
    if not steam_results:
        return None, False
    top = steam_results[0]
    confident = normalize_name(target_name) == normalize_name(top["name"])
    return top, confident
