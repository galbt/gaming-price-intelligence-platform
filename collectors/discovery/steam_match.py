import requests

EDITION_SUFFIXES = [
    # Only cosmetic suffixes that don't change which underlying product the
    # name refers to. "remastered", "director's cut", and "game of the year
    # edition" are deliberately excluded: those usually denote a distinct
    # storefront listing (different appid) from the original release, so
    # stripping them would make a remaster match its original as "confident".
    ": deluxe edition", ": complete edition",
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
