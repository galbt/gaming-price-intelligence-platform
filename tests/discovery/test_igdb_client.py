import responses

from collectors.discovery.igdb_client import get_access_token, fetch_platform_id, fetch_games_by_platform


@responses.activate
def test_get_access_token_returns_token_from_response():
    responses.add(
        responses.POST,
        "https://id.twitch.tv/oauth2/token",
        json={"access_token": "abc123", "expires_in": 5000000, "token_type": "bearer"},
        status=200,
    )

    assert get_access_token("client-id", "client-secret") == "abc123"


@responses.activate
def test_fetch_platform_id_matches_by_name():
    responses.add(
        responses.POST,
        "https://api.igdb.com/v4/platforms",
        json=[{"id": 6, "name": "PC (Microsoft Windows)"}],
        status=200,
    )

    assert fetch_platform_id("PC (Microsoft Windows)", "client-id", "token") == 6


@responses.activate
def test_fetch_platform_id_returns_none_when_not_found():
    responses.add(
        responses.POST,
        "https://api.igdb.com/v4/platforms",
        json=[],
        status=200,
    )

    assert fetch_platform_id("Nonexistent Platform", "client-id", "token") is None


@responses.activate
def test_fetch_games_by_platform_flattens_websites_and_defaults_platforms():
    responses.add(
        responses.POST,
        "https://api.igdb.com/v4/games",
        json=[
            {"id": 1, "name": "Elden Ring", "platforms": [6, 167], "websites": [{"url": "https://store.steampowered.com/app/1245620"}]},
            {"id": 2, "name": "No Platforms Tagged"},
        ],
        status=200,
    )

    games = fetch_games_by_platform(platform_id=6, client_id="client-id", access_token="token")

    assert games == [
        {"id": 1, "name": "Elden Ring", "platforms": [6, 167], "websites": ["https://store.steampowered.com/app/1245620"]},
        {"id": 2, "name": "No Platforms Tagged", "platforms": [], "websites": []},
    ]


@responses.activate
def test_fetch_games_by_platform_follows_pagination():
    page_one = [{"id": i, "name": f"Game {i}", "platforms": [6], "websites": []} for i in range(500)]
    page_two = [{"id": 500, "name": "Game 500", "platforms": [6], "websites": []}]
    responses.add(responses.POST, "https://api.igdb.com/v4/games", json=page_one, status=200)
    responses.add(responses.POST, "https://api.igdb.com/v4/games", json=page_two, status=200)

    games = fetch_games_by_platform(platform_id=6, client_id="client-id", access_token="token")

    assert len(games) == 501
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_games_by_platform_stops_at_max_pages():
    full_page = [{"id": i, "name": f"Game {i}", "platforms": [6], "websites": []} for i in range(500)]
    responses.add(responses.POST, "https://api.igdb.com/v4/games", json=full_page, status=200)

    games = fetch_games_by_platform(platform_id=6, client_id="client-id", access_token="token", max_pages=1)

    assert len(responses.calls) == 1
    assert len(games) == 500
