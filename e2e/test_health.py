import requests


def test_health(service):
    # 200 only when the indexer reaches its database
    r = requests.get(service("indexer", 8000) + "/health", timeout=10)
    assert r.status_code == 200
