from analytics_app.session_cache import load_once_per_session


def test_reuses_gold_snapshot_while_source_paths_are_unchanged():
    session = {}
    calls = []

    def load_gold():
        calls.append("read")
        return ("live", "historical", "lookup")

    first = load_once_per_session(session, ("historical-a", "live-a", "lookup-a"), load_gold)
    second = load_once_per_session(session, ("historical-a", "live-a", "lookup-a"), load_gold)

    assert first == ("live", "historical", "lookup")
    assert second == first
    assert calls == ["read"]


def test_reloads_gold_snapshot_when_source_paths_change():
    session = {}
    calls = []

    def load_gold():
        calls.append("read")
        return len(calls)

    assert load_once_per_session(session, ("historical-a", "live-a", "lookup-a"), load_gold) == 1
    assert load_once_per_session(session, ("historical-b", "live-a", "lookup-a"), load_gold) == 2
    assert calls == ["read", "read"]
