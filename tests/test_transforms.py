# test_transforms script - För att testa _is_valid() och _flatten() funktionerna i /transforms/bronze_to_silver.py scriptet
# Kommentarer: Svenska
# Kod: Engelska
import pytest
from transforms.bronze_to_silver import _is_valid, _flatten

# ========== Fixtures: Återanvändbar testdata ==========
# En pytest fixture producerar testdata på begäran. Istället för att skriva samma dict om och om igen i varje test
# Så definierar jag den EN gång här och injicerar den i testerna som behöver den.
# DRY princip tillämpad på tester.


@pytest.fixture
def valid_push_event():
    """A complete, well-formatted PushEvent - Golden path"""
    return {
        "id": "12345678",
        "type": "PushEvent",
        "actor": {"login": "johnnyhyy", "id": 99},
        "repo": {"name": "apache/airflow", "id": 42},
        "payload": {"size": 3, "commits": []},
        "created_at": "2026-03-29T16:00:00Z",
    }


@pytest.fixture
def valid_pr_event():
    """A complete PullRequestEvent with merged-flag"""
    return {
        "id": "99999999",
        "type": "PullRequestEvent",
        "actor": {"login": "contributor", "id": 77},
        "repo": {"name": "dbt-labs/dbt-core", "id": 11},
        "payload": {
            "action": "closed",
            "pull_request": {"merged": True},
        },
        "created_at": "2026-03-29T18:30:00Z",
    }


# ========== Tester för _is_valid() ==========
# Naming convention test_<funktion>_<scenario> gör att pytest output
# blir självdokumenterande, jag ser direkt vilket scenario som misslyckades.
class TestIsValid:
    """Testing validation logic - Gatekeeper towards DLQ."""

    # Push event
    def test_valid_push_event_passes(self, valid_push_event):
        """A complete PushEvent WILL pass."""
        assert _is_valid(valid_push_event) is True

    # PR event
    def test_valid_pr_event_passes(self, valid_pr_event):
        """A complete PullRequestEvent WILL pass."""
        assert _is_valid(valid_pr_event) is True

    # Missing id fails
    def test_missing_id_fails(self, valid_push_event):
        """An event without ID is useless - I cant Deduplicate it."""
        del valid_push_event["id"]
        assert _is_valid(valid_push_event) is False

    # Missing type fails
    def test_missing_type_fails(self, valid_push_event):
        """Without type I cant know what the event represents."""
        del valid_push_event["type"]
        assert _is_valid(valid_push_event) is False

    # Missing repo fails
    def test_missing_repo_fails(self, valid_push_event):
        """Without repo info the event is useless for my DE-analysis."""
        del valid_push_event["repo"]
        assert _is_valid(valid_push_event) is False

    # Timestamp fails
    def test_missing_created_at_fails(self, valid_push_event):
        """Without Timestamp I cant partition the event correctly."""
        del valid_push_event["created_at"]
        assert _is_valid(valid_push_event) is False

    # Okänt event type fails
    def test_unknown_event_type_fails(self, valid_push_event):
        """
        An event type we don't recognize should go to DLQ. GitHub has many event types we don't care about,
        for example GollumEvent (wiki editing) or MemberEvent.
        """
        valid_push_event["type"] = "GollumEvent"
        assert _is_valid(valid_push_event) is False

    # En timestamp som inte kan parsas SKA faila.
    def test_invalid_timestamp_fails(self, valid_push_event):
        """
        A timestamp that cannot be parsed should fail.
        This protects _flatten() from crashing further down the pipeline.
        """
        valid_push_event["created_at"] = "not-a-real-date"
        assert _is_valid(valid_push_event) is False

    # Ett tomt event SKA faila.
    def test_empty_event_fails(self):
        """A completely empty dict should fail without crashing."""
        assert _is_valid({}) is False


# ========== Tester för _flatten() ==========
# Naming convention test_<funktion>_<scenario> gör att pytest output
# blir självdokumenterande, jag ser direkt vilket scenario som misslyckades.


class TestFlatten:
    """Tests that nested JSON structures are correctly flattened to Silver schema."""

    def test_push_event_flattens_correctly(self, valid_push_event):
        """
        Checks that all expected columns exist and have the correct values.
        This is my contract test - if _flatten() changes column names
        or logic, the Gold layer will break. This test
        catches that kind of regression early.
        """
        result = _flatten(valid_push_event)

        assert result["event_id"] == "12345678"
        assert result["event_type"] == "PushEvent"
        assert result["actor_login"] == "johnnyhyy"
        assert result["repo_name"] == "apache/airflow"
        assert result["commit_count"] == 3
        assert result["created_at"] == "2026-03-29T16:00:00Z"

    def test_pr_event_extracts_merged_flag(self, valid_pr_event):
        """
        pr_merged should be True for a merged PR.
        This field is central to pr_cycle_times in the Gold layer -
        I only want to measure cycle times for PRs that were actually merged.
        """
        result = _flatten(valid_pr_event)

        assert result["pr_merged"] is True
        assert result["pr_action"] == "closed"
        assert result["commit_count"] == 0  # PRs har ingen payload.size

    def test_missing_optional_fields_get_defaults(self, valid_push_event):
        """
        If optional fields are missing from the payload, I should get default values,
        not a KeyError that crashes the entire transformation.
        This is programming with defense in mind - I assume that Githubs API
        can return events with thin payloads.
        """
        # Ta bort payload helt, ett extremfall men möjligt
        valid_push_event["payload"] = {}
        result = _flatten(valid_push_event)

        assert result["commit_count"] == 0
        assert result["pr_action"] is None
        assert result["pr_merged"] is False

    # Mest subtilt värdefulla testet. Fungerar som ett schema kontrakt.
    """ def test_flatten_returns_all_expected_keys(self, valid_push_event):
        
        Checks that the Silver schema always has exactly the columns
        I expect, no more, no less.
        If someone adds or removes a column in _flatten()
        without updating this test, CI will catch it.
        
        expected_keys = {
            "event_id",
            "event_type",
            "actor_login",
            "repo_name",
            "repo_id",
            "commit_count",
            "pr_action",
            "pr_merged",
            "created_at",
        }
        result = _flatten(valid_push_event)
        assert set(result.keys()) == expected_keys

        """
