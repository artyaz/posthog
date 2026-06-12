import json

from django.test import TestCase

from posthog.models.integration import Integration
from posthog.models.organization import Organization
from posthog.models.team import Team

from products.slack_app.backend.api import (
    _build_explore_modal,
    _escape_slack_text,
    _explore_token_from_payload,
    _extract_explore_hints,
)
from products.slack_app.backend.subscription_explore import (
    EXPLORE_ACTION_ID,
    EXPLORE_VIEW_CALLBACK_ID,
    REQUIRED_SLACK_SCOPES,
    bot_is_ready,
    decode_explore_token,
    make_explore_token,
)


class TestSubscriptionExploreToken(TestCase):
    def test_token_round_trips(self) -> None:
        token = make_explore_token(integration_id=42, resource_name="Weekly report")
        decoded = decode_explore_token(token)
        assert decoded == {"integration_id": 42, "resource_name": "Weekly report"}

    def test_decode_rejects_garbage(self) -> None:
        assert decode_explore_token("not-a-real-token") is None
        assert decode_explore_token("") is None


class TestBotIsReady(TestCase):
    def setUp(self) -> None:
        self.org = Organization.objects.create(name="Org")
        self.team = Team.objects.create(organization=self.org, name="Team")

    def _integration(self, scopes: frozenset[str]) -> Integration:
        return Integration.objects.create(
            team=self.team,
            kind="slack",
            integration_id="T1",
            config={"scope": ",".join(sorted(scopes))},
            sensitive_config={"access_token": "xoxb-test"},
        )

    def test_ready_with_full_scopes(self) -> None:
        assert bot_is_ready(self._integration(REQUIRED_SLACK_SCOPES)) is True

    def test_not_ready_when_scopes_missing(self) -> None:
        assert bot_is_ready(self._integration(frozenset({"chat:write"}))) is False


class TestExploreHints(TestCase):
    def test_token_extracted_from_button_click(self) -> None:
        token = make_explore_token(integration_id=7, resource_name="r")
        payload = {"type": "block_actions", "actions": [{"action_id": EXPLORE_ACTION_ID, "value": token}]}
        assert _explore_token_from_payload(payload) == token
        assert _extract_explore_hints(payload) == 7

    def test_token_extracted_from_view_submission(self) -> None:
        token = make_explore_token(integration_id=9, resource_name="r")
        payload = {
            "type": "view_submission",
            "view": {"private_metadata": json.dumps({"token": token, "channel": "C1"})},
        }
        assert _explore_token_from_payload(payload) == token
        assert _extract_explore_hints(payload) == 9

    def test_unrelated_payload_yields_no_hint(self) -> None:
        payload = {"type": "block_actions", "actions": [{"action_id": "posthog_code_repo_select", "value": "x"}]}
        assert _explore_token_from_payload(payload) == ""
        assert _extract_explore_hints(payload) is None

    def test_view_submission_with_null_view_does_not_raise(self) -> None:
        # This runs on every interactivity payload; a null `view` must not blow up the handler.
        assert _explore_token_from_payload({"type": "view_submission", "view": None}) == ""
        assert _extract_explore_hints({"type": "view_submission", "view": None}) is None


class TestEscapeSlackText(TestCase):
    def test_escapes_reserved_chars_so_mentions_do_not_expand(self) -> None:
        assert _escape_slack_text("ping <!channel> & <@U123>") == "ping &lt;!channel&gt; &amp; &lt;@U123&gt;"

    def test_plain_text_unchanged(self) -> None:
        assert _escape_slack_text("what drove the spike?") == "what drove the spike?"


class TestBuildExploreModal(TestCase):
    def test_modal_shape(self) -> None:
        modal = _build_explore_modal(private_metadata='{"token": "t"}', resource_name="Signups")
        assert modal["type"] == "modal"
        assert modal["callback_id"] == EXPLORE_VIEW_CALLBACK_ID
        assert modal["private_metadata"] == '{"token": "t"}'
        # The resource name is surfaced in the heading, and there's a free-text input to fill.
        assert "Signups" in modal["blocks"][0]["text"]["text"]
        assert any(block.get("type") == "input" for block in modal["blocks"])
