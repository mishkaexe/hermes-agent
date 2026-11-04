"""Provider failures must close the started turn for plugin observers."""
from unittest.mock import MagicMock, patch

from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
from run_agent import AIAgent


def test_terminal_provider_error_notifies_end():
    manager = PluginManager()
    manager._discovered = True
    ctx = PluginContext(PluginManifest(name="recorder", key="recorder"), manager)
    events = []
    ctx.register_hook("on_session_start", lambda **kw: events.append(("start", kw)))
    ctx.register_hook("on_session_end", lambda **kw: events.append(("end", kw)))
    ctx.register_hook("post_llm_call", lambda **kw: events.append(("post", kw)))

    class ProviderError(Exception):
        status_code = 400

    with (patch("hermes_cli.plugins.get_plugin_manager", return_value=manager),
          patch("model_tools.get_tool_definitions", return_value=[]),
          patch("model_tools.check_toolset_requirements", return_value={}),
          patch("agent.process_bootstrap.OpenAI", return_value=MagicMock())):
        agent = AIAgent(api_key="test-key-1234567890", provider="openrouter",
                        base_url="https://openrouter.ai/api/v1", model="test-model",
                        quiet_mode=True, skip_context_files=True, skip_memory=True)
        agent._interruptible_api_call = MagicMock(side_effect=ProviderError("bad request"))
        result = agent.run_conversation("hello")
        result2 = agent.run_conversation("again", conversation_history=result["messages"])

    starts = [payload for name, payload in events if name == "start"]
    ends = [payload for name, payload in events if name == "end"]
    assert result["failed"] is True
    assert result2["failed"] is True
    assert len(starts) == 1
    assert len(ends) == 2
    assert ends[0]["session_id"] == starts[0]["session_id"]
    assert ends[0]["failed"] is True
    assert ends[0]["completed"] is False
    assert ends[0]["turn_id"] != ends[1]["turn_id"]
    assert not [payload for name, payload in events if name == "post"]
