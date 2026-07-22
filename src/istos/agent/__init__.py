"""Agent loop over mesh tools — plan → ``query_once`` → observe.

The fabric primitives stay as they are (``@channel``, ``@handle``, queues).
This package is the glue that turns a channel handler into an agent whose tools
are other services on Zenoh.
"""

from istos.agent.loop import AgentEvent, drive_channel, history_to_messages, run_agent, user_text
from istos.agent.model import Model, ModelError, ModelReply, OpenAIChatModel, ToolCall
from istos.agent.multi import Agent, build_registry, drive_agents, run_multi_agent
from istos.agent.tools import MeshTool, tool_name, tools_from_handlers

__all__ = [
    "Agent",
    "AgentEvent",
    "MeshTool",
    "Model",
    "ModelError",
    "ModelReply",
    "OpenAIChatModel",
    "ToolCall",
    "build_registry",
    "drive_agents",
    "drive_channel",
    "history_to_messages",
    "run_agent",
    "run_multi_agent",
    "tool_name",
    "tools_from_handlers",
    "user_text",
]
