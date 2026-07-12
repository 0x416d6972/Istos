import inspect
from typing import Any, Callable, Dict, get_type_hints
import yaml

try:
    from pydantic import BaseModel, create_model
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False

from istos.core.validation import _is_basemodel


def get_function_schemas(func: Callable) -> Dict[str, Any]:
    """
    Extracts JSON Schema for parameters (payload/query) and return type
    of a given function, using Pydantic.
    Returns: 
        {
            "payload_schema": dict | None,
            "return_schema": dict | None
        }
    """
    if not HAS_PYDANTIC:
        return {"payload_schema": None, "return_schema": None}

    sig = inspect.signature(func)
    hints = get_type_hints(func)

    # Payload Schema
    payload_schema = None
    non_self_params = [
        (name, param) for name, param in sig.parameters.items()
        if name != "self"
    ]

    if len(non_self_params) == 1:
        param_name, param = non_self_params[0]
        param_type = hints.get(param_name)
        if param_type and _is_basemodel(param_type):
            payload_schema = param_type.model_json_schema()

    if payload_schema is None and non_self_params:
        # Dynamic model
        field_definitions = {}
        for name, param in non_self_params:
            annotation = hints.get(name, Any)
            if annotation is Any:
                field_definitions[name] = (Any, ...)
            elif param.default is not inspect.Parameter.empty:
                field_definitions[name] = (annotation, param.default)
            else:
                field_definitions[name] = (annotation, ...)
        
        if field_definitions:
            DynamicModel = create_model(f"{func.__name__}_Payload", **field_definitions)  # type: ignore[call-overload]
            payload_schema = DynamicModel.model_json_schema()
            
    # Return Schema
    return_schema = None
    return_hint = hints.get("return")
    if return_hint and return_hint is not type(None):
        if _is_basemodel(return_hint):
            return_schema = return_hint.model_json_schema()
        else:
            # Wrap in a dynamic model to get schema
            try:
                ReturnModel = create_model(f"{func.__name__}_Return", result=(return_hint, ...))
                schema = ReturnModel.model_json_schema()
                if "properties" in schema and "result" in schema["properties"]:
                    return_schema = schema["properties"]["result"]
            except Exception:
                pass

    return {
        "payload_schema": payload_schema,
        "return_schema": return_schema
    }


class AsyncApiGenerator:
    def __init__(self, title: str = "Istos Network", version: str = "1.0.0"):
        self.doc: Dict[str, Any] = {
            "asyncapi": "3.0.0",
            "info": {
                "title": title,
                "version": version
            },
            "channels": {},
            "operations": {},
            "components": {
                "messages": {},
                "schemas": {}
            }
        }
        self._message_count = 0

    def _register_message(self, name_hint: str, schema: dict) -> str:
        self._message_count += 1
        msg_id = f"{name_hint}Message_{self._message_count}"
        self.doc["components"]["messages"][msg_id] = {
            "payload": schema
        }
        return f"#/components/messages/{msg_id}"

    def generate(self, istos_instance: Any) -> str:
        """Generates the AsyncAPI YAML specification."""
        self.doc["asyncapi"] = "3.0.0"
        if "operations" not in self.doc:
            self.doc["operations"] = {}
        
        # Helper to setup channel
        def _ensure_channel(ch_name, address, title, description):
            if ch_name not in self.doc["channels"]:
                self.doc["channels"][ch_name] = {
                    "address": address,
                    "title": title,
                    "description": description,
                    "messages": {}
                }

        # 1. Handlers (RPC - Receive Query and Reply)
        for handler in istos_instance._handlers:
            schemas = get_function_schemas(handler.func)
            ch_name = handler.prefix.replace('/', '_').replace('*', 'star')
            _ensure_channel(ch_name, handler.prefix, f"Handler: {handler.func.__name__}", inspect.getdoc(handler.func) or "")
            
            op_id = f"handle_{handler.func.__name__}"
            op: Dict[str, Any] = {
                "action": "receive",
                "channel": { "$ref": f"#/channels/{ch_name}" },
                "tags": [{"name": "@handle"}, {"name": "RPC"}]
            }
            
            if schemas["payload_schema"]:
                msg_key = handler.func.__name__ + "_req"
                msg_ref = self._register_message(msg_key, schemas["payload_schema"])
                self.doc["channels"][ch_name]["messages"][msg_key] = { "$ref": msg_ref }
                op["messages"] = [{ "$ref": f"#/channels/{ch_name}/messages/{msg_key}" }]
                
            if schemas["return_schema"]:
                rep_msg_key = handler.func.__name__ + "_rep"
                rep_msg_ref = self._register_message(rep_msg_key, schemas["return_schema"])
                self.doc["channels"][ch_name]["messages"][rep_msg_key] = { "$ref": rep_msg_ref }
                op["reply"] = {
                    "messages": [{ "$ref": f"#/channels/{ch_name}/messages/{rep_msg_key}" }]
                }
                
            self.doc["operations"][op_id] = op

        # 2. Subscribers (Pub/Sub - Receive Events)
        for sub in istos_instance._subscribers:
            schemas = get_function_schemas(sub.func)
            ch_name = sub.prefix.replace('/', '_').replace('*', 'star')
            _ensure_channel(ch_name, sub.prefix, f"Topic: {sub.prefix}", inspect.getdoc(sub.func) or "")
                
            op_id = f"subscribe_{sub.func.__name__}"
            op = {
                "action": "receive",
                "channel": { "$ref": f"#/channels/{ch_name}" },
                "tags": [{"name": "@subscribe"}, {"name": "Pub/Sub"}]
            }
            
            if schemas["payload_schema"]:
                msg_key = sub.func.__name__ + "_event"
                msg_ref = self._register_message(msg_key, schemas["payload_schema"])
                self.doc["channels"][ch_name]["messages"][msg_key] = { "$ref": msg_ref }
                op["messages"] = [{ "$ref": f"#/channels/{ch_name}/messages/{msg_key}" }]
                
            self.doc["operations"][op_id] = op

        # 3. Publishers (Pub/Sub - Send Events)
        for pub in istos_instance._publishers:
            schemas = get_function_schemas(pub.func)
            ch_name = pub.prefix.replace('/', '_').replace('*', 'star')
            _ensure_channel(ch_name, pub.prefix, f"Topic: {pub.prefix}", inspect.getdoc(pub.func) or "")
                
            op_id = f"publish_{pub.func.__name__}"
            op = {
                "action": "send",
                "channel": { "$ref": f"#/channels/{ch_name}" },
                "tags": [{"name": "@publish"}, {"name": "Pub/Sub"}]
            }
            
            if schemas["return_schema"]:
                msg_key = pub.func.__name__ + "_event"
                msg_ref = self._register_message(msg_key, schemas["return_schema"])
                self.doc["channels"][ch_name]["messages"][msg_key] = { "$ref": msg_ref }
                op["messages"] = [{ "$ref": f"#/channels/{ch_name}/messages/{msg_key}" }]
                
            self.doc["operations"][op_id] = op

        # 4. Queries (RPC - Send Query and Expect Reply)
        for query in getattr(istos_instance, "_queries", []):
            schemas = get_function_schemas(query.func)
            ch_name = query.prefix.replace('/', '_').replace('*', 'star')
            _ensure_channel(ch_name, query.prefix, f"Query: {query.func.__name__}", inspect.getdoc(query.func) or "")
            
            op_id = f"query_{query.func.__name__}"
            op = {
                "action": "send",
                "channel": { "$ref": f"#/channels/{ch_name}" },
                "tags": [{"name": "@query"}, {"name": "RPC"}]
            }
            
            if schemas["payload_schema"]:
                msg_key = query.func.__name__ + "_req"
                msg_ref = self._register_message(msg_key, schemas["payload_schema"])
                self.doc["channels"][ch_name]["messages"][msg_key] = { "$ref": msg_ref }
                op["messages"] = [{ "$ref": f"#/channels/{ch_name}/messages/{msg_key}" }]
                
            if schemas["return_schema"]:
                rep_msg_key = query.func.__name__ + "_rep"
                rep_msg_ref = self._register_message(rep_msg_key, schemas["return_schema"])
                self.doc["channels"][ch_name]["messages"][rep_msg_key] = { "$ref": rep_msg_ref }
                op["reply"] = {
                    "messages": [{ "$ref": f"#/channels/{ch_name}/messages/{rep_msg_key}" }]
                }

            self.doc["operations"][op_id] = op

        def _safe_schemas(func: Callable) -> Dict[str, Any]:
            # A @channel handler takes a ChannelSession, which has no JSON Schema;
            # never let an unrepresentable parameter sink the whole document.
            try:
                return get_function_schemas(func)
            except Exception:
                return {"payload_schema": None, "return_schema": None}

        # 5. Streams (RPC - one request, many reply chunks)
        for stream in getattr(istos_instance, "_streams", []):
            schemas = _safe_schemas(stream.func)
            ch_name = stream.prefix.replace('/', '_').replace('*', 'star')
            _ensure_channel(ch_name, stream.prefix, f"Stream: {stream.func.__name__}", inspect.getdoc(stream.func) or "")

            op_id = f"stream_{stream.func.__name__}"
            op = {
                "action": "receive",
                "channel": { "$ref": f"#/channels/{ch_name}" },
                "tags": [{"name": "@stream"}, {"name": "Streaming"}]
            }
            if schemas["payload_schema"]:
                msg_key = stream.func.__name__ + "_req"
                msg_ref = self._register_message(msg_key, schemas["payload_schema"])
                self.doc["channels"][ch_name]["messages"][msg_key] = { "$ref": msg_ref }
                op["messages"] = [{ "$ref": f"#/channels/{ch_name}/messages/{msg_key}" }]
            self.doc["operations"][op_id] = op

        # 6. Channels (bidirectional - full-duplex session)
        for channel in getattr(istos_instance, "_channels", []):
            schemas = _safe_schemas(channel.func)
            ch_name = channel.prefix.replace('/', '_').replace('*', 'star')
            _ensure_channel(ch_name, channel.prefix, f"Channel: {channel.func.__name__}", inspect.getdoc(channel.func) or "")

            op_id = f"channel_{channel.func.__name__}"
            op = {
                "action": "receive",
                "channel": { "$ref": f"#/channels/{ch_name}" },
                "tags": [{"name": "@channel"}, {"name": "Bidirectional"}]
            }
            if schemas["payload_schema"]:
                msg_key = channel.func.__name__ + "_open"
                msg_ref = self._register_message(msg_key, schemas["payload_schema"])
                self.doc["channels"][ch_name]["messages"][msg_key] = { "$ref": msg_ref }
                op["messages"] = [{ "$ref": f"#/channels/{ch_name}/messages/{msg_key}" }]
            self.doc["operations"][op_id] = op

        return str(yaml.dump(self.doc, sort_keys=False))

def get_asyncapi_ui_html(title: str = "Istos Network Docs", schema_url: str = "/asyncapi.yaml") -> str:
    """Returns the HTML string for the AsyncAPI Standalone React UI."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{title}</title>
    <link rel="stylesheet" href="https://unpkg.com/@asyncapi/react-component@3.1.2/styles/default.min.css" />
</head>
<body style="margin: 0; padding: 0; height: 100vh; background: #fafafa;">
    <div id="asyncapi" style="width: 100%; height: 100%;"></div>
    <script src="https://unpkg.com/@asyncapi/react-component@3.1.2/browser/standalone/index.js"></script>
    <script>
      fetch('{schema_url}')
        .then(response => response.text())
        .then(schema => {{
          AsyncApiStandalone.render({{
            schema: schema,
            config: {{ show: {{ sidebar: true, errors: true }} }}
          }}, document.getElementById('asyncapi'));
        }})
        .catch(err => console.error("Failed to load schema", err));
    </script>
</body>
</html>"""
