import inspect
from typing import Any, Callable, Dict, Optional, Type, get_type_hints

try:
    from pydantic import BaseModel, ValidationError, create_model
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False


class SchemaValidationError(Exception):
    """Raised when incoming parameters fail schema validation."""
    def __init__(self, errors: Any, message: str = "Schema validation failed"):
        self.errors = errors
        super().__init__(f"{message}: {errors}")


def validate_params(func: Callable, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates and coerces incoming parameters against a function's type hints.

    Supports three modes:
      1. Function accepts a single Pydantic BaseModel → full model validation
      2. Function has typed parameters (int, str, float, etc.) → auto-coercion via dynamic model
      3. Function has no type hints → passthrough (no validation)

    Returns the validated and type-coerced parameters as a dict.
    Raises SchemaValidationError if validation fails.
    """
    sig = inspect.signature(func)
    hints = get_type_hints(func)

    # --- Mode 1: Single Pydantic BaseModel parameter ---
    non_self_params = [
        (name, param) for name, param in sig.parameters.items()
        if name != "self"
    ]

    if len(non_self_params) == 1:
        param_name, param = non_self_params[0]
        param_type = hints.get(param_name)
        if param_type and HAS_PYDANTIC and _is_basemodel(param_type):
            try:
                validated = param_type.model_validate(params)
                return {param_name: validated}
            except ValidationError as e:
                raise SchemaValidationError(e.errors()) from e

    # --- Mode 2: Auto-coerce individual typed parameters ---
    if not HAS_PYDANTIC or not hints:
        # No pydantic or no hints → passthrough
        return params

    # Build a dynamic Pydantic model from the function's signature
    field_definitions = {}
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        annotation = hints.get(name, Any)
        if annotation is Any:
            # No typed hint for this param, skip validation
            field_definitions[name] = (Any, ...)
        elif param.default is not inspect.Parameter.empty:
            field_definitions[name] = (annotation, param.default)
        else:
            field_definitions[name] = (annotation, ...)

    if not field_definitions:
        return params

    try:
        DynamicModel = create_model("DynamicValidation", **field_definitions)
        validated = DynamicModel.model_validate(params)
        return validated.model_dump()
    except ValidationError as e:
        raise SchemaValidationError(e.errors()) from e


def _is_basemodel(cls: Any) -> bool:
    """Check if a class is a Pydantic BaseModel subclass."""
    try:
        return isinstance(cls, type) and issubclass(cls, BaseModel)
    except TypeError:
        return False
