from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Sequence

try:
    from agents import (
        function_span as _agents_function_span,
        generation_span as _agents_generation_span,
        get_current_trace as _agents_get_current_trace,
        set_tracing_disabled as _agents_set_tracing_disabled,
        set_tracing_export_api_key as _agents_set_tracing_export_api_key,
        trace as _agents_trace,
    )
except ImportError:  # pragma: no cover - optional runtime dependency
    _agents_function_span = None
    _agents_generation_span = None
    _agents_get_current_trace = None
    _agents_set_tracing_disabled = None
    _agents_set_tracing_export_api_key = None
    _agents_trace = None


@dataclass(frozen=True)
class OpenAITraceContext:
    workflow_name: str | None = None
    group_id: str | None = None
    metadata: dict[str, Any] | None = None


_CURRENT_TRACE_CONTEXT: ContextVar[OpenAITraceContext | None] = ContextVar(
    "lovelychaos_openai_trace_context",
    default=None,
)


def configure_openai_tracing(*, api_key: str, enabled: bool) -> None:
    if _agents_set_tracing_disabled is None:
        return
    _agents_set_tracing_disabled(not enabled)
    if enabled and api_key and _agents_set_tracing_export_api_key is not None:
        _agents_set_tracing_export_api_key(api_key)


@contextmanager
def request_trace_context(
    *,
    workflow_name: str | None = None,
    group_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> Iterator[None]:
    token = _CURRENT_TRACE_CONTEXT.set(
        OpenAITraceContext(
            workflow_name=workflow_name,
            group_id=group_id,
            metadata=dict(metadata or {}),
        )
    )
    try:
        yield
    finally:
        _CURRENT_TRACE_CONTEXT.reset(token)


def current_trace_context() -> OpenAITraceContext | None:
    return _CURRENT_TRACE_CONTEXT.get()


@contextmanager
def workflow_trace(
    workflow_name: str,
    *,
    group_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    disabled: bool = False,
) -> Iterator[None]:
    if _agents_trace is None or disabled:
        yield
        return
    with _agents_trace(workflow_name, group_id=group_id, metadata=metadata or {}, disabled=disabled):
        yield


@contextmanager
def function_trace_span(
    name: str,
    *,
    input_text: str | None = None,
    output_text: str | None = None,
    disabled: bool = False,
) -> Iterator[None]:
    if (
        _agents_function_span is None
        or _agents_get_current_trace is None
        or _agents_get_current_trace() is None
        or disabled
    ):
        yield
        return
    with _agents_function_span(name=name, input=input_text, output=output_text, disabled=disabled):
        yield


@contextmanager
def generation_trace_span(
    *,
    input_items: Sequence[Mapping[str, Any]] | None = None,
    output_items: Sequence[Mapping[str, Any]] | None = None,
    model: str | None = None,
    model_config: Mapping[str, Any] | None = None,
    usage: dict[str, Any] | None = None,
    disabled: bool = False,
) -> Iterator[None]:
    if (
        _agents_generation_span is None
        or _agents_get_current_trace is None
        or _agents_get_current_trace() is None
        or disabled
    ):
        yield
        return
    with _agents_generation_span(
        input=input_items,
        output=output_items,
        model=model,
        model_config=model_config,
        usage=usage,
        disabled=disabled,
    ):
        yield
