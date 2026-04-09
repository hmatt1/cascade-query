from __future__ import annotations

import math
import types
from dataclasses import dataclass
from typing import NamedTuple

import pytest

from cascade._serde import (
    PERSISTENCE_FORMAT,
    dumps_payload,
    loads_payload,
    stable_value_digest,
)
from cascade._serde import _from_jsonable as from_j
from cascade._serde import _resolve_type as resolve_type
from cascade._serde import _to_jsonable as to_j
from cascade._state import Dependency, InputVersion, MemoEntry, TraceEvent


@dataclass(frozen=True)
class _SampleDC:
    a: int
    b: str


class _SampleNT(NamedTuple):
    x: int
    y: int


def test_stable_digest_deterministic_for_collections_and_dataclass() -> None:
    assert stable_value_digest({"z": 1, "a": 2}) == stable_value_digest({"a": 2, "z": 1})
    assert stable_value_digest(_SampleDC(1, "x")) == stable_value_digest(_SampleDC(1, "x"))
    assert stable_value_digest(frozenset((3, 1, 2))) == stable_value_digest(frozenset((1, 2, 3)))


def test_stable_digest_float_specials_and_bytearray() -> None:
    h_nan = stable_value_digest(float("nan"))
    assert h_nan == stable_value_digest(math.copysign(float("nan"), 1.0))
    assert stable_value_digest(float("inf")) != stable_value_digest(float("-inf"))
    assert stable_value_digest(bytearray(b"ab")) == stable_value_digest(b"ab")


def test_namedtuple_round_trip_through_jsonable() -> None:
    p = _SampleNT(3, 4)
    assert from_j(to_j(p)) == p


def test_dataclass_round_trip() -> None:
    v = _SampleDC(9, "ok")
    assert from_j(to_j(v)) == v


def test_dumps_payload_round_trip_minimal() -> None:
    payload = {
        "revision": 0,
        "cancel_epoch": 0,
        "inputs": {},
        "memos": {},
        "dependents": {},
        "trace": [],
        "access_id": 0,
    }
    assert loads_payload(dumps_payload(payload)) == payload


def test_dumps_payload_round_trip_with_dataclass_memo_value() -> None:
    key = ("query", "q", ())
    memo = MemoEntry(
        value=_SampleDC(1, "z"),
        value_hash="h",
        changed_at=1,
        verified_at=1,
        deps=(),
        effects={},
        last_access=1,
    )
    payload = {
        "revision": 1,
        "cancel_epoch": 0,
        "inputs": {},
        "memos": {key: memo},
        "dependents": {},
        "trace": [],
        "access_id": 0,
    }
    out = loads_payload(dumps_payload(payload))
    assert out["memos"][key].value == _SampleDC(1, "z")


def test_loads_payload_rejects_bad_envelope() -> None:
    with pytest.raises(ValueError, match="persistence envelope"):
        loads_payload(b"[]")
    with pytest.raises(ValueError, match="unsupported persistence format"):
        loads_payload(
            json_bytes({"format": 0, "payload": {"__map__": []}})
        )
    with pytest.raises(ValueError, match="missing payload field"):
        loads_payload(json_bytes({"format": PERSISTENCE_FORMAT}))
    with pytest.raises(ValueError, match="payload must decode to a dict"):
        loads_payload(
            json_bytes({"format": PERSISTENCE_FORMAT, "payload": {"__list__": [1, 2]}})
        )


def json_bytes(obj: object) -> bytes:
    import json

    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def test_from_jsonable_rejects_non_dict_top() -> None:
    with pytest.raises(TypeError, match="expected dict"):
        from_j([1, 2])


def test_from_jsonable_rejects_multi_key_plain_dict() -> None:
    with pytest.raises(TypeError, match="unrecognized object shape"):
        from_j({"a": 1, "b": 2})


def test_from_jsonable_map_requires_pairs() -> None:
    with pytest.raises(ValueError, match="invalid __map__ entry"):
        from_j({"__map__": [[1]]})


def test_from_jsonable_memoentry_deps_must_be_list() -> None:
    bad = {
        "__MemoEntry__": {
            "value": 1,
            "value_hash": "x",
            "changed_at": 0,
            "verified_at": 0,
            "deps": {"__map__": []},
            "effects": {"__map__": []},
            "last_access": 0,
        }
    }
    with pytest.raises(ValueError, match="MemoEntry.deps must decode to a list"):
        from_j(bad)


def test_resolve_type_builtin_int() -> None:
    assert resolve_type("builtins", "int") is int


def test_resolve_type_rejects_non_type_global() -> None:
    with pytest.raises(TypeError, match="is not a type"):
        resolve_type("builtins", "len")


def test_dataclass_decode_falls_back_to_namespace() -> None:
    blob = {
        "__dataclass__": {
            "m": "no_such_module_xyz",
            "q": "Missing",
            "f": [["a", 1]],
        }
    }
    ns = from_j(blob)
    assert isinstance(ns, types.SimpleNamespace)
    assert ns.a == 1


def test_namedtuple_decode_falls_back_to_plain_tuple() -> None:
    blob = {
        "__namedtuple__": {
            "m": "no_such_module_xyz",
            "q": "Missing",
            "f": [1, 2],
        }
    }
    assert from_j(blob) == (1, 2)


def test_trace_event_and_dependency_round_trip() -> None:
    te = TraceEvent(event="e", key="k", revision=3, detail="d", timestamp=1.5)
    assert from_j(to_j(te)) == te
    dep = Dependency(key=("query", "q", ()), observed_changed_at=2)
    assert from_j(to_j(dep)) == dep


def test_input_version_round_trip() -> None:
    iv = InputVersion(revision=1, changed_at=2, value_hash="hh", value=(1, 2))
    assert from_j(to_j(iv)) == iv


def test_to_jsonable_rejects_arbitrary_object() -> None:
    class O:
        pass

    with pytest.raises(TypeError, match="unsupported type"):
        to_j(O())


def test_decode_float_nan_and_negative_infinity() -> None:
    assert math.isnan(from_j({"__float__": "nan", "sign": -1.0}))
    assert from_j({"__float__": "inf", "sign": -1.0}) == float("-inf")


def test_from_jsonable_passes_through_json_float() -> None:
    assert from_j(1.25) == 1.25


def test_from_jsonable_set_and_frozenset_tags() -> None:
    assert from_j({"__set__": [1, 2]}) == {1, 2}
    assert from_j({"__frozenset__": [1, 2]}) == frozenset((1, 2))


def test_from_jsonable_bytes_tag() -> None:
    import base64

    b = b"xyz"
    enc = base64.standard_b64encode(b).decode("ascii")
    assert from_j({"__bytes__": enc}) == b
