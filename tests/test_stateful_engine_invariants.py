from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from hypothesis import HealthCheck, settings, strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, precondition, rule

from cascade import Engine, Snapshot
from tests.scale_helpers import assert_internal_dependents_consistent

INDEXES = st.integers(min_value=0, max_value=5)
SMALL_INTS = st.integers(min_value=-8, max_value=8)
MAYBE_INT = st.one_of(st.none(), SMALL_INTS)


class EngineStateMachine(RuleBasedStateMachine):
    snapshots = Bundle("snapshots")

    def __init__(self) -> None:
        super().__init__()
        self._tmpdir = TemporaryDirectory()
        self._state_db = Path(self._tmpdir.name) / "state.db"
        self._revision = 0
        self._history: dict[tuple[str, tuple[int, ...]], list[tuple[int, int | None]]] = {}
        self._id_to_name: dict[str, str] = {}
        self._observed_indexes: set[int] = set()
        self._choose_compute_count = 0
        self._build_engine()

    def teardown(self) -> None:
        self._tmpdir.cleanup()

    def _build_engine(self) -> None:
        engine = Engine()

        @engine.input
        def mode() -> int:
            return 0

        @engine.input
        def left(index: int) -> int | None:
            return index

        @engine.input
        def right(index: int) -> int | None:
            return -index

        @engine.input
        def bias() -> int:
            return 0

        @engine.query
        def choose(index: int) -> int:
            self._choose_compute_count += 1
            active = left(index) if mode() == 0 else right(index)
            return (0 if active is None else active) + bias()

        @engine.query
        def pair(index: int) -> int:
            return choose(index) + choose(index + 1)

        self.engine = engine
        self.mode = mode
        self.left = left
        self.right = right
        self.bias = bias
        self.choose = choose
        self.pair = pair
        self._id_to_name = {
            self.mode.id: "mode",
            self.left.id: "left",
            self.right.id: "right",
            self.bias.id: "bias",
        }

    def _sync_from_engine(self) -> None:
        self._revision = self.engine.revision
        rebuilt: dict[tuple[str, tuple[int, ...]], list[tuple[int, int | None]]] = {}
        for (input_id, args), versions in self.engine._inputs.items():  # noqa: SLF001
            name = self._id_to_name.get(input_id)
            if name is None:
                continue
            rebuilt[(name, args)] = [(version.revision, version.value) for version in versions]
        self._history = rebuilt

    def _default_value(self, name: str, args: tuple[int, ...]) -> int | None:
        if name == "mode":
            return 0
        if name == "bias":
            return 0
        index = args[0]
        if name == "left":
            return index
        if name == "right":
            return -index
        raise AssertionError(f"unknown input name {name}")

    def _value_at(self, name: str, args: tuple[int, ...], revision: int) -> int | None:
        timeline = self._history.get((name, args))
        if timeline:
            for event_revision, value in reversed(timeline):
                if event_revision <= revision:
                    return value
        return self._default_value(name, args)

    def _record_set(self, name: str, args: tuple[int, ...], value: int | None) -> None:
        self._sync_from_engine()
        timeline = self._history[(name, args)]
        assert timeline[-1][1] == value

    def _expected_choose(self, index: int, revision: int) -> int:
        current_mode = self._value_at("mode", (), revision)
        left_value = self._value_at("left", (index,), revision)
        right_value = self._value_at("right", (index,), revision)
        active = left_value if current_mode == 0 else right_value
        normalized = 0 if active is None else active
        bias_value = self._value_at("bias", (), revision)
        assert bias_value is not None
        return normalized + bias_value

    @rule(value=st.integers(min_value=0, max_value=1))
    def set_mode(self, value: int) -> None:
        self.mode.set(value=value)
        self._record_set("mode", (), value)

    @rule(index=INDEXES, value=MAYBE_INT)
    def set_left(self, index: int, value: int | None) -> None:
        self.left.set(index, value=value)
        self._record_set("left", (index,), value)

    @rule(index=INDEXES, value=MAYBE_INT)
    def set_right(self, index: int, value: int | None) -> None:
        self.right.set(index, value=value)
        self._record_set("right", (index,), value)

    @rule(value=SMALL_INTS)
    def set_bias(self, value: int) -> None:
        self.bias.set(value=value)
        self._record_set("bias", (), value)

    @rule(index=INDEXES)
    def read_choose_live(self, index: int) -> None:
        actual = self.choose(index)
        self._sync_from_engine()
        assert actual == self._expected_choose(index, self._revision)
        self._observed_indexes.add(index)

    @rule(index=INDEXES)
    def read_pair_live(self, index: int) -> None:
        actual = self.pair(index)
        self._sync_from_engine()
        expected = self._expected_choose(index, self._revision) + self._expected_choose(index + 1, self._revision)
        assert actual == expected
        self._observed_indexes.update({index, index + 1})

    @rule(target=snapshots)
    def take_snapshot(self) -> Snapshot:
        self._sync_from_engine()
        snapshot = self.engine.snapshot()
        assert snapshot.revision == self._revision
        return snapshot

    @rule(snapshot=snapshots, index=INDEXES)
    def read_choose_snapshot(self, snapshot: Snapshot, index: int) -> None:
        self._sync_from_engine()
        assert self.choose(index, snapshot=snapshot) == self._expected_choose(index, snapshot.revision)

    @rule(index=INDEXES, value=MAYBE_INT)
    def inactive_branch_mutation_does_not_recompute_choose(self, index: int, value: int | None) -> None:
        self._sync_from_engine()
        assert self.choose(index) == self._expected_choose(index, self._revision)
        self._sync_from_engine()
        calls_before = self._choose_compute_count
        mode_value = self._value_at("mode", (), self._revision)
        if mode_value == 0:
            self.right.set(index, value=value)
            self._record_set("right", (index,), value)
        else:
            self.left.set(index, value=value)
            self._record_set("left", (index,), value)

        assert self.choose(index) == self._expected_choose(index, self._revision)
        self._sync_from_engine()
        assert self._choose_compute_count == calls_before

    @precondition(lambda self: self._revision > 0)
    @rule()
    def save_and_reload_engine(self) -> None:
        self._sync_from_engine()
        self.engine.save(str(self._state_db))
        self._build_engine()
        self.engine.load(str(self._state_db))
        self._sync_from_engine()
        assert self.engine.revision == self._revision
        assert self.engine._in_flight == {}  # noqa: SLF001
        for index in sorted(self._observed_indexes)[:3]:
            assert self.choose(index) == self._expected_choose(index, self._revision)

    @invariant()
    def revision_matches_model(self) -> None:
        self._sync_from_engine()
        assert self.engine.revision == self._revision

    @invariant()
    def internal_dependent_index_remains_consistent(self) -> None:
        assert_internal_dependents_consistent(self.engine)

    @invariant()
    def in_flight_map_is_empty_after_operations(self) -> None:
        assert self.engine._in_flight == {}  # noqa: SLF001


TestEngineStateMachine = EngineStateMachine.TestCase
TestEngineStateMachine.settings = settings(
    max_examples=35,
    stateful_step_count=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
