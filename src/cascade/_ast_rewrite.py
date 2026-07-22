"""AST interception for ``@query`` functions.

The developer writes completely standard Python. Before a query function is
registered, this module parses its source, finds supported reducer calls
(``sum``, ``len``, ``min``, ``max``, ``any``, ``all``, ``list``, ``set``,
``dict``, ``sorted``, and string-literal ``.join``) over supported pipelines
(single-``for`` list/generator comprehensions, ``map``, ``filter``,
``reversed``), and rewrites each site into a call to the engine's incremental
runtime. The runtime decides at call time whether the source is a cascade
collection; anything else runs with plain Python semantics.

Boundaries are strict and fail toward normal execution:

* ``enumerate()`` and ``zip()`` sources are explicitly excluded; the code is
  left exactly as written and runs the standard O(N) way. Prepending to a
  list shifts every ``enumerate`` index, and ``zip`` would need stateful
  stream alignment across several diff queues, so neither can be made
  incremental safely.
* Arbitrary ``for`` loops are never touched.
* If any relevant builtin name is shadowed in the function or its module, or
  the source cannot be parsed, or the function is async or a lambda, the
  function is registered unmodified.

The rewritten function is compiled against the original filename with the
original line numbers, and its closure cells are the original cell objects,
so stack traces and closures behave exactly as they would without rewriting.
"""

from __future__ import annotations

import ast
import builtins
import inspect
import textwrap
import types
from typing import Any, Callable

RT_NAME = "__cascade_rt__"

_REDUCER_NAMES = frozenset(
    {"sum", "len", "min", "max", "any", "all", "list", "set", "dict", "sorted"}
)
_PIPE_NAMES = frozenset({"map", "filter", "reversed"})
_RELEVANT = _REDUCER_NAMES | _PIPE_NAMES | {"bool"}
_EXCLUDED_SOURCES = frozenset({"enumerate", "zip"})

_ABORT = object()


def rewrite_query(fn: Callable[..., Any], rt: Any) -> Callable[..., Any] | None:
    """Rewritten version of ``fn`` bound to runtime ``rt``, or None to skip."""
    try:
        return _rewrite(fn, rt)
    except Exception:
        return None


def _rewrite(fn: Callable[..., Any], rt: Any) -> Callable[..., Any] | None:
    if not hasattr(fn, "__code__") or fn.__name__ == "<lambda>":
        return None
    if inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn):
        return None
    for name in _RELEVANT | _EXCLUDED_SOURCES:
        if name in fn.__globals__ and fn.__globals__[name] is not getattr(
            builtins, name, None
        ):
            return None

    lines, start = inspect.getsourcelines(fn)
    tree = ast.parse(textwrap.dedent("".join(lines)))
    ast.increment_lineno(tree, start - 1)
    fdef = tree.body[0]
    if not isinstance(fdef, ast.FunctionDef) or fdef.name != fn.__name__:
        return None
    if _shadows_relevant_names(fdef):
        return None

    rewriter = _Rewriter(fn.__qualname__)
    fdef.body = rewriter.rewrite_block(fdef.body)
    if rewriter.count == 0:
        return None

    fdef.decorator_list = []
    _strip_annotations(fdef)
    return _compile(fn, fdef, rt)


def _shadows_relevant_names(fdef: ast.FunctionDef) -> bool:
    bound: set[str] = {a.arg for a in ast.walk(fdef) if isinstance(a, ast.arg)}
    for node in ast.walk(fdef):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            bound.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(node.name)
        elif isinstance(node, ast.alias):
            bound.add((node.asname or node.name).split(".")[0])
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            bound.update(node.names)
    return bool(bound & _RELEVANT)


def _strip_annotations(fdef: ast.FunctionDef) -> None:
    # Annotations would be re-evaluated eagerly by the recompile even if the
    # module used ``from __future__ import annotations``; they are metadata,
    # so drop them from the AST and copy ``__annotations__`` over afterwards.
    for node in ast.walk(fdef):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.returns = None
        elif isinstance(node, ast.arg):
            node.annotation = None


class _Rewriter(ast.NodeTransformer):
    def __init__(self, qualname: str) -> None:
        self.qualname = qualname
        self.count = 0
        self._n = 0
        self._pending: list[list[ast.stmt]] = []

    # --- statement-level plumbing ---
    def rewrite_block(self, stmts: list[ast.stmt]) -> list[ast.stmt]:
        out: list[ast.stmt] = []
        for stmt in stmts:
            self._pending.append([])
            new_stmt = self.visit(stmt)
            out.extend(self._pending.pop())
            if isinstance(new_stmt, list):
                out.extend(new_stmt)
            elif new_stmt is not None:
                out.append(new_stmt)
        return out

    def _visit_compound(self, node: ast.AST) -> ast.AST:
        for name, old in ast.iter_fields(node):
            if isinstance(old, list):
                if old and isinstance(old[0], ast.stmt):
                    setattr(node, name, self.rewrite_block(old))
                else:
                    items = []
                    for item in old:
                        if isinstance(item, (ast.ExceptHandler, ast.match_case)):
                            self._visit_compound(item)
                            items.append(item)
                        elif isinstance(item, ast.AST):
                            items.append(self.visit(item))
                        else:
                            items.append(item)
                    setattr(node, name, items)
            elif isinstance(old, ast.AST):
                setattr(node, name, self.visit(old))
        return node

    visit_FunctionDef = _visit_compound
    visit_ClassDef = _visit_compound
    visit_For = _visit_compound
    visit_While = _visit_compound
    visit_If = _visit_compound
    visit_With = _visit_compound
    visit_Try = _visit_compound
    visit_TryStar = _visit_compound
    visit_Match = _visit_compound

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AST:
        return node

    def visit_AsyncFor(self, node: ast.AsyncFor) -> ast.AST:
        return node

    def visit_AsyncWith(self, node: ast.AsyncWith) -> ast.AST:
        return self._visit_compound(node)

    def visit_Lambda(self, node: ast.Lambda) -> ast.AST:
        # A lambda body has no statement list to hold synthesized stage
        # functions, so everything inside runs unmodified.
        return node

    # --- the actual interception ---
    def visit_Call(self, node: ast.Call) -> ast.AST:
        matched = self._match_terminal(node)
        if matched is None:
            return self.generic_visit(node)
        reducer, pipe_expr, extra = matched
        pipeline = _match_pipeline(pipe_expr)
        if pipeline is _ABORT or pipeline is None:
            return self.generic_visit(node)
        source, stages = pipeline
        return self._build_run_call(node, reducer, source, stages, extra)

    def visit_ListComp(self, node: ast.ListComp) -> ast.AST:
        # A bare comprehension is its own terminal: [f(x) for x in xs]
        # is list-reduce over a map stage.
        pipeline = _match_pipeline(node)
        if pipeline is _ABORT:
            return self.generic_visit(node)
        source, stages = pipeline
        return self._build_run_call(node, "list", source, stages, [])

    def visit_SetComp(self, node: ast.SetComp) -> ast.AST:
        decomposed = _decompose_comp(node, node.elt)
        if decomposed is _ABORT:
            return self.generic_visit(node)
        source, stages = decomposed
        return self._build_run_call(node, "set", source, stages, [])

    def visit_DictComp(self, node: ast.DictComp) -> ast.AST:
        target = node.generators[0].target if node.generators else None
        if (
            isinstance(target, ast.Tuple)
            and len(target.elts) == 2
            and all(isinstance(e, ast.Name) for e in target.elts)
            and isinstance(node.key, ast.Name)
            and isinstance(node.value, ast.Name)
            and node.key.id == target.elts[0].id
            and node.value.id == target.elts[1].id
        ):
            elt: ast.expr | None = None  # {k: v for k, v in pairs}: identity
        else:
            elt = ast.Tuple(elts=[node.key, node.value], ctx=ast.Load())
            ast.copy_location(elt, node.key)
        decomposed = _decompose_comp(node, elt)
        if decomposed is _ABORT:
            return self.generic_visit(node)
        source, stages = decomposed
        return self._build_run_call(node, "dict", source, stages, [])

    def _match_terminal(
        self, node: ast.Call
    ) -> tuple[str, ast.expr, list[ast.keyword]] | None:
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "join"
            and isinstance(func.value, ast.Constant)
            and isinstance(func.value.value, str)
            and len(node.args) == 1
            and not node.keywords
        ):
            return "join", node.args[0], [ast.keyword(arg="sep", value=func.value)]
        if not isinstance(func, ast.Name):
            return None
        name = func.id
        if name == "sorted":
            if len(node.args) != 1:
                return None
            extra = []
            for kw in node.keywords:
                if kw.arg == "key":
                    extra.append(ast.keyword(arg="key", value=kw.value))
                elif kw.arg == "reverse":
                    extra.append(ast.keyword(arg="sort_reverse", value=kw.value))
                else:
                    return None
            return "sorted", node.args[0], extra
        if name in _REDUCER_NAMES and len(node.args) == 1 and not node.keywords:
            if name == "len" and isinstance(node.args[0], ast.GeneratorExp):
                return None  # len() of a generator is a TypeError; keep it one
            if any(isinstance(a, ast.Starred) for a in node.args):
                return None
            return name, node.args[0], []
        return None

    def _build_run_call(
        self,
        node: ast.Call,
        reducer: str,
        source: ast.expr,
        stages: list[tuple[Any, ...]],
        extra: list[ast.keyword],
    ) -> ast.AST:
        stage_nodes: list[ast.expr] = []
        for stage in stages:
            kind = stage[0]
            if kind == "reverse":
                stage_nodes.append(
                    ast.Tuple(elts=[ast.Constant("reverse")], ctx=ast.Load())
                )
            elif kind in ("map", "filter"):
                stage_nodes.append(
                    ast.Tuple(
                        elts=[ast.Constant(kind), self.visit(stage[1])], ctx=ast.Load()
                    )
                )
            else:  # comp_map / comp_filter: synthesize a one-item function
                _, target, body = stage
                fn_name = self._synthesize(target, body, node)
                stage_nodes.append(
                    ast.Tuple(
                        elts=[
                            ast.Constant("map" if kind == "comp_map" else "filter"),
                            ast.Name(id=fn_name, ctx=ast.Load()),
                        ],
                        ctx=ast.Load(),
                    )
                )
        for kw in extra:
            kw.value = self.visit(kw.value)
        call = ast.Call(
            func=ast.Attribute(
                value=ast.Name(id=RT_NAME, ctx=ast.Load()), attr="run", ctx=ast.Load()
            ),
            args=[
                ast.Constant(f"{self.qualname}:{node.lineno}:{node.col_offset}"),
                ast.Constant(reducer),
                self.visit(source),
                ast.Tuple(elts=stage_nodes, ctx=ast.Load()),
            ],
            keywords=extra,
        )
        ast.copy_location(call, node)
        self.count += 1
        return call

    def _synthesize(self, target: ast.expr, body: ast.expr, site: ast.Call) -> str:
        self._n += 1
        fn_name = f"__cq{self._n}"
        item = f"__cq_item{self._n}"
        self._pending.append([])
        new_body = self.visit(body)
        inner_pending = self._pending.pop()
        fdef = ast.FunctionDef(
            name=fn_name,
            args=ast.arguments(
                posonlyargs=[],
                args=[ast.arg(arg=item)],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[],
            ),
            body=[
                ast.Assign(targets=[target], value=ast.Name(id=item, ctx=ast.Load())),
                *inner_pending,
                ast.Return(value=new_body),
            ],
            decorator_list=[],
        )
        ast.copy_location(fdef, site)
        self._pending[-1].append(fdef)
        return fn_name


def _decompose_comp(
    comp: ast.ListComp | ast.GeneratorExp | ast.SetComp | ast.DictComp,
    elt: ast.expr | None,
) -> tuple[ast.expr, list[tuple[Any, ...]]] | object:
    """Turn a single-generator comprehension into (source, stages).

    ``elt`` of None means the element passes through unchanged (identity).
    """
    if len(comp.generators) != 1:
        return _ABORT
    gen = comp.generators[0]
    if gen.is_async:
        return _ABORT
    inner = _match_pipeline(gen.iter)
    if inner is _ABORT:
        return _ABORT
    source, stages = inner
    for cond in gen.ifs:
        stages.append(("comp_filter", gen.target, cond))
    if elt is not None and not (
        isinstance(elt, ast.Name)
        and isinstance(gen.target, ast.Name)
        and elt.id == gen.target.id
    ):
        stages.append(("comp_map", gen.target, elt))
    return source, stages


def _match_pipeline(expr: ast.expr) -> tuple[ast.expr, list[tuple[Any, ...]]] | object:
    """Decompose ``expr`` into (source, stages), _ABORT, or a bare source."""
    if isinstance(expr, (ast.SetComp, ast.DictComp)):
        # Set/dict comprehensions under a reducer call change multiplicity
        # semantics (dedup before reduce); keep standard execution.
        return _ABORT
    if isinstance(expr, (ast.ListComp, ast.GeneratorExp)):
        return _decompose_comp(expr, expr.elt)
    if (
        isinstance(expr, ast.Call)
        and isinstance(expr.func, ast.Name)
        and not expr.keywords
    ):
        name = expr.func.id
        if name in _EXCLUDED_SOURCES:
            return _ABORT
        if name in ("map", "filter") and len(expr.args) == 2:
            if any(isinstance(a, ast.Starred) for a in expr.args):
                return _ABORT
            inner = _match_pipeline(expr.args[1])
            if inner is _ABORT:
                return _ABORT
            source, stages = inner
            fn_expr = expr.args[0]
            if (
                name == "filter"
                and isinstance(fn_expr, ast.Constant)
                and fn_expr.value is None
            ):
                fn_expr = ast.copy_location(
                    ast.Name(id="bool", ctx=ast.Load()), fn_expr
                )
            stages.append((name, fn_expr))
            return source, stages
        if name == "map" and len(expr.args) > 2:
            return _ABORT  # multi-iterable map is zip-shaped; standard execution
        if name == "reversed" and len(expr.args) == 1:
            inner = _match_pipeline(expr.args[0])
            if inner is _ABORT or inner[1]:
                return _ABORT  # reversed() of an iterator is a TypeError; keep it one
            return inner[0], [("reverse",)]
        if name == "sorted":
            return _ABORT  # nested sorted stays on the standard path
    return expr, []


def _compile(
    fn: Callable[..., Any], fdef: ast.FunctionDef, rt: Any
) -> Callable[..., Any]:
    cell_names = list(fn.__code__.co_freevars) + [RT_NAME]
    outer = ast.FunctionDef(
        name="__cascade_outer",
        args=ast.arguments(
            posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]
        ),
        body=[
            ast.Assign(
                targets=[ast.Name(id=n, ctx=ast.Store()) for n in cell_names],
                value=ast.Constant(None),
            ),
            fdef,
        ],
        decorator_list=[],
    )
    module = ast.Module(body=[outer], type_ignores=[])
    ast.fix_missing_locations(module)
    code = compile(module, fn.__code__.co_filename, "exec")
    outer_code = _find_code(code, "__cascade_outer")
    inner_code = _find_code(outer_code, fn.__name__).replace(
        co_qualname=fn.__qualname__
    )

    orig_cells = dict(zip(fn.__code__.co_freevars, fn.__closure__ or ()))
    closure = tuple(
        types.CellType(rt) if name == RT_NAME else orig_cells[name]
        for name in inner_code.co_freevars
    )
    new_fn = types.FunctionType(
        inner_code, fn.__globals__, fn.__name__, fn.__defaults__, closure
    )
    new_fn.__kwdefaults__ = fn.__kwdefaults__
    new_fn.__qualname__ = fn.__qualname__
    new_fn.__doc__ = fn.__doc__
    new_fn.__module__ = fn.__module__
    new_fn.__annotations__ = dict(fn.__annotations__)
    new_fn.__dict__.update(fn.__dict__)
    new_fn.__wrapped__ = fn
    new_fn.__cascade_rewritten__ = True
    return new_fn


def _find_code(container: types.CodeType, name: str) -> types.CodeType:
    for const in container.co_consts:
        if isinstance(const, types.CodeType) and const.co_name == name:
            return const
    raise LookupError(name)
