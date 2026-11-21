"""Content hashes used by the oracle and the gate.

raw: sha256 of the exact UTF-8 bytes of a _source (byte-exact truth for stored source).
canonical: sha256 of a normal form that survives Elasticsearch re-rendering (synthetic source,
re-inserted vectors): keys sorted, compact separators, strings decoded, and every number
replaced by its exact decimal value (Decimal, never float), so 1.0 == 1 == 1E+0 but
9007199254740993 != 9007199254740992 and 0.1000000000000000055511 != 0.1.
"""

import hashlib
import json
from decimal import Decimal


def _number(literal: str) -> str:
    d = Decimal(literal)
    if d.is_zero():
        return "\x00N0\x00"
    # Decimal.normalize() would round to the context's 28 digits; strip trailing zeros exactly.
    sign, digits, exp = d.as_tuple()
    n = len(digits)
    while digits[n - 1] == 0:
        n -= 1
    if n < len(digits):
        d = Decimal((sign, digits[:n], exp + len(digits) - n))
    return "\x00N" + format(d, "E") + "\x00"


def _reject_constant(name: str):
    raise ValueError(f"non-standard JSON constant {name}")


def canonical_text(source: str) -> str:
    value = json.loads(source, parse_float=_number, parse_int=_number, parse_constant=_reject_constant)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canonical_hash(source: str) -> str:
    return hashlib.sha256(canonical_text(source).encode()).hexdigest()


def raw_hash(source: str | bytes) -> str:
    return hashlib.sha256(source if isinstance(source, bytes) else source.encode()).hexdigest()
