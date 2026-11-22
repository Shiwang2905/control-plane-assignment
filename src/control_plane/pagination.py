"""Opaque keyset (cursor) pagination over (created_at, id).

Why keyset and not OFFSET: OFFSET skips/duplicates rows when new rows are inserted
between page fetches, and gets slower the deeper you page. A cursor that remembers
"the last (created_at, id) I saw" is stable and uses the index directly.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


class InvalidCursor(ValueError):
    pass


@dataclass(frozen=True)
class Cursor:
    created_at: datetime
    id: UUID

    def encode(self) -> str:
        raw = json.dumps({"c": self.created_at.isoformat(), "i": str(self.id)})
        return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")

    @classmethod
    def decode(cls, token: str) -> Cursor:
        try:
            padded = token + "=" * (-len(token) % 4)
            data = json.loads(base64.urlsafe_b64decode(padded.encode()))
            return cls(created_at=datetime.fromisoformat(data["c"]), id=UUID(data["i"]))
        except (ValueError, KeyError, TypeError, binascii.Error) as exc:
            raise InvalidCursor("cursor is malformed") from exc
