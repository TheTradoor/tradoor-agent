"""In-process pub/sub event bus. One asyncio.Queue per subscriber per topic.

Publishing never blocks: if a subscriber is too slow its queue fills and newer
events are dropped (and counted). That is the right behaviour for market data;
components that must not lose events use their own persistence.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, defaultdict
from typing import Any

log = logging.getLogger(__name__)

TOPIC_TOKEN_NEW = "token.new"
TOPIC_TOKEN_MIGRATED = "token.migrated"
TOPIC_TRADE = "trade"
TOPIC_CANDIDATE = "candidate"
TOPIC_SCORED = "candidate.scored"
TOPIC_DEXPAID = "dexpaid"
TOPIC_DECISION = "decision"
TOPIC_ORDER = "order"
TOPIC_POSITION = "position"
TOPIC_ALERT = "alert"


class EventBus:
    def __init__(self, default_maxsize: int = 10_000) -> None:
        self._subs: dict[str, list[asyncio.Queue]] = defaultdict(list)
        self._default_maxsize = default_maxsize
        self.dropped: Counter = Counter()
        self.published: Counter = Counter()

    def subscribe(self, topic: str, maxsize: int | None = None) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=maxsize or self._default_maxsize)
        self._subs[topic].append(q)
        return q

    def publish(self, topic: str, event: Any) -> None:
        self.published[topic] += 1
        for q in self._subs.get(topic, ()):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                n = self.dropped[topic] = self.dropped[topic] + 1
                if n in (1, 100, 1_000) or n % 10_000 == 0:
                    log.warning("bus: dropped %d events on topic %s (slow subscriber)", n, topic)

    def alert(self, text: str, level: str = "info") -> None:
        """Human-facing alert. Telegram ops forwards these to the owner."""
        self.publish(TOPIC_ALERT, {"text": text, "level": level, "ts": time.time()})
