"""
Publisher bindings: which sender may speak for which `<publisher>` segment of the archive topics.

Off by default: the historian then stores whatever arrives under the prefixes it watches, like every other VOLTTRON
historian. Switched on (`bind_publishers: true`), each publisher name is bound to one sender, the identity the platform
presents for the publication (or the RPC caller). Listed publishers (`publisher_bindings`) pin a sender in advance;
an unlisted publisher is bound to the first sender that uses its name, for the life of the database (the binding is
persisted by the store). Anything from another sender is dropped and counted; the count is logged at WARNING once per
minute per publisher and sender, so a flood is one line, not one per message.

Identities are written `identity@instance`. While the platform presents bare identities (`tn.campus` rather than
`tn.campus@campus`), a bare presented identity matches a qualified binding by its tail, and a bare learned binding
matches a qualified presented identity the same way; which form the platform presents is logged once. A bare binding
therefore does not distinguish the same identity on two platforms until the router qualifies identities.
"""
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass

_log = logging.getLogger(__name__)

WARNING_INTERVAL = 60.0  # seconds between warning lines for one (publisher, sender)


class Store:
    """What PublisherBindings needs from HistorianStore: the learned bindings."""

    def bound_sender(self, publisher: str) -> str | None: ...

    def bind_publisher(self, publisher: str, sender: str) -> None: ...


@dataclass(frozen=True)
class Decision:
    allowed: bool
    expected: str | None = None  # the sender bound to the publisher, when there is one
    message: str | None = None   # the refusal, worded for a status reply or RPC error


def parse_bindings_configuration(bind_publishers, publisher_bindings) -> tuple[bool, dict[str, str]]:
    """
    Validate the two configuration keys and return (enabled, {publisher: sender}). Raises ValueError saying what the
    keys must look like; a configured sender must be an `identity@instance` or a bare identity, never empty.
    """
    example = '{"bind_publishers": true, "publisher_bindings": {"<publisher>": {"sender": "identity@instance"}}}'
    if not isinstance(bind_publishers, bool):
        raise ValueError(f'"bind_publishers" must be true or false, got {bind_publishers!r}. Expected {example}.')
    if publisher_bindings is None:
        publisher_bindings = {}
    if not isinstance(publisher_bindings, Mapping):
        raise ValueError(f'"publisher_bindings" must be a mapping of publisher to {{"sender": ...}}, got '
                         f'{type(publisher_bindings).__name__}. Expected {example}.')
    bindings: dict[str, str] = {}
    for publisher, binding in publisher_bindings.items():
        if not isinstance(publisher, str) or not publisher or '/' in publisher:
            raise ValueError(f'"publisher_bindings" key {publisher!r} is not a publisher name (one topic segment). '
                             f'Expected {example}.')
        if not isinstance(binding, Mapping) or set(binding) != {'sender'}:
            raise ValueError(f'"publisher_bindings"[{publisher!r}] must be {{"sender": "identity@instance"}}, got '
                             f'{binding!r}.')
        sender = binding['sender']
        if not isinstance(sender, str) or not sender or sender.startswith('@') or sender.endswith('@') \
                or sender.count('@') > 1:
            raise ValueError(f'"publisher_bindings"[{publisher!r}]["sender"] must be "identity@instance" or a bare '
                             f'identity, got {sender!r}.')
        bindings[publisher] = sender
    return bind_publishers, bindings


def senders_match(expected: str, presented: str) -> bool:
    """Equal identities match; so do a bare one and a qualified one with that bare identity as its tail."""
    if expected == presented:
        return True
    expected_bare, presented_bare = '@' not in expected, '@' not in presented
    if expected_bare == presented_bare:
        return False
    return expected.split('@', 1)[0] == presented.split('@', 1)[0]


class PublisherBindings:

    def __init__(self, enabled: bool, configured: Mapping[str, str], clock=time.monotonic,
                 warning_interval: float = WARNING_INTERVAL):
        self.enabled = enabled
        self.configured: dict[str, str] = dict(configured)
        self.store: Store | None = None
        self._clock = clock
        self._warning_interval = warning_interval
        self._mismatches: dict[tuple[str, str], tuple[int, float]] = {}  # (publisher, sender) -> (count, last log)
        self.mismatch_counts: dict[str, int] = {}  # publisher -> total mismatches since start
        self._identity_form_logged = False

    def attach(self, store: Store, learned: Mapping[str, str]) -> None:
        """Give the bindings their persistence; a learned binding that conflicts with a configured one is reported,
        the configured one wins."""
        self.store = store
        if not self.enabled:
            return
        for publisher, sender in learned.items():
            pinned = self.configured.get(publisher)
            if pinned is not None and not senders_match(pinned, sender):
                _log.warning(f'Publisher {publisher!r} is bound to sender {sender!r} in the database but configured '
                             f'with sender {pinned!r}; the configured sender applies.')
        _log.info(f'Publisher bindings on: {len(self.configured)} configured, {len(learned)} learned.')

    def check(self, publisher: str, sender: str | None) -> Decision:
        """Allowed when bindings are off, when the sender is the publisher's bound one, or when the publisher has no
        binding yet (the sender is then bound to it). Otherwise refused, counted and logged with rate limiting."""
        if not self.enabled:
            return Decision(True)
        if not sender:
            return self._refuse(publisher, sender, None,
                                f'A message for publisher {publisher!r} carries no sender identity; with publisher '
                                f'bindings on it cannot be accepted.')
        self._log_identity_form(sender)
        expected = self.configured.get(publisher)
        if expected is None and self.store is not None:
            expected = self.store.bound_sender(publisher)
        if expected is None:
            if self.store is not None:
                self.store.bind_publisher(publisher, sender)
            _log.info(f'Publisher {publisher!r} is now bound to its first sender {sender!r}.')
            return Decision(True, sender)
        if senders_match(expected, sender):
            return Decision(True, expected)
        return self._refuse(publisher, sender, expected,
                            f'Sender {sender!r} may not register or archive for publisher {publisher!r}; that '
                            f'publisher is bound to sender {expected!r}.')

    def _refuse(self, publisher: str, sender: str | None, expected: str | None, message: str) -> Decision:
        self.mismatch_counts[publisher] = self.mismatch_counts.get(publisher, 0) + 1
        key = (publisher, sender or '')
        count, last_logged = self._mismatches.get(key, (0, None))
        count += 1
        now = self._clock()
        if last_logged is None or now - last_logged >= self._warning_interval:
            since = '' if last_logged is None else f' ({count} message(s) since the last warning)'
            _log.warning(f'{message} Dropped{since}; {self.mismatch_counts[publisher]} dropped for this publisher '
                         f'since start.')
            self._mismatches[key] = (0, now)
        else:
            self._mismatches[key] = (count, last_logged)
        return Decision(False, expected, message)

    def _log_identity_form(self, sender: str) -> None:
        if self._identity_form_logged:
            return
        self._identity_form_logged = True
        if '@' in sender:
            _log.info(f'The platform presents qualified sender identities ({sender!r}); bindings match exactly.')
        else:
            _log.info(f'The platform presents bare sender identities ({sender!r}, no @instance); a bare identity '
                      f'matches a qualified binding by its identity part, so the same identity on another platform '
                      f'is not distinguished until the router qualifies identities.')
