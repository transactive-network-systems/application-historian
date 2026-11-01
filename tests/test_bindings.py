"""
Publisher bindings: off by default, configured (pinned) and learned (first-come) senders, qualified and bare
identities, the capture path (records and definitions messages), the RPC path, the rate-limited warning and persistence
across a restart.
"""
import logging

import pytest

from conftest import envelope_record, registration, standard_record
from historian.application.agent import SENDER_HEADER, ApplicationHistorian
from historian.application.bindings import PublisherBindings, parse_bindings_configuration, senders_match
from historian.application.store import PUBLISHER_SENDERS

MARKET_ID = "0190a3c0-0000-7000-8000-000000000001"


def running_historian(connection, **config):
    agent = ApplicationHistorian()
    agent.configure({"connection": connection, **config})
    agent.historian_setup()
    return agent


def market_envelope(market_id=MARKET_ID, name="day-ahead"):
    return {"table_name": "market", "data": {"id": market_id, "name": name, "market_type": "consensus",
                                             "delivery_start_time": "2026-06-09T13:00:00+00:00", "active": False}}


def records_from(sender, publisher="campus", market_id=MARKET_ID, record_id=1):
    record = envelope_record([market_envelope(market_id)], topic=f"app/archive/{publisher}/records",
                             record_id=record_id)
    record["headers"] = {SENDER_HEADER: sender} if sender is not None else {}
    return record


def definitions_from(sender, publisher="campus", request_id="req-1"):
    record = standard_record(topic=f"app/archive/{publisher}/definitions",
                             value={"publisher": publisher, "request_id": request_id, **registration()})
    record["headers"] = {SENDER_HEADER: sender}
    return record


def market_count(agent):
    return len(agent.store.data_manager.query_data("market"))


# ---------------------------------------------------------------- configuration
class TestConfiguration:

    def test_default_is_off_with_no_bindings(self):
        assert parse_bindings_configuration(False, None) == (False, {})
        agent = ApplicationHistorian()
        assert agent.bindings.enabled is False
        assert agent._default_config["bind_publishers"] is False and agent._default_config["publisher_bindings"] == {}

    def test_bindings_are_parsed(self):
        enabled, bindings = parse_bindings_configuration(True, {"campus": {"sender": "tn.campus@campus"},
                                                                "city": {"sender": "tn.city"}})
        assert enabled is True and bindings == {"campus": "tn.campus@campus", "city": "tn.city"}

    @pytest.mark.parametrize("bind, bindings", [
        ("yes", None),
        (True, ["campus"]),
        (True, {"campus": "tn.campus@campus"}),
        (True, {"campus": {"sender": "tn.campus", "extra": 1}}),
        (True, {"campus": {"sender": ""}}),
        (True, {"campus": {"sender": "@campus"}}),
        (True, {"campus": {"sender": "a@b@c"}}),
        (True, {"cam/pus": {"sender": "tn.campus"}}),
        (True, {"": {"sender": "tn.campus"}}),
    ])
    def test_bad_shapes_are_refused(self, bind, bindings):
        with pytest.raises(ValueError, match="publisher_bindings|bind_publishers"):
            parse_bindings_configuration(bind, bindings)

    def test_configure_applies_the_keys_and_refuses_bad_ones(self, connection):
        agent = running_historian(connection, bind_publishers=True,
                                  publisher_bindings={"campus": {"sender": "tn.campus@campus"}})
        assert agent.bindings.enabled and agent.bindings.configured == {"campus": "tn.campus@campus"}
        with pytest.raises(ValueError, match="bind_publishers"):
            ApplicationHistorian().configure({"connection": connection, "bind_publishers": "on"})


class TestSendersMatch:

    @pytest.mark.parametrize("expected, presented, outcome", [
        ("tn.campus@campus", "tn.campus@campus", True),
        ("tn.campus@campus", "tn.campus", True),           # bare presented matches a qualified binding by its tail
        ("tn.campus", "tn.campus@campus", True),           # bare learned binding, qualified presented
        ("tn.campus", "tn.campus", True),
        ("tn.campus@campus", "tn.campus@city", False),     # both qualified: exact only
        ("tn.campus@campus", "tn.city", False),
        ("tn.campus", "tn.campus.extra", False),
        ("tn.campus@campus", "campus", False),
    ])
    def test_rule(self, expected, presented, outcome):
        assert senders_match(expected, presented) is outcome


# ---------------------------------------------------------------- off by default
def test_off_by_default_stores_from_any_sender_and_learns_nothing(connection):
    agent = running_historian(connection)
    agent.create_tables(registration())
    agent.publish_to_historian([records_from("anyone"), records_from(None, market_id=MARKET_ID[:-1] + "2", record_id=2),
                                records_from("someone.else", market_id=MARKET_ID[:-1] + "3", record_id=3)])
    assert market_count(agent) == 3
    assert agent.store.data_manager.query_data(PUBLISHER_SENDERS) == []
    assert agent.store.bound_senders == {}


# ---------------------------------------------------------------- listed publishers
def test_listed_publisher_accepts_its_sender_and_refuses_another(connection, caplog):
    agent = running_historian(connection, bind_publishers=True,
                              publisher_bindings={"campus": {"sender": "tn.campus@campus"}})
    agent.create_tables(registration())
    with caplog.at_level(logging.WARNING):
        agent.publish_to_historian([records_from("tn.campus@campus"),
                                    records_from("tn.intruder@campus", market_id=MARKET_ID[:-1] + "2", record_id=2)])
    assert market_count(agent) == 1
    assert agent.handled_all == 1  # the refused record is dropped, not retried
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and r.name.endswith("bindings")]
    assert len(warnings) == 1
    assert "'tn.intruder@campus'" in warnings[0].message and "'campus'" in warnings[0].message
    assert "bound to sender 'tn.campus@campus'" in warnings[0].message
    assert agent.bindings.mismatch_counts == {"campus": 1}
    assert agent.store.data_manager.query_data(PUBLISHER_SENDERS) == []  # a pinned publisher is not learned


def test_qualified_binding_accepts_the_bare_tail_and_logs_the_form_once(connection, caplog):
    agent = running_historian(connection, bind_publishers=True,
                              publisher_bindings={"campus": {"sender": "tn.campus@campus"}})
    agent.create_tables(registration())
    with caplog.at_level(logging.INFO, logger="historian.application.bindings"):
        agent.publish_to_historian([records_from("tn.campus"),
                                    records_from("tn.campus", market_id=MARKET_ID[:-1] + "2", record_id=2)])
    assert market_count(agent) == 2
    form_lines = [r for r in caplog.records if "presents bare sender identities" in r.message]
    assert len(form_lines) == 1


def test_a_message_without_a_sender_is_refused_when_bindings_are_on(connection, caplog):
    agent = running_historian(connection, bind_publishers=True)
    agent.create_tables(registration())
    with caplog.at_level(logging.WARNING):
        agent.publish_to_historian([records_from(None)])
    assert market_count(agent) == 0
    assert any("carries no sender identity" in r.message for r in caplog.records)


# ---------------------------------------------------------------- learned (first-come) bindings
def test_first_sender_is_bound_and_a_second_is_refused(connection, caplog):
    agent = running_historian(connection, bind_publishers=True)
    agent.create_tables(registration())
    agent.publish_to_historian([records_from("tn.campus")])
    assert agent.store.bound_senders == {"campus": "tn.campus"}
    [row] = agent.store.data_manager.query_data(PUBLISHER_SENDERS)
    assert row["publisher"] == "campus" and row["sender"] == "tn.campus" and row["first_seen"]
    with caplog.at_level(logging.WARNING):
        agent.publish_to_historian([records_from("tn.other", market_id=MARKET_ID[:-1] + "2", record_id=2),
                                    records_from("tn.campus", market_id=MARKET_ID[:-1] + "3", record_id=3)])
    assert market_count(agent) == 2
    assert any("'tn.other'" in r.message and "bound to sender 'tn.campus'" in r.message for r in caplog.records)


def test_bare_learned_binding_accepts_the_qualified_identity_later(connection):
    agent = running_historian(connection, bind_publishers=True)
    agent.create_tables(registration())
    agent.publish_to_historian([records_from("tn.campus")])
    agent.publish_to_historian([records_from("tn.campus@campus", market_id=MARKET_ID[:-1] + "2", record_id=2)])
    assert market_count(agent) == 2


def test_learned_binding_persists_across_restart(connection):
    first = running_historian(connection, bind_publishers=True)
    first.create_tables(registration())
    first.publish_to_historian([records_from("tn.campus")])

    restarted = running_historian(connection, bind_publishers=True)
    assert restarted.store.bound_senders == {"campus": "tn.campus"}
    restarted.publish_to_historian([records_from("tn.other", market_id=MARKET_ID[:-1] + "2", record_id=2),
                                    records_from("tn.campus", market_id=MARKET_ID[:-1] + "3", record_id=3)])
    assert market_count(restarted) == 2


def test_learned_binding_is_kept_but_ignored_while_bindings_are_off(connection):
    on = running_historian(connection, bind_publishers=True)
    on.create_tables(registration())
    on.publish_to_historian([records_from("tn.campus")])
    off = running_historian(connection)
    off.publish_to_historian([records_from("tn.other", market_id=MARKET_ID[:-1] + "2", record_id=2)])
    assert market_count(off) == 2
    assert off.store.bound_senders == {"campus": "tn.campus"}


def test_configured_binding_wins_over_a_conflicting_learned_one(connection, caplog):
    learned = running_historian(connection, bind_publishers=True)
    learned.create_tables(registration())
    learned.publish_to_historian([records_from("tn.first")])
    with caplog.at_level(logging.WARNING, logger="historian.application.bindings"):
        pinned = running_historian(connection, bind_publishers=True,
                                   publisher_bindings={"campus": {"sender": "tn.pinned@campus"}})
    assert any("bound to sender 'tn.first' in the database but configured with sender 'tn.pinned@campus'" in r.message
               for r in caplog.records)
    pinned.publish_to_historian([records_from("tn.first", market_id=MARKET_ID[:-1] + "2", record_id=2),
                                 records_from("tn.pinned", market_id=MARKET_ID[:-1] + "3", record_id=3)])
    assert market_count(pinned) == 2  # the first row and the pinned sender's; tn.first is refused now


# ---------------------------------------------------------------- definitions messages and the RPC path
def test_definitions_message_from_the_wrong_sender_gets_an_error_status_and_registers_nothing(connection):
    agent = running_historian(connection, bind_publishers=True,
                              publisher_bindings={"campus": {"sender": "tn.campus@campus"}})
    agent.publish_to_historian([definitions_from("tn.intruder", request_id="req-x")])
    assert agent.store.registered_tables == set()
    [(topic, _, status)] = agent.vip.pubsub.published
    assert topic == "app/archive/campus/status"
    assert status["status"] == "error" and status["request_id"] == "req-x" and status["registered_tables"] == []
    assert "Sender 'tn.intruder' may not register" in status["message"] and "'campus'" in status["message"]
    assert "bound to sender 'tn.campus@campus'" in status["message"]
    agent.publish_to_historian([definitions_from("tn.campus", request_id="req-ok")])
    assert agent.store.registered_tables == {"component", "market", "time_interval"}
    assert agent.vip.pubsub.published[-1][2]["status"] == "success"


def test_refused_definitions_message_without_request_id_gets_no_reply(connection):
    agent = running_historian(connection, bind_publishers=True,
                              publisher_bindings={"campus": {"sender": "tn.campus@campus"}})
    record = definitions_from("tn.intruder")
    del record["value"]["request_id"]
    agent.publish_to_historian([record])
    assert agent.vip.pubsub.published == [] and agent.store.registered_tables == set()


def test_a_definitions_message_binds_an_unlisted_publisher_to_its_sender(connection):
    agent = running_historian(connection, bind_publishers=True)
    agent.publish_to_historian([definitions_from("tn.campus")])
    assert agent.store.bound_senders == {"campus": "tn.campus"}
    assert agent.vip.pubsub.published[-1][2]["status"] == "success"


def test_rpc_registration_checks_the_caller_against_the_named_publisher(connection):
    agent = running_historian(connection, bind_publishers=True,
                              publisher_bindings={"campus": {"sender": "tn.campus@campus"}})
    agent.vip.rpc.context.vip_message.user = "tn.intruder"
    reply = agent.create_tables({"publisher": "campus", **registration()})
    assert reply["status"] == "error" and reply["registered_tables"] == []
    assert "Sender 'tn.intruder' may not register" in reply["message"]
    assert agent.store.registered_tables == set()

    agent.vip.rpc.context.vip_message.user = b"tn.campus"  # the platform may hand the frame over as bytes
    reply = agent.create_tables({"publisher": "campus", **registration()})
    assert reply["status"] == "success"


def test_rpc_registration_binds_an_unlisted_publisher_to_the_caller(connection):
    agent = running_historian(connection, bind_publishers=True)
    agent.vip.rpc.context.vip_message.user = "tn.city"
    assert agent.create_tables({"publisher": "city", **registration()})["status"] == "success"
    assert agent.store.bound_senders == {"city": "tn.city"}
    agent.vip.rpc.context.vip_message.user = "tn.other"
    assert agent.create_tables({"publisher": "city", **registration()})["status"] == "error"


def test_rpc_registration_without_a_publisher_is_accepted_with_a_warning(connection, caplog):
    agent = running_historian(connection, bind_publishers=True)
    with caplog.at_level(logging.WARNING):
        reply = agent.create_tables(registration())
    assert reply["status"] == "success"
    assert any('names no "publisher"' in r.message for r in caplog.records)
    assert agent.store.bound_senders == {}


def test_rpc_registration_ignores_the_caller_when_bindings_are_off(connection):
    agent = running_historian(connection)
    agent.vip.rpc.context.vip_message.user = "anyone"
    assert agent.create_tables({"publisher": "campus", **registration()})["status"] == "success"


# ---------------------------------------------------------------- the capture callback and the warning rate
def test_capture_stamps_the_platform_sender_over_any_header_a_publisher_set():
    agent = ApplicationHistorian()
    agent._capture_record_data("pubsub", "tn.campus", "", "app/archive/campus/records",
                               {"Date": "2026-06-09T12:00:00+00:00", SENDER_HEADER: "tn.spoofed"}, [market_envelope()])
    [(topic, headers, message)] = agent.captured
    assert topic == "app/archive/campus/records"
    assert headers[SENDER_HEADER] == "tn.campus" and headers["Date"] == "2026-06-09T12:00:00+00:00"
    assert message == [market_envelope()]


def test_mismatch_warnings_are_one_line_per_minute_per_sender(caplog):
    clock = [1000.0]
    bindings = PublisherBindings(True, {"campus": "tn.campus@campus"}, clock=lambda: clock[0])
    with caplog.at_level(logging.WARNING, logger="historian.application.bindings"):
        for _ in range(5):
            assert bindings.check("campus", "tn.intruder").allowed is False
        clock[0] += 30
        bindings.check("campus", "tn.intruder")
        bindings.check("campus", "tn.second")  # another sender: its own first line
        clock[0] += 31
        bindings.check("campus", "tn.intruder")
    warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 3
    assert "tn.intruder" in warnings[0] and "since the last warning" not in warnings[0]
    assert "tn.second" in warnings[1]
    assert "(6 message(s) since the last warning)" in warnings[2] and "8 dropped for this publisher" in warnings[2]
    assert bindings.mismatch_counts == {"campus": 8}
