import warnings

import pytest # type: ignore
import zenoh # type: ignore
from pydantic import SecretStr, ValidationError

from istos import IstosSecurityWarning
from istos.communication.sessions import (  # type: ignore
    ZenohSession,
    AsyncZenohSession,
)
from istos.communication.config import IstosZenohConfig  # type: ignore

def test_zenoh_session_sync_lifecycle(mocker):
    """Verifies that ZenohSession opens and closes the session correctly."""
    mock_session = mocker.Mock()
    mock_open = mocker.patch("zenoh.open", return_value=mock_session)
    
    z_session = ZenohSession()
    with z_session as session:
        assert session == mock_session
        mock_open.assert_called_once()
    
    # Verify close was called on exit
    mock_session.close.assert_called_once()

@pytest.mark.asyncio
async def test_zenoh_session_async_lifecycle(mocker):
    """Verifies that AsyncZenohSession opens and closes the session correctly in async mode."""
    mock_session = mocker.Mock()
    mock_open = mocker.patch("zenoh.open", return_value=mock_session)
    
    z_session = AsyncZenohSession()
    async with z_session as session:
        assert session == mock_session
        mock_open.assert_called_once()
            
    mock_session.close.assert_called_once()

def test_zenoh_session_info_sync(mocker):
    """Verifies get_info handles different Zenoh info implementations (method vs attribute)."""
    # CASE 1: info is a method
    mock_session = mocker.Mock()
    mock_info_obj = mocker.Mock()
    mock_info_obj.zid = "test-zid-1"
    mock_session.info.return_value = mock_info_obj
    
    mocker.patch("zenoh.open", return_value=mock_session)
    z_session = ZenohSession()
    with z_session:
        info = z_session.get_info()
        assert info["zid"] == "test-zid-1"

    # CASE 2: info is a property/attribute (simulated by a non-callable object)
    class PlainInfo:
        def __init__(self, zid):
            self.zid = zid

    mock_info_obj_2 = PlainInfo("test-zid-2")
    
    class FakeSession:
        def __init__(self, info):
            self.info = info
        def close(self):
            pass

    mock_session_2 = FakeSession(mock_info_obj_2)
    
    mocker.patch("zenoh.open", return_value=mock_session_2)
    z_session_2 = ZenohSession()
    with z_session_2:
        info = z_session_2.get_info()
        assert info["zid"] == "test-zid-2"


# ---------------------------------------------------------------------------
# IstosZenohConfig validation (now enforced at construction, not build())
# ---------------------------------------------------------------------------

def test_invalid_endpoint_rejected():
    # ValidationError is a subclass of ValueError; it now raises at construction.
    with pytest.raises(ValidationError, match="Invalid Zenoh endpoint"):
        IstosZenohConfig(connect_endpoints=["router:7447"])  # missing proto/


def test_validation_happens_at_construction_not_build():
    # The error surfaces before build() is ever called.
    with pytest.raises(ValidationError):
        IstosZenohConfig(listen_certificate="/cert.pem")  # no .build()


def test_mode_literal_rejects_invalid():
    with pytest.raises(ValidationError):
        IstosZenohConfig(mode="peeer")


def test_tls_cert_without_key_rejected():
    with pytest.raises(ValidationError, match="must be provided together"):
        IstosZenohConfig(listen_certificate="/cert.pem")


def test_mtls_without_ca_rejected():
    with pytest.raises(ValidationError, match="requires root_ca_certificate"):
        IstosZenohConfig(
            enable_mtls=True, listen_certificate="/c.pem", listen_private_key="/k.pem"
        )


# ---------------------------------------------------------------------------
# Endpoint parsing (JSON array / comma-separated / list)
# ---------------------------------------------------------------------------

def test_endpoints_from_csv_env(monkeypatch):
    monkeypatch.setenv("ISTOS_ZENOH_CONNECT_ENDPOINTS", "tls/a:7447,tls/b:7447")
    cfg = IstosZenohConfig(username="u", password="p", root_ca_certificate="/ca.pem")
    assert cfg.connect_endpoints == ["tls/a:7447", "tls/b:7447"]


def test_endpoints_from_json_env(monkeypatch):
    monkeypatch.setenv("ISTOS_ZENOH_CONNECT_ENDPOINTS", '["tls/c:7447"]')
    cfg = IstosZenohConfig(username="u", password="p", root_ca_certificate="/ca.pem")
    assert cfg.connect_endpoints == ["tls/c:7447"]


def test_endpoints_from_list_in_code():
    cfg = IstosZenohConfig(
        connect_endpoints=["tls/d:7447"], username="u", password="p", root_ca_certificate="/ca.pem"
    )
    assert cfg.connect_endpoints == ["tls/d:7447"]


def test_unknown_field_forbidden():
    with pytest.raises(ValidationError):
        IstosZenohConfig(usernam="typo")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------

def test_password_is_secret_and_masked():
    cfg = IstosZenohConfig(username="u", password="hunter2", root_ca_certificate="/ca.pem")
    assert isinstance(cfg.password, SecretStr)
    assert "hunter2" not in repr(cfg)
    assert "hunter2" not in str(cfg)


def test_private_key_is_secret():
    cfg = IstosZenohConfig(listen_certificate="/c.pem", listen_private_key="PRIV-KEY")
    assert isinstance(cfg.listen_private_key, SecretStr)
    assert "PRIV-KEY" not in repr(cfg)


# ---------------------------------------------------------------------------
# Security warnings
#
# Config-time security notices are emitted via warnings.warn(IstosSecurityWarning)
# — the idiomatic mechanism (cf. urllib3's InsecureRequestWarning) — so tests use
# pytest.warns / catch_warnings rather than a logging handler.
# ---------------------------------------------------------------------------

def test_no_auth_no_tls_warns():
    with pytest.warns(IstosSecurityWarning, match="neither authentication"):
        IstosZenohConfig(mode="peer").build()


def test_auth_without_tls_warns():
    with pytest.warns(IstosSecurityWarning, match="auth is configured without TLS"):
        IstosZenohConfig(username="u", password="p").build()


def test_auth_with_tls_is_silent():
    with warnings.catch_warnings():
        warnings.simplefilter("error", IstosSecurityWarning)
        # No IstosSecurityWarning should be raised (would become an error).
        IstosZenohConfig(
            username="u", password="p", root_ca_certificate="/ca.pem"
        ).build()


def test_security_warning_can_be_escalated_to_error():
    with warnings.catch_warnings():
        warnings.simplefilter("error", IstosSecurityWarning)
        with pytest.raises(IstosSecurityWarning):
            IstosZenohConfig(mode="peer").build()


# ---------------------------------------------------------------------------
# Scouting toggle & escape hatch
# ---------------------------------------------------------------------------

def test_multicast_scouting_disabled_builds():
    IstosZenohConfig(
        multicast_scouting=False,
        connect_endpoints=["tls/r:7447"],
        username="u", password="p", root_ca_certificate="/ca.pem",
    ).build()  # no raise


def test_additional_config_deep_merge():
    base = {"transport": {"link": {"tls": {"a": 1}}}}
    overlay = {"transport": {"link": {"tls": {"b": 2}}, "unicast": {"x": 9}}}
    IstosZenohConfig._deep_merge(base, overlay)
    assert base == {"transport": {"link": {"tls": {"a": 1, "b": 2}}, "unicast": {"x": 9}}}


# ---------------------------------------------------------------------------
# Session ownership
# ---------------------------------------------------------------------------

class _FakeOwnedSession:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_injected_session_not_closed_on_exit():
    fake = _FakeOwnedSession()
    mgr = ZenohSession(session=fake)
    assert mgr.is_active
    with mgr:
        pass
    assert fake.closed is False  # caller-owned, must not be closed


def test_manager_inactive_without_session():
    assert ZenohSession().is_active is False


@pytest.mark.asyncio
async def test_async_injected_session_not_closed():
    fake = _FakeOwnedSession()
    mgr = AsyncZenohSession(session=fake)
    async with mgr:
        pass
    assert fake.closed is False


def test_opened_session_is_closed(mocker):
    mock_session = mocker.Mock()
    mocker.patch("zenoh.open", return_value=mock_session)
    with ZenohSession():
        pass
    mock_session.close.assert_called_once()


# ---------------------------------------------------------------------------
# Istos(config=...) convenience: build the session manager from a config
# ---------------------------------------------------------------------------

def _secure_config():
    return IstosZenohConfig(
        mode="client",
        connect_endpoints=["tls/r:7447"],
        username="u", password="p", root_ca_certificate="/ca.pem",
    )


def test_istos_config_builds_session_manager():
    from istos import Istos
    app = Istos(config=_secure_config())
    assert isinstance(app._session_manager, AsyncZenohSession)
    assert app._session_manager._config is not None


def test_istos_config_accepts_raw_zenoh_config():
    from istos import Istos
    app = Istos(config=zenoh.Config())
    assert isinstance(app._session_manager, AsyncZenohSession)


def test_istos_config_and_session_manager_are_mutually_exclusive():
    from istos import Istos
    with pytest.raises(ValueError, match="not both"):
        Istos(config=_secure_config(), session_manager=AsyncZenohSession())


def test_config_selects_session_flavor():
    from istos import Istos
    a = Istos(config=IstosZenohConfig(
        session="async", username="u", password="p", root_ca_certificate="/ca.pem"))
    s = Istos(config=IstosZenohConfig(
        session="sync", username="u", password="p", root_ca_certificate="/ca.pem"))
    assert isinstance(a._session_manager, AsyncZenohSession)
    assert isinstance(s._session_manager, ZenohSession)
    assert a.config.session == "async"


def test_session_field_rejects_invalid():
    with pytest.raises(ValidationError):
        IstosZenohConfig(session="threads")


# ---------------------------------------------------------------------------
# Network decorators require the service's shared session (no more per-call
# single-run fallback).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_query_decorator_requires_active_session():
    from istos import Istos
    app = Istos()  # no run() -> no active session

    @app.query("math/add")
    def use(result):
        return result

    with pytest.raises(RuntimeError, match="No active Zenoh session"):
        await use()


@pytest.mark.asyncio
async def test_publish_decorator_requires_active_session():
    from istos import Istos
    app = Istos()

    @app.publish("drone/telemetry")
    def emit():
        return {"battery": 90}

    with pytest.raises(RuntimeError, match="No active Zenoh session"):
        await emit()
