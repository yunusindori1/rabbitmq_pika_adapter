def test_import_public_api():
    # Canonical package exports
    from mq_adapters import RabbitMQClient, PublisherPool, Listener

    assert Listener is not None
    assert PublisherPool is not None
    assert RabbitMQClient is not None


def test_legacy_sender_removed():
    import mq_adapters

    assert not hasattr(mq_adapters, "Sender"), "Legacy Sender should not be part of the trimmed public API"


def test_src_layout_no_root_package():
    """Repo uses a src/ layout.

    Historically this repo contained a root-level __init__.py. We intentionally removed it to
    avoid confusing packaging/discovery.

    The importable library package is `mq_adapters`.
    """

    import pathlib

    root_init = pathlib.Path(__file__).resolve().parents[1] / "__init__.py"
    assert not root_init.exists()

    import mq_adapters

    assert hasattr(mq_adapters, "RabbitMQClient")
    assert hasattr(mq_adapters, "PublisherPool")
    assert hasattr(mq_adapters, "Listener")
