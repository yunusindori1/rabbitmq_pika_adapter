def test_import_public_api():
    # Canonical package exports
    from mq_adapters import RabbitMQClient, PublisherPool, Listener

    assert Listener is not None
    assert PublisherPool is not None
    assert RabbitMQClient is not None


def test_legacy_sender_removed():
    import mq_adapters

    assert not hasattr(mq_adapters, "Sender"), "Legacy Sender should not be part of the trimmed public API"


def test_project_root_can_be_imported_as_module():
    """This repo contains a root-level __init__.py. Ensure it imports without errors.

    Note: the distribution/project name is `rabbitmq-pika-adapter`, but the importable package is `mq_adapters`.
    """

    import importlib.util
    import pathlib

    root_init = pathlib.Path(__file__).resolve().parents[1] / "__init__.py"
    assert root_init.exists()

    spec = importlib.util.spec_from_file_location("rabbitmq_pika_adapter_root", root_init)
    assert spec is not None and spec.loader is not None

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # Validate the same exports exist at root, too.
    assert hasattr(mod, "RabbitMQClient")
    assert hasattr(mod, "PublisherPool")
    assert hasattr(mod, "Listener")
    assert hasattr(mod, "AsyncSender")
    assert hasattr(mod, "AsyncListener")
