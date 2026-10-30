# e2e tests need a running network, run.sh sets E2E_PROJECT. Without it, e.g.
# plain `pytest` for unit tests in ci-ok, this directory is skipped.
# The same file is in every PoUW-ZK service repository.
import os
import socket

import pytest

if not os.environ.get("E2E_PROJECT"):
    collect_ignore_glob = ["*"]


@pytest.fixture
def service():
    """service(name, port) -> "http://name:port", the base URL of a compose service.

    A test of a service that does not run in this network (its profile in
    pouw-deploy is off, e.g. in Jenkins before the image is in GHCR) is skipped.
    """

    def base_url(name, port):
        try:
            socket.gethostbyname(name)
        except OSError:
            pytest.skip(f"{name} does not run in this network")
        return f"http://{name}:{port}"

    return base_url
