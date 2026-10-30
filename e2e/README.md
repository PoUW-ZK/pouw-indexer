# e2e

End-to-end tests against a running PoUW network. Unit tests stay in `tests/`
and run in GitHub Actions (`ci-ok`). These run in Jenkins: every evening on
`main` before the deploy to staging, and on every PR in `pouw-deploy`.

Add a `test_*.py` file here, nothing else is needed:

```python
import requests


def test_health(service):
    r = requests.get(service("indexer", 8000) + "/health", timeout=10)
    assert r.status_code == 200
```

`service(name, port)` (from `conftest.py`) gives the base URL and skips the test
when that service does not run in the network (its profile in pouw-deploy is off).

For a language other than Python, agree it with @Faustynn and replace the body of `run.sh`.

- Services are reached by their name in `compose.yml` of `pouw-deploy`, there
  are no ports on the host.
- Extra libraries (e.g. `zeep` for SOAP) go into `requirements.txt`.
- Tests that are not pytest: replace the body of `run.sh` and keep its
  contract (argument, `JUNIT_DIR`, exit code).

Run locally from `pouw-deploy` next to this clone: every service is built from
your working copies (uncommitted changes too), the tests are taken from them.

```bash
make dev-test
make test-down
```

How to write unit, smoke, integration and e2e tests: the README of pouw-deploy,
section Tests.

More in the docs: CI/CD, section E2E tests of services.
