#!/usr/bin/env bash
# End-to-end tests of this service in a running network: ./e2e/run.sh <compose project>
#
# Called by `make test` in pouw-deploy after the test copy of the network is up
# (in Jenkins and locally). The same file is in every PoUW-ZK service repository.
#
# Contract with pouw-deploy:
#   - argument: name of the compose project of the running network;
#   - JUNIT_DIR: directory for JUnit XML results (Tests tab in Jenkins);
#   - exit code other than 0 = tests failed, staging is not deployed.
#
# Tests are e2e/test_*.py (pytest). They run in a container attached to the
# network, so services are reached by their compose name (http://indexer:8000),
# there are no ports on the host.
set -euo pipefail

project="${1:?Usage: $0 <compose project>}"
here="$(cd "$(dirname "$0")" && pwd)"
repo="$(basename "$(dirname "$here")")"

if ! compgen -G "$here/test_*.py" >/dev/null; then
    echo "$repo: no e2e tests"
    exit 0
fi

# Created here, otherwise Docker would create it as root and cleanWs() could not delete it
mkdir -p "${JUNIT_DIR:=reports}"
junit_dir="$(cd "$JUNIT_DIR" && pwd)"

# Runs as the current user, so results are not owned by root. HOME=/tmp lets pip
# install there, the e2e directory itself is read-only.
docker run --rm \
    --network "${project}_default" \
    --user "$(id -u):$(id -g)" \
    -e HOME=/tmp \
    -e E2E_PROJECT="$project" \
    -e PIP_DISABLE_PIP_VERSION_CHECK=1 \
    -e PYTHONDONTWRITEBYTECODE=1 \
    -v "$here:/e2e:ro" \
    -v "$junit_dir:/reports" \
    -w /e2e \
    python:3.12-slim \
    sh -c "pip install -q --user --no-warn-script-location -r requirements.txt &&
        python -m pytest -p no:cacheprovider -o junit_suite_name=$repo --junitxml=/reports/$repo.xml"
