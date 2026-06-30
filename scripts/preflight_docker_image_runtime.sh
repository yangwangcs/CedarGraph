#!/bin/bash
# =============================================================================
# CedarGraph Docker Image Runtime Preflight
# =============================================================================
# Verifies that the pinned CedarGraph runtime image exists locally and contains
# runnable core binaries with all shared libraries resolved. It does not start a
# CedarGraph service or touch host data directories.
# =============================================================================

set -euo pipefail

IMAGE="${CEDAR_DOCKER_IMAGE:-cedargraph/cedar:k8s-fix-20260630}"

require_command() {
    local cmd="$1"
    command -v "${cmd}" >/dev/null 2>&1 || {
        echo "Required command not found: ${cmd}" >&2
        exit 1
    }
}

require_command docker

docker image inspect "${IMAGE}" >/dev/null

image_user="$(docker image inspect "${IMAGE}" --format '{{.Config.User}}')"
if [ "${image_user}" != "cedar" ]; then
    echo "Docker image must run as the non-root cedar user, got: ${image_user:-<empty>}" >&2
    exit 1
fi

entrypoint="$(docker image inspect "${IMAGE}" --format '{{json .Config.Entrypoint}}')"
if [ "${entrypoint}" != '["/usr/local/bin/docker-entrypoint.sh"]' ]; then
    echo "Docker image must use the role-aware docker-entrypoint.sh, got: ${entrypoint}" >&2
    exit 1
fi

healthcheck="$(docker image inspect "${IMAGE}" --format '{{json .Config.Healthcheck.Test}}')"
case "${healthcheck}" in
    *10559*9779*9669*) ;;
    *)
        echo "Docker image healthcheck must cover MetaD, StorageD, and GraphD runtime ports, got: ${healthcheck}" >&2
        exit 1
        ;;
esac

docker run --rm --entrypoint sh "${IMAGE}" -lc '
set -eu
test "$(id -u)" != "0"
test -x /usr/local/bin/docker-entrypoint.sh
sh -n /usr/local/bin/docker-entrypoint.sh
command -v cedar-cli >/dev/null
command -v cedar-admin >/dev/null
test -d /data
test -d /logs
for binary in cedar-metad cedar-storaged cedar-graphd; do
    path="/usr/local/bin/${binary}"
    test -x "${path}"
    "${path}" --help >/tmp/${binary}.help 2>&1 || status=$?
    status="${status:-0}"
    case "${status}" in
        0|1) ;;
        *) echo "${binary} --help exited with unexpected status ${status}" >&2; exit "${status}" ;;
    esac
    test -s "/tmp/${binary}.help"
    unset status
done

if ldd /usr/local/bin/cedar-metad /usr/local/bin/cedar-storaged /usr/local/bin/cedar-graphd | grep "not found"; then
    echo "Docker image has unresolved shared-library dependencies" >&2
    exit 1
fi
'

echo "Docker image runtime preflight passed for ${IMAGE}"
