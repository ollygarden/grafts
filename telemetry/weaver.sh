#!/usr/bin/env bash
# Validate a component's semantic-convention registry, regenerate its Go
# bindings, or diff it against a baseline.
#
# Every component in this repository shares this script. A component owns its
# telemetry/registry/*.yaml and component.yaml and nothing else -- the Weaver
# version, the upstream semconv pin, container flags, mounts, template wiring,
# policies and gofmt all live here, so bumping any of them is one change rather
# than a fan-out across every receiver.
#
#   ./telemetry/weaver.sh check    ./receiver/pgbouncerreceiver/telemetry/registry
#   ./telemetry/weaver.sh generate ./receiver/pgbouncerreceiver/telemetry/registry \
#                                  ./receiver/pgbouncerreceiver/internal/telemetry
#   ./telemetry/weaver.sh resolve  ./receiver/pgbouncerreceiver/telemetry/registry
#   ./telemetry/weaver.sh diff     <baseline-registry> <registry-dir>
set -euo pipefail

# Pinned: schema validation behaviour changes between Weaver releases.
readonly WEAVER_VERSION=v0.25.1

# The upstream semantic conventions every component registry depends on, as
# `registry_path: /semconv/`. Fetched rather than vendored: the version is
# pinned in one place, so a semconv bump stays a one-line diff instead of
# several megabytes of YAML. The commit is pinned alongside the tag because a
# tag can be moved and a commit cannot.
readonly SEMCONV_VERSION=v1.44.0
readonly SEMCONV_COMMIT=e10a930844c6951757a43b849d364f7d056ac32b

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repo_root
readonly semconv_cache="${repo_root}/telemetry/.semconv/${SEMCONV_VERSION}"

abs() { (cd "$1" && pwd); }

# Shallow-clone the pinned semconv model, then verify the tag resolved to the
# commit we pinned. A moved tag fails here rather than silently changing what
# every registry validates against.
fetch_semconv() {
	if [[ ! -d "${semconv_cache}/.git" ]]; then
		rm -rf "${semconv_cache}"
		mkdir -p "$(dirname "${semconv_cache}")"
		git clone --quiet --depth 1 --branch "${SEMCONV_VERSION}" \
			https://github.com/open-telemetry/semantic-conventions.git "${semconv_cache}"
	fi

	local actual
	actual="$(git -C "${semconv_cache}" rev-parse HEAD)"
	if [[ "${actual}" != "${SEMCONV_COMMIT}" ]]; then
		echo "$0: semconv ${SEMCONV_VERSION} is ${actual}, expected ${SEMCONV_COMMIT}" >&2
		echo "$0: the tag moved, or the cache is stale -- remove ${semconv_cache}" >&2
		exit 1
	fi
}

# Mount only what each mode needs, read-only wherever the container has no
# business writing. label=disable rather than the usual :z, because :z relabels
# the mounted paths on the host and leaves them that way -- a checkout should
# not come back altered from a read-only validation. It is a no-op off SELinux.
weaver() {
	local registry="$1"
	shift
	docker run --rm --user "$(id -u):$(id -g)" --security-opt label=disable \
		-v "${repo_root}/telemetry:/telemetry:ro" \
		-v "${semconv_cache}/model:/semconv:ro" \
		-v "${registry}:/registry:ro" \
		"$@"
}

usage() {
	echo "usage: $0 {check|generate|resolve|diff} <registry-dir> [output-dir|baseline-registry]" >&2
	exit 2
}

case "${1:-}" in
check)
	registry="$(abs "${2:?$(usage)}")"
	fetch_semconv
	# Weaver only warns when a dependency resolves to a version other than the
	# one the manifest asks for, and still exits 0. A component pinned to a
	# semconv version this script no longer fetches would drift silently, so
	# make that mismatch fail here.
	set +e
	output="$(weaver "${registry}" \
		"otel/weaver:${WEAVER_VERSION}" \
		registry check --v2 \
		--registry /registry/ \
		--policy /telemetry/policies/ 2>&1)"
	status=$?
	set -e
	# Weaver warns "File format definition/2 is not yet stable" once per
	# definition file. Upstream semconv is ~200 of them, which buries every
	# real diagnostic. Expected as of v0.25.1, per the Weaver adoption record.
	#
	# A diagnostic is a marker line followed by `│` continuations, so drop the
	# whole block rather than matching line by line -- filtering continuations
	# outright would truncate every message that wraps.
	printf '%s\n' "${output}" |
		awk '
			/^[[:space:]]*[⚠×✔ℹ]/ { drop = /is not yet stable/ }
			/^[[:space:]]*│/       { if (drop) next }
			!/^[[:space:]]*│/ && !/^[[:space:]]*[⚠×✔ℹ]/ { drop = 0 }
			!drop
		' | cat -s
	if [[ ${status} -eq 0 ]] && grep -q 'differs from requested' <<<"${output}"; then
		echo "$0: dependency version mismatch above; update the dependency schema_url" >&2
		exit 1
	fi
	exit "${status}"
	;;
generate)
	registry="$(abs "${2:?$(usage)}")"
	output="${3:?$(usage)}"
	fetch_semconv
	mkdir -p "${output}"
	# The migration table names the exporter being replaced. That is a
	# per-component fact, and component.yaml is where per-component facts live,
	# so read it here rather than letting it into the shared template.
	upstream="$(sed -n 's|^  repository: .*/\([^/]*\)$|\1|p' "${registry}/../component.yaml" | head -1)"
	weaver "${registry}" -v "$(abs "${output}"):/out" \
		"otel/weaver:${WEAVER_VERSION}" \
		registry generate --v2 \
		--registry /registry/ \
		--templates /telemetry/templates/ \
		--policy /telemetry/policies/ \
		-D "package_name=${4:-telemetry}" \
		-D "promcompat_package=go.olly.garden/grafts/internal/promcompat" \
		-D "upstream_exporter=${upstream:-the upstream exporter}" \
		go \
		/out
	# Jinja whitespace leaves blank lines that would fail CI's diff gate.
	gofmt -w "${output}"
	;;
resolve)
	# The resolved registry as JSON, for checks Rego cannot make.
	#
	# A Weaver policy only ever sees the registry, so anything comparing it to
	# component.yaml -- the stability disclosure, above all -- has to happen out
	# here with both files in hand.
	registry="$(abs "${2:?$(usage)}")"
	fetch_semconv
	weaver "${registry}" \
		"otel/weaver:${WEAVER_VERSION}" \
		registry resolve --v2 \
		--registry /registry/ \
		--format json 2>/dev/null
	;;
diff)
	baseline="$(abs "${2:?$(usage)}")"
	registry="$(abs "${3:?$(usage)}")"
	fetch_semconv
	weaver "${registry}" -v "${baseline}:/baseline:ro" \
		"otel/weaver:${WEAVER_VERSION}" \
		registry diff --v2 \
		--baseline-registry /baseline/ \
		--registry /registry/ \
		--format markdown
	;;
*)
	usage
	;;
esac
