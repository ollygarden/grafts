#!/usr/bin/env bash
# Prove each Rego policy fires.
#
# A policy nobody has seen fail is a policy nobody knows works -- and these
# exist precisely because `weaver registry check` passes the cases they catch.
# Every directory under testdata/ is a registry that violates the policy it is
# named after; this asserts the violation is reported, by id.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly here
readonly weaver="${here}/../weaver.sh"

failures=0

for fixture in "${here}"/testdata/*/; do
	policy="$(basename "${fixture}")"

	if output="$("${weaver}" check "${fixture}" 2>&1)"; then
		echo "FAIL ${policy}: check passed, expected a violation"
		failures=$((failures + 1))
		continue
	fi

	if ! grep -q "Violation: .*${policy}" <<<"${output}"; then
		echo "FAIL ${policy}: check failed, but not with a ${policy} violation"
		printf '%s\n' "${output}" | sed 's/^/     /'
		failures=$((failures + 1))
		continue
	fi

	echo "ok   ${policy}"
done

if [[ ${failures} -gt 0 ]]; then
	echo "${failures} policy fixture(s) did not behave as expected" >&2
	exit 1
fi
