#!/usr/bin/env bash
# Checks that the devenv gives the CI tool versions (ax-yj5).
# Run it at the repo root, in the environment of devenv print-dev-env.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1
export GOTOOLCHAIN=local
fail=0
bad() { echo "::error::$1: got ${2:-<missing>}, want $3"; fail=1; }

want_go="go$(awk '$1=="go"{print $2}' go.mod)"
want_bazel="bazel $(tr -d '[:space:]' < .bazelversion)"

go_path=$(readlink -f "$(command -v go)" 2>/dev/null)
echo "go path: ${go_path:-<missing>}"
case "$go_path" in /nix/store/*) ;; *) bad 'go path' "$go_path" '/nix/store/...' ;; esac

got=$(go env GOVERSION 2>&1)
echo "go: $got"
[ "$got" = "$want_go" ] || bad go "$got" "$want_go"

got=$(readlink -f "$(command -v gofmt)" 2>/dev/null)
want="$(readlink -f "$(go env GOROOT 2>/dev/null)" 2>/dev/null)/bin/gofmt"
echo "gofmt path: ${got:-<missing>}"
[ "$got" = "$want" ] || bad 'gofmt path' "$got" "$want"

in_store() {
  p=$(readlink -f "$(command -v "$1")" 2>/dev/null)
  echo "$1 path: ${p:-<missing>}"
  case "$p" in /nix/store/*) ;; *) bad "$1 path" "$p" '/nix/store/...' ;; esac
}

in_store bazel
got=$(bazel --version 2>&1)
echo "bazel: $got"
[ "$got" = "$want_bazel" ] || bad bazel "$got" "$want_bazel"

in_store gitleaks
got=$(gitleaks version 2>&1) || bad gitleaks "$got" 'exit 0'
echo "gitleaks: $got"

in_store govulncheck
got=$(govulncheck -version 2>&1) || bad govulncheck "$got" 'exit 0'
echo "govulncheck: $got"

exit "$fail"
