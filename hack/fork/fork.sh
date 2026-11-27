#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

# Tools for the stigenai patch stack. FORK.md tells how to use them.
#
#   fork.sh base          Print the upstream tag that the stack sits on.
#   fork.sh series        List the fork commits with their topic and status.
#   fork.sh check         Fail if a fork commit has a missing or bad trailer.
#   fork.sh export DIR    Write the stack to DIR as patches and a series file.
#   fork.sh rebase TAG    Replay the stack onto the upstream tag TAG.

die() { echo "fork.sh: $*" >&2; exit 1; }

base() {
  # Fork release tags (v1.0.1-stigenai.1) are on the stack, so skip them.
  git describe --tags --abbrev=0 --match 'v[0-9]*' --exclude '*-stigenai*' HEAD
}

series() {
  git log --reverse --no-merges \
    --format='%h%x09%(trailers:key=Fork-Topic,valueonly,separator=%x2C)%x09%(trailers:key=Fork-Upstream,valueonly,separator=%x2C)%x09%s' \
    "$(base)..HEAD"
}

check() {
  local bad
  bad=$(series | awk -F'\t' '$2 == "" || $3 !~ /^(candidate|fork-only|test-only)$/')
  [[ -z "$bad" ]] || die "$(printf 'these commits need Fork-Topic and Fork-Upstream trailers:\n%s' "$bad")"
}

export_stack() {
  local dir=${1:?usage: fork.sh export DIR}
  # Only replace a directory that a previous export made.
  [[ ! -e "$dir" || -f "$dir/series" ]] || die "$dir exists and has no series file"
  rm -rf "$dir"
  git format-patch --quiet --zero-commit -o "$dir" "$(base)..HEAD"
  (cd "$dir" && printf '%s\n' *.patch) | paste - <(series | cut -f2-) >"$dir/series"
  echo "base $(base): $(wc -l <"$dir/series" | tr -d ' ') patches in $dir"
}

rebase_stack() {
  local new=${1:?usage: fork.sh rebase TAG} old head
  git rev-parse -q --verify "refs/tags/$new" >/dev/null || die "no tag $new; run: git fetch upstream --tags"
  old=$(base)
  head=$(git rev-parse --short HEAD)
  git config rerere.enabled true
  git config rerere.autoupdate true
  echo "After the rebase, review the stack with:"
  echo "  git range-diff $old..$head $new..HEAD"
  git rebase --onto "$new" "$old"
}

cmd=${1:-}
shift || true
case "$cmd" in
  base) base ;;
  series) series ;;
  check) check ;;
  export) export_stack "$@" ;;
  rebase) rebase_stack "$@" ;;
  *) die "usage: fork.sh base|series|check|export DIR|rebase TAG" ;;
esac
