{ pkgs, ... }:

let
  # The official binary that bazelisk gives CI for .bazelversion 9.1.0.
  # ponytail: x86_64-linux with /lib64/ld-linux-x86-64.so.2 only (factory VMs, CI); add a per-system asset map for other hosts.
  bazel = pkgs.runCommand "bazel-9.1.0" { } ''
    install -D -m 0755 ${
      pkgs.fetchurl {
        url = "https://github.com/bazelbuild/bazel/releases/download/9.1.0/bazel-9.1.0-linux-x86_64";
        hash = "sha256-pmdFTz9PiHjfgZkTa4LBmfatqEd7M3+uOx74VPAeTi8=";
      }
    } $out/bin/bazel
  '';
in
{
  packages = [
    pkgs.go_1_27
    bazel
    pkgs.gitleaks
    pkgs.govulncheck
  ];
}
