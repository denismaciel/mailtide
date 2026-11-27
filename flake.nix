{
  description = "mailtide: watertight Gmail sync engine (Rust core, Python bindings)";

  inputs = {
    # nixpkgs-unstable (not nixos-unstable): its channel gate includes Darwin.
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    # rust-overlay reads rust-toolchain.toml as is; fenix would also need the
    # toolchain's sha256 here, a second place to edit on every bump.
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    { nixpkgs, rust-overlay, ... }:
    let
      # No x86_64-darwin: nixpkgs dropped it after 26.05.
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f (
            import nixpkgs {
              inherit system;
              overlays = [ rust-overlay.overlays.default ];
            }
          )
        );
    in
    {
      devShells = forAllSystems (
        pkgs:
        let
          rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
          pinnedUv = pkgs.writeShellScriptBin "uv" ''
            exec ${pkgs.uv}/bin/uv tool run --from uv==0.12.4 uv "$@"
          '';
        in
        {
          # Native code is only bundled SQLite and aws-lc, both built with `cc`:
          # mkShell's stdenv compiler suffices (no pkg-config, cmake or OpenSSL).
          # On Darwin its default Apple SDK also brings libiconv.
          default = pkgs.mkShell {
            packages = [
              rustToolchain
              pkgs.rust-analyzer
              pkgs.python315
              pinnedUv
              pkgs.cargo-deny
              pkgs.git
            ];

            RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";
            UV_PYTHON_PREFERENCE = "only-system";
            UV_PYTHON_DOWNLOADS = "never";
          };
        }
      );
    };
}
