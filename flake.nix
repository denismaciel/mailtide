{
  description = "mailtide development shell";

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
      devShells = forAllSystems (pkgs: {
        # Native code is only bundled SQLite and aws-lc, both built with `cc`:
        # mkShell's stdenv compiler suffices (no pkg-config, cmake or OpenSSL).
        # On Darwin its default Apple SDK also brings libiconv.
        default = pkgs.mkShell {
          packages = [
            (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
            pkgs.uv
            pkgs.cargo-deny
            pkgs.git
          ];
        };
      });
    };
}
