{
  description = "Swytch - a leaderless distributed cache/database";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          lib = pkgs.lib;

          swytch = pkgs.buildGoLatestModule (finalAttrs: {
            pname = "swytch";
            version = self.shortRev or "dev";

            src = self;

            vendorHash = "sha256-rjX0bB4WsFS3T6ETdCH9P08jXSVshj5dmbfmkmVM4mA=";

            __structuredAttrs = true;
            subPackages = [ "." ];

            env.CGO_ENABLED = "0";

            ldflags = [
              "-s"
              "-w"
              "-X=main.Version=${finalAttrs.version}"
            ];

            meta = {
              description = "A leaderless distributed cache/database";
              homepage = "https://github.com/swytchdb/swytch";
              license = lib.licenses.agpl3Plus;
              mainProgram = "swytch";
              platforms = systems;
            };
          });
        in
        {
          default = swytch;
          inherit swytch;
        }
      );

      apps = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          package = self.packages.${system}.swytch;

          swytchRedis = pkgs.writeShellApplication {
            name = "swytch-redis";
            runtimeInputs = [ package ];
            text = ''
              exec swytch redis --maxmemory 85% "$@"
            '';
          };

          app = {
            type = "app";
            program = "${swytchRedis}/bin/swytch-redis";
            meta.description = "Run the Swytch Redis server";
          };
        in
        {
          default = app;
          swytch = app;
        }
      );

      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = [
              pkgs.go_latest
              pkgs.just
            ];
          };
        }
      );
    };
}
