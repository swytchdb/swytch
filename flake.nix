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

          swytch = pkgs.buildGo127Module (finalAttrs: {
            pname = "swytch";
            version = "1.6.1";

            src = pkgs.fetchFromGitHub {
              owner = "swytchdb";
              repo = "swytch";
              tag = "v${finalAttrs.version}";
              hash = "sha256-riJQAreafllverdvCbvitMKWwwL1G+724tSKjAUnXoI=";
            };

            vendorHash = "sha256-VrP5VR/Ik3hjPh3D8hACQNWhVfNUjMZsJHswcUUBXNg=";

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
