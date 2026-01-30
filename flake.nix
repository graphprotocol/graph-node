{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    foundry.url = "github:shazow/foundry.nix/b7adb89167832516589c899addcd25ca2a78dcfe";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
    services-flake.url = "github:juspay/services-flake";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs = inputs @ {
    flake-parts,
    process-compose-flake,
    services-flake,
    nixpkgs,
    fenix,
    foundry,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      imports = [process-compose-flake.flakeModule];
      systems = [
        "x86_64-linux" # 64-bit Intel/AMD Linux
        "aarch64-linux" # 64-bit ARM Linux
        "x86_64-darwin" # 64-bit Intel macOS
        "aarch64-darwin" # 64-bit ARM macOS
      ];

      perSystem = {
        config,
        self',
        inputs',
        pkgs,
        system,
        ...
      }: let
        overlays = [
          fenix.overlays.default
          foundry.overlay
        ];

        pkgs = import nixpkgs {
          inherit overlays system;
        };

        toolchain = with fenix.packages.${system};
          combine [
            (fromToolchainFile {
              file = ./rust-toolchain.toml;
              sha256 = "sha256-+9FmLhAOezBZCOziO0Qct1NOrfpjNsXxc/8I0c7BdKE=";
            })
            stable.rust-src # This is needed for rust-analyzer to find stdlib symbols. Should use the same channel as the toolchain.
          ];
      in {
        formatter = pkgs.alejandra;
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            toolchain
            foundry-bin
            solc
            protobuf
            uv
            cmake
            corepack
            nodejs
            postgresql
            just
            cargo-nextest
          ];
        };

        process-compose = let
          inherit (services-flake.lib) multiService;
          ipfs = multiService ./nix/ipfs.nix;
          anvil = multiService ./nix/anvil.nix;

          # Helper function to create postgres configuration with graph-specific defaults
          mkPostgresConfig = {
            name,
            port,
            user,
            password,
            database,
            dataDir,
          }: {
            enable = true;
            inherit port dataDir;
            initialScript = {
              before = ''
                CREATE ROLE postgres WITH LOGIN;
                CREATE USER \"${user}\" WITH PASSWORD '${password}' SUPERUSER;
              '';
            };
            initialDatabases = [
              {
                inherit name;
                schemas = [
                  (pkgs.writeText "init-${name}.sql" ''
                    CREATE EXTENSION IF NOT EXISTS pg_trgm;
                    CREATE EXTENSION IF NOT EXISTS btree_gist;
                    CREATE EXTENSION IF NOT EXISTS postgres_fdw;
                    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
                    GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO "${user}";
                    ALTER DATABASE "${database}" OWNER TO "${user}";
                  '')
                ];
              }
            ];
            settings = {
              shared_preload_libraries = "pg_stat_statements";
              log_statement = "all";
              default_text_search_config = "pg_catalog.english";
              max_connections = 500;
            };
          };
        in {
          # Unit tests configuration
          unit = {
            imports = [
              services-flake.processComposeModules.default
              ipfs
              anvil
            ];

            cli = {
              environment.PC_DISABLE_TUI = true;
              options = {
                port = 8881;
              };
            };

            services.postgres."postgres-unit" = mkPostgresConfig {
              name = "graph-test";
              port = 5432;
              dataDir = "./.data/unit/postgres";
              user = "graph";
              password = "graph";
              database = "graph-test";
            };

            # Set PGUSER so psql connects as the OS user (matching initdb's superuser)
            settings.processes."postgres-unit-init".environment = {
              PGUSER = builtins.getEnv "USER";
            };

            services.ipfs."ipfs-unit" = {
              enable = true;
              dataDir = "./.data/unit/ipfs";
              port = 5001;
              gateway = 8080;
            };
          };

          # Integration tests configuration
          integration = {
            imports = [
              services-flake.processComposeModules.default
              ipfs
              anvil
            ];

            cli = {
              environment.PC_DISABLE_TUI = true;
              options = {
                port = 8882;
              };
            };

            services.postgres."postgres-integration" = mkPostgresConfig {
              name = "graph-node";
              port = 3011;
              dataDir = "./.data/integration/postgres";
              user = "graph-node";
              password = "let-me-in";
              database = "graph-node";
            };

            # Set PGUSER so psql connects as the OS user (matching initdb's superuser)
            settings.processes."postgres-integration-init".environment = {
              PGUSER = builtins.getEnv "USER";
            };

            services.ipfs."ipfs-integration" = {
              enable = true;
              dataDir = "./.data/integration/ipfs";
              port = 3001;
              gateway = 3002;
            };

            services.anvil."anvil-integration" = {
              enable = true;
              package = pkgs.foundry-bin;
              port = 3021;
              timestamp = 1743944919;
              gasLimit = 100000000000;
              baseFee = 1;
              blockTime = 2;
              state = "./.data/integration/anvil/state.json";
              stateInterval = 30;
              preserveHistoricalStates = true;
            };
          };
        };
      };
    };
}
