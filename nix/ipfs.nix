{ pkgs, lib, name, config, ... }:
{
  options = {
    package = lib.mkPackageOption pkgs "kubo" { };

    port = lib.mkOption {
      type = lib.types.port;
      default = 5001;
      description = "Port for IPFS API";
    };

    gateway = lib.mkOption {
      type = lib.types.port;
      default = 8080;
      description = "Port for IPFS gateway";
    };
  };

  config = {
    outputs.settings.processes.${name} = {
      command = ''
        export IPFS_PATH="${config.dataDir}"
        if [ ! -f "${config.dataDir}/config" ]; then
          mkdir -p "${config.dataDir}"
          ${lib.getExe config.package} init
          ${lib.getExe config.package} config Addresses.API /ip4/127.0.0.1/tcp/${toString config.port}
          ${lib.getExe config.package} config Addresses.Gateway /ip4/127.0.0.1/tcp/${toString config.gateway}
        fi
        ${lib.getExe config.package} daemon --offline
      '';
      
      environment = {
        IPFS_PATH = config.dataDir;
      };

      availability = {
        restart = "always";
      };

      readiness_probe = {
        http_get = {
          host = "localhost";
          port = config.port;
          path = "/version";
        };
        initial_delay_seconds = 5;
        period_seconds = 3;
        timeout_seconds = 10;
        success_threshold = 1;
        failure_threshold = 10;
      };
    };
  };
}
