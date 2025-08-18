{ pkgs, lib, name, config, ... }:
{
  options = {
    package = lib.mkOption {
      type = lib.types.package;
      description = "Foundry package containing anvil";
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 8545;
      description = "Port for Anvil RPC server";
    };

    timestamp = lib.mkOption {
      type = lib.types.int;
      default = 1743944919;
      description = "Timestamp for the genesis block";
    };
  };

  config = {
    outputs.settings.processes.${name} = {
      command = "${lib.getExe' config.package "anvil"} --disable-block-gas-limit --disable-code-size-limit --base-fee 1 --block-time 2 --timestamp ${toString config.timestamp} --port ${toString config.port}";

      availability = {
        restart = "always";
      };

      readiness_probe = {
        exec = {
          command = "nc -z localhost ${toString config.port}";
        };
        initial_delay_seconds = 3;
        period_seconds = 2;
        timeout_seconds = 5;
        success_threshold = 1;
        failure_threshold = 10;
      };
    };
  };
}
