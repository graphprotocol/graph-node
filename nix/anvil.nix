{
  pkgs,
  lib,
  name,
  config,
  ...
}: {
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

    gasLimit = lib.mkOption {
      type = lib.types.int;
      default = 100000000000;
      description = "Gas limit for the genesis block";
    };

    baseFee = lib.mkOption {
      type = lib.types.int;
      default = 1;
      description = "Base fee for the genesis block";
    };

    blockTime = lib.mkOption {
      type = lib.types.nullOr lib.types.int;
      default = null;
      description = "Block time in seconds. Null means instant mining.";
    };

    state = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Path to state file (loads on start, saves on exit)";
    };

    stateInterval = lib.mkOption {
      type = lib.types.nullOr lib.types.int;
      default = null;
      description = "Interval in seconds to dump state to disk. Useful when graceful shutdown isn't guaranteed.";
    };

    preserveHistoricalStates = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Preserve historical state snapshots when dumping. Enables RPC calls for older blocks after state reload.";
    };
  };

  config = {
    outputs.settings.processes.${name} = {
      command = let
        stateDir =
          if config.state != null
          then builtins.dirOf config.state
          else null;
        mkdirCmd =
          if stateDir != null
          then "mkdir -p ${stateDir} && "
          else "";
      in
        mkdirCmd
        + builtins.concatStringsSep " " (lib.filter (s: s != "") [
          "${lib.getExe' config.package "anvil"}"
          "--gas-limit ${toString config.gasLimit}"
          "--base-fee ${toString config.baseFee}"
          "--timestamp ${toString config.timestamp}"
          "--port ${toString config.port}"
          (lib.optionalString (config.blockTime != null) "--block-time ${toString config.blockTime}")
          (lib.optionalString (config.state != null) "--state ${config.state}")
          (lib.optionalString (config.stateInterval != null) "--state-interval ${toString config.stateInterval}")
          (lib.optionalString config.preserveHistoricalStates "--preserve-historical-states")
        ]);

      availability = {
        restart = "on_failure";
      };

      shutdown = {
        command = "kill -TERM $${PROCESS_PID}";
        timeout_seconds = 10;
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
