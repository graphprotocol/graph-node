# Ethereum Configuration

In addition to [command-line arguments](../README.md#command-line-interface) and
[environment variables](./environment-variables.md), some Ethereum parameters
can also be configured via an `ethereum.toml` file.

This file has to be created in one of the following places:

```
Global:       /etc/graph-node/ethereum.toml
User (Linux): $HOME/.config/graph-node/ethereum.toml
User (macOS): $HOME/Library/Application Support/graph-node/ethereum.toml
```

When both a global and a user config file exist, their content is merged, with
user config vars overriding global config vars.

## Format of `ethereum.toml`

The config file uses the [TOML](https://toml.io/) file format. TOML files are
divided into sections, similar to `.ini` files. 

A complete example:

```toml
[rpc."http://your.ethereum.node/json-rpc/"]
http_headers = { Authorization = "Bearer foo" }

[rpc."http://another.ethereum.node/v1/"]
http_headers = { apikey = "something" }
```

## Supported Sections

At the moment, the following sections are supported:

- `rpc."<URL>"` to configure an Ethereum node or provider that was
  passed in via e.g. `--ethereum-rpc`.
  
### Section `rpc."<URL>"`

The following config values can be set under an `rpc."<URL>"` section:

- `http_headers` - a map of HTTP headers and header values. This can be used to
  pass e.g. API keys to an Ethereum node. The format of the map is:
  ```toml
  http_headers = { key1 = "value1", key2 = "value2" }
  ```
  Keys can also be quoted in case they include special characters:
  ```toml
  http_headers = { "x-some-custom-header" = "some value" }
  ```
