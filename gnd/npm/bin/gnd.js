#!/usr/bin/env node

const { execFileSync } = require("child_process");
const path = require("path");

const PLATFORMS = {
  "darwin-arm64": "@graphprotocol/gnd-darwin-arm64",
  "darwin-x64": "@graphprotocol/gnd-darwin-x64",
  "linux-arm64": "@graphprotocol/gnd-linux-arm64",
  "linux-x64": "@graphprotocol/gnd-linux-x64",
  "win32-x64": "@graphprotocol/gnd-win32-x64",
};

const key = `${process.platform}-${process.arch}`;
const pkg = PLATFORMS[key];
if (!pkg) {
  console.error(`Unsupported platform: ${key}`);
  process.exit(1);
}

const bin = process.platform === "win32" ? "gnd.exe" : "gnd";
const binPath = path.join(
  require.resolve(`${pkg}/package.json`),
  "..",
  "bin",
  bin
);

try {
  execFileSync(binPath, process.argv.slice(2), { stdio: "inherit" });
} catch (e) {
  process.exit(e.status ?? 1);
}
