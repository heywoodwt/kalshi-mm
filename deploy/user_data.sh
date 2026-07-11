#!/bin/bash
# EC2 User Data Script - initialize instance for the kalshi-mm bot.
# Builds happen ON the instance (not cross-compiled) so the ONNX Runtime
# binary ort's download-binaries feature fetches matches this OS/arch.

yum update -y
yum install -y gcc git

# Install as ec2-user (not root) so `source ~/.cargo/env` in
# start_live_trading.sh finds the toolchain.
sudo -u ec2-user bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'

echo "Instance initialized for kalshi-mm"