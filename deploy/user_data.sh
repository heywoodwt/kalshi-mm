#!/bin/bash
# EC2 User Data — prepare the instance to run kalshi-mm as a Docker container.
#
# We build the image ON the instance (inside rust:trixie via the Dockerfile),
# NOT with a native `cargo build`. Amazon Linux 2023 ships glibc 2.34, but the
# prebuilt ONNX Runtime that ort's download-binaries feature fetches references
# __isoc23_strtoull / __isoc23_strtol — symbols that only exist in glibc >= 2.38
# — so a native build fails at link (rust-lld: undefined symbol). The Docker
# build runs inside trixie (glibc 2.41), so the host's old glibc is irrelevant.
set -x
yum update -y
yum install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user

echo "Instance initialized for kalshi-mm (docker)"