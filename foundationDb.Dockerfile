# syntax=docker/dockerfile:1
# Dockerfile
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# bastardized version of this - https://github.com/apple/foundationdb/blob/main/packaging/docker/Dockerfile
# to support just arm container for local tests
FROM docker.io/rockylinux/rockylinux:9.4-minimal AS base
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
RUN echo foo

RUN echo "${TARGETPLATFORM}-${TARGETOS}-${TARGETARCH}-${TARGETVARIANT}"

RUN microdnf install -y \
    bind-utils \
    binutils \
    curl \
    gdb \
    hostname \
    jq \
    less \
    libubsan \
    lsof \
    net-tools \
    nmap-ncat \
    perf \
    perl \
    procps-ng \
    strace \
    sysstat \
    tar \
    tcpdump \
    telnet \
    traceroute \
    unzip \
    openssl \
    gpg \
    vim-enhanced && \
    microdnf clean all

WORKDIR /tmp

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TARGETARCH} tini
RUN curl -Ls https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TARGETARCH} -o tini  && \
    curl -Ls https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TARGETARCH}.asc -o tini.asc && \
    gpg --batch --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 595E85A6B1B4779EA4DAAEC70B588DFF0527A9B7 && \
    gpg --batch --verify tini.asc tini && \
    chmod +x tini && \
    mv tini /usr/bin/ && \
    rm -rf /tmp/*

WORKDIR /

FROM base AS foundationdb-base

WORKDIR /tmp
ARG FDB_VERSION=7.3.59
ARG FDB_LIBRARY_VERSIONS="${FDB_VERSION}"
ARG FDB_WEBSITE=https://github.com/apple/foundationdb/releases/download

COPY <<EOF get_arch_suffix.sh
#!/bin/bash
case "${TARGETARCH}" in
  "amd64")
    echo -n x86_64;;
  "arm64")
    echo -n aarch64;;
  *)
    echo "Architecture ${TARGETARCH} does not have pre-built binaries" &>2;
    exit 1
esac
EOF

RUN chmod +x get_arch_suffix.sh

RUN ls -lah . && pwd && echo "Selected suffix - `/tmp/get_arch_suffix.sh`"

RUN mkdir -p /var/fdb/{logs,tmp,lib} && \
    mkdir -p /usr/lib/fdb/multiversion && \
    mkdir -p /tmp/website && \
    echo ${FDB_VERSION} > /var/fdb/version

# Set up a non-root user
RUN groupadd --gid 4059 \
             fdb && \
    useradd --gid 4059 \
            --uid 4059 \
            --no-create-home \
            --shell /bin/bash \
            fdb && \
    chown -R fdb:fdb /var/fdb

# no website, we just created empty dir
# COPY website /tmp/website/

# Install FoundationDB Binaries
RUN ARCH_SUFFIX=`/tmp/get_arch_suffix.sh` && \
    for file in fdbserver fdbbackup fdbcli fdbmonitor; do \
        curl --fail -Ls ${FDB_WEBSITE}/${FDB_VERSION}/$file.${ARCH_SUFFIX} -o $file; \
        chmod +x $file; \
        mv $file /usr/bin; \
    done

# Setup all symlinks for the other binaries that are a copy of fdbbackup
RUN for file in fdbdr fdbrestore backup_agent dr_agent fastrestore_tool; do \
        ln -s /usr/bin/fdbbackup /usr/bin/$file; \
    done

# Install additional FoundationDB Client Libraries
RUN ARCH_SUFFIX=`/tmp/get_arch_suffix.sh` && \
    for version in $FDB_LIBRARY_VERSIONS; do \
    curl --fail -Ls ${FDB_WEBSITE}/${version}/libfdb_c.${ARCH_SUFFIX}.so -o /usr/lib/fdb/multiversion/libfdb_c_${version%.*}.so; \
    done

# Install additional FoundationDB Client Libraries (for sidecar)
RUN ARCH_SUFFIX=`/tmp/get_arch_suffix.sh` &&  \
    mkdir -p /var/fdb/lib && \
    for version in $FDB_LIBRARY_VERSIONS; do \
    curl --fail -Ls ${FDB_WEBSITE}/${version}/libfdb_c.${ARCH_SUFFIX}.so -o /var/fdb/lib/libfdb_c_${version%.*}.so; \
    done

RUN ARCH_SUFFIX=`/tmp/get_arch_suffix.sh` &&  \
    curl -Ls $FDB_WEBSITE/$FDB_VERSION/libfdb_c.${ARCH_SUFFIX}.so -o /usr/lib/libfdb_c.so

RUN rm -rf /tmp/*
WORKDIR /

#unfortunately, FDB_VERSION can not be defined for this FROM scope to be visible, so it's hardcoded
FROM --platform=amd64 foundationdb/foundationdb:7.3.59 AS scripts

RUN ls -lah /var/fdb/scripts/

FROM foundationdb-base AS foundationdb

WORKDIR /tmp
RUN curl -LsO https://raw.githubusercontent.com/brendangregg/FlameGraph/90533539b75400297092f973163b8a7b067c66d3/stackcollapse-perf.pl && \
    curl -LsO https://raw.githubusercontent.com/brendangregg/FlameGraph/90533539b75400297092f973163b8a7b067c66d3/flamegraph.pl && \
    echo "a682ac46497d6fdbf9904d1e405d3aea3ad255fcb156f6b2b1a541324628dfc0  flamegraph.pl" > flamegraph-sha.txt && \
    echo "5bcfb73ff2c2ab7bf2ad2b851125064780b58c51cc602335ec0001bec92679a5  stackcollapse-perf.pl" >> flamegraph-sha.txt && \
    sha256sum --quiet -c flamegraph-sha.txt && \
    chmod +x stackcollapse-perf.pl flamegraph.pl && \
    mv stackcollapse-perf.pl flamegraph.pl /usr/bin && \
    rm -rf /tmp/*
WORKDIR /
# Set Up Runtime Scripts and Directories
COPY --from=scripts ["/var/fdb/scripts/fdb.bash", "/var/fdb/scripts/fdb_single.bash", "/var/fdb/scripts/" ]
VOLUME /var/fdb/data
ENV FDB_PORT 4500
ENV FDB_CLUSTER_FILE /var/fdb/fdb.cluster
ENV FDB_NETWORKING_MODE container
ENV FDB_COORDINATOR ""
ENV FDB_COORDINATOR_PORT 4500
ENV FDB_CLUSTER_FILE_CONTENTS ""
ENV FDB_PROCESS_CLASS unset
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/var/fdb/scripts/fdb.bash"]