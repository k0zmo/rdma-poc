FROM ubuntu:20.04

ENV LANG C.UTF-8

ARG DEBIAN_FRONTEND=noninteractive

ENV TZ=Etc/UTC
ENV LANG C.UTF-8

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      apt-transport-https \
      software-properties-common \
      openssh-client \
      pkg-config \
      gdb \
      file \
      less \
      vim \
      git \
      build-essential \
      g++ \
      librdmacm-dev \
      libibverbs-dev \
      wget \
      unzip \
      ca-certificates \
      autoconf \
      automake \
      libtool \
      iproute2 \
      sudo \
 && rm -rf /var/lib/apt/lists/*

# Install CMake
ARG CMAKE_VERSION=3.30.0
RUN wget -O - https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz \
  | tar --strip-components=1 --directory /usr/local -xzf -

# Install Ninja
ARG NINJA_VERSION=1.11.1
RUN wget https://github.com/ninja-build/ninja/releases/download/v${NINJA_VERSION}/ninja-linux.zip \
 && unzip ninja-linux.zip -d /usr/local/bin \
 && rm ninja-linux.zip

# Install fish
RUN apt-add-repository ppa:fish-shell/release-3 \
 && apt-get update \
 && apt-get install -y --no-install-recommends fish

# Create the user
ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
 && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME -s /usr/bin/fish \
 && apt-get install -y sudo \
 && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
 && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME

WORKDIR /home/$USERNAME

# Libfabric
# 2.3.1
ARG LIBFABRC_VERSION=413a2d315ee8485488fb4fdcc0012e029f570b6b
RUN wget -O - https://github.com/ofiwg/libfabric/archive/${LIBFABRC_VERSION}.tar.gz | tar -zxf - \
 && cd libfabric-${LIBFABRC_VERSION} \
 && ./autogen.sh \
 && ./configure --enable-debug --enable-verbs=dl --enable-efa=dl --enable-sockets=no --enable-usnic=no --enable-shared --disable-static --prefix=/opt/libfabric \
 && make -j$(nproc) \
 && sudo make install
