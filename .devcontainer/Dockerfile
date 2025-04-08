FROM ubuntu:20.04

ENV LANG C.UTF-8

ARG LIBFABRIC_REV=grassvalley
ARG DEBIAN_FRONTEND=noninteractive

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
      ninja-build \
      librdmacm-dev \
      libibverbs-dev \
      wget \
      unzip \
      ca-certificates \
      autoconf \
      automake \
      libtool \
      iproute2 \
      sudo

# CMake
RUN wget -O - https://github.com/Kitware/CMake/releases/download/v3.30.0/cmake-3.30.0-Linux-x86_64.tar.gz | tar --strip=1 -C /usr/local -xzf -

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
RUN wget -O - https://github.com/k0zmo/libfabric/archive/${LIBFABRIC_REV}.tar.gz | tar -zxf - \
 && cd libfabric-${LIBFABRIC_REV} \
 && ./autogen.sh \
 && ./configure --enable-debug --enable-verbs=dl --enable-efa=dl --enable-sockets=no --enable-usnic=no --enable-shared --disable-static --prefix=/opt/libfabric \
 && make -j$(nproc) \
 && sudo make install
