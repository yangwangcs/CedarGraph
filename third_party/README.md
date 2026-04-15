# Third-Party Dependencies

This directory can hold vendored (in-tree) copies of heavy dependencies that
are difficult to install system-wide.

## Current Vendorable Dependencies

| Dependency | Purpose | Size (approx) | Why Vendor? |
|------------|---------|---------------|-------------|
| **brpc** | Baidu's RPC framework (required by braft) | ~50 MB source | Large, complex to package on all distros |
| **braft** | Production Raft consensus library | ~5 MB source | Depends on a specific brpc version |

## Quick Start

Run the setup script from the project root:

```bash
./scripts/setup_braft.sh
```

Then build with the vendor flag:

```bash
cmake -S . -B build -DCEDAR_VENDOR_BRAFT=ON
cmake --build build --parallel
```

## System Dependencies Still Required

Even with vendored sources, the following must be installed on your system:

- **CMake** 3.16+
- **OpenSSL** (libssl + headers)
- **Protobuf** 3.21+ (libprotobuf + protoc)
- **gflags**
- **leveldb**
- **gRPC** (CedarGraph itself needs this)

### macOS

```bash
brew install cmake openssl protobuf gflags leveldb grpc
```

### Ubuntu / Debian

```bash
sudo apt-get update
sudo apt-get install -y cmake libssl-dev libprotobuf-dev protobuf-compiler \
    libgflags-dev libleveldb-dev libgrpc++-dev
```

## Why Not FetchContent?

`FetchContent` (downloading at configure-time) is an alternative, but it has
drawbacks for this project:

1. **Network dependency on every fresh build** – CI machines and air-gapped
   environments may not have stable GitHub access.
2. **Extremely long configure/build times** – brpc alone can take 10–30 min.
3. **Version reproducibility** – vendoring guarantees every developer and CI
   runner uses exactly the same brpc+braft revision.

If you prefer `FetchContent`, you can easily adapt `CMakeLists.txt` to use
`FetchContent_Declare()` instead of `add_subdirectory(third_party/...)`.

## Updating Versions

Edit `scripts/setup_braft.sh` (change `BRPC_VERSION` or `BRAFT_VERSION`),
then delete the corresponding folder in `third_party/` and re-run the script.
