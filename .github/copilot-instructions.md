# Copilot Instructions for LTest

LTest (Linearizability Test) is a C++ framework for writing tests for concurrent data structures.

## Project Overview

- **Language**: C++20
- **Build System**: CMake + Ninja
- **Key concepts**: linearizability, coroutines, context switches, schedulers

## Development Environment

Use the Docker container defined in `Dockerfile` for development:
```sh
./scripts/rund.sh
```

## Build & Test

```sh
cmake -G Ninja -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target lin_check_test
```
