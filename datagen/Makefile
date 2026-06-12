# Build wrapper for the datagen binaries.
# Use `make kafka` when kafka.enabled=true in the config.

BUILD_DIR ?= build
CMAKE ?= cmake
JOBS ?= 2

.PHONY: all configure kafka clean

all: configure
	$(CMAKE) --build $(BUILD_DIR) --target datagen datagen_shared -j$(JOBS)

configure:
	$(CMAKE) -S . -B $(BUILD_DIR)

kafka:
	$(CMAKE) -S . -B $(BUILD_DIR) -DDATAGEN_REQUIRE_KAFKA=ON
	$(CMAKE) --build $(BUILD_DIR) --target datagen datagen_shared -j$(JOBS)

clean:
	$(CMAKE) --build $(BUILD_DIR) --target clean
