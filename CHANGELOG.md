# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial MQTT ingest service setup
- Multi-stage Docker build for optimized images
- FastAPI application with uvicorn server
- Health check endpoint at `/live`
- Support for local library dependencies (logging-lib, kafka-consumer-producer-lib)

### Changed
- None yet

### Fixed
- None yet

### Removed
- None yet

## [0.1.0] - 2026-03-19

### Added
- Initial release of MQTT ingest service
- MQTT message ingestion capability
- Integration with Kafka for message distribution
- Docker containerization support
- Health check and metrics endpoints
- Non-root user execution for security
- Virtual environment in Docker for dependency isolation

[Unreleased]: https://github.com/IoT-Hub-Alpha/mqtt-ingest/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/IoT-Hub-Alpha/mqtt-ingest/releases/tag/v0.1.0