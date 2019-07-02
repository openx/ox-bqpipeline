# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- [#2 Feature Request: have named logs per job/pipeline.](https://github.com/openx/ox-bqpipeline/issues/2)
adds cron box hostname (contains team name) and job name to the stackdriver log name for simplified filtering.

## [0.0.2] - 2019-06-13
### Added
- This was a dummy [single commit](https://github.com/openx/ox-bqpipeline/commit/4e83b2b7af6df2adc8af1eff244e324dfc0a997c) 
release to fix twine issue 

## [0.0.1] - 2019-06-13
### Added
- First cut of ox-bqpipeline Including:
- CloudLoggingHandler to write pipeline logs and errors to stackdriver.
- Timestamped GCS Exports as CSV, JSON, AVRO formats.
- Ability to run queries and specify destination tables.

[Unreleased]: https://github.com/openx/ox-bqpipeline/compare/v1.0.0...HEAD
[0.0.3]: https://github.com/openx/ox-bqpipeline/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/openx/ox-bqpipeline/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/openx/ox-bqpipeline/releases/tag/v0.0.1

