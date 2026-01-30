# Changelog

All notable changes to this project are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project follows semantic versioning as closely as possible while in 0.x.

---

## [0.2.12] - 2026-01-30

### Fixed
  - Prevented errors when encountering objects that cannot be deep-copied by falling back to shallow references.
  - Historical inputs are now captured before node execution, ensuring history reflects the true inputs even if a node modifies them.

## [0.2.1] - 2026-01-29

### Fixed
 - Corrected logging during registry release
 - Fixed incorrect registry placement pop if persistent cahce is used

## [0.2.0] - 2026-01-29

### Breaking / Behavioral changes

- Introduced an internal object registry to manage intermediate results,
  runtime-only objects, runtime constants, and cache state.
- Persistent cache is now restored automatically at pipeline startup when
  `use_persistent_cache=True`.
- Pipeline execution fails fast if a name collision is detected between
  runtime constants and persistent cache entries.
- Intermediate in-memory objects are released deterministically once they
  are no longer needed, which may change memory behavior compared to 0.1.x.
- Fixed history tracking so that recorded inputs and outputs are stored as
  deep-copied snapshots and are no longer mutated during cleanup.

### Notes

- Existing persistent cache files may need to be cleared if pipelines relied
  on implicit overwriting or reuse of cached values.
- These changes are intentional to solidify core semantics while the project
  is still in Alpha.

---

## [0.1.1] - 2026-01-26

### Fixed
- Fix broken caching of non-pickable objects.

---

## [0.1.0] - 2026-01-26

### Added
- Initial public release of `linepipe`.
- Support for linear pipelines composed of nodes with named inputs and outputs.
- Optional persistent caching and execution history tracking.
- Initial memory profiling support for nodes (optional dependency).
