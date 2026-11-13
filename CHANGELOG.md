# Changelog

## [1.1.0](https://github.com/Faustvii/rreading-glasses/compare/v1.0.0...v1.1.0) (2026-11-13)


### Features

* **gr:** allow overriding upstream token and host via config ([#6](https://github.com/Faustvii/rreading-glasses/issues/6)) ([05feb35](https://github.com/Faustvii/rreading-glasses/commit/05feb3530eb3d551eeb354f37ef7754c549bf2f3))


### Bug Fixes

* **hardcover:** survive free-tier rate limits (upstream [#596](https://github.com/Faustvii/rreading-glasses/issues/596)) ([#15](https://github.com/Faustvii/rreading-glasses/issues/15)) ([adc0e71](https://github.com/Faustvii/rreading-glasses/commit/adc0e71dfd3b2f7ae71c0af9b04732b1424078e9))
* purge stale cache and port recommendation and logging fixes ([#16](https://github.com/Faustvii/rreading-glasses/issues/16)) ([5436760](https://github.com/Faustvii/rreading-glasses/commit/54367602c1f8dda1d0cff1d9d1c78f6082e2ce10))


### Miscellaneous Chores

* **ci:** fix version tag for release-please ([a8bc3a8](https://github.com/Faustvii/rreading-glasses/commit/a8bc3a8139703aa2d377fb34d5a51b5c06661118))


### Code Refactoring

* use standard library rand for query aliases ([#20](https://github.com/Faustvii/rreading-glasses/issues/20)) ([e9aa354](https://github.com/Faustvii/rreading-glasses/commit/e9aa3543c707dfa2d37151b89119e521248bf007))


### Tests

* bound incremental denormalization assertions ([#19](https://github.com/Faustvii/rreading-glasses/issues/19)) ([0510528](https://github.com/Faustvii/rreading-glasses/commit/0510528c75541ef56efc489eef159705f374e18e))

## 1.0.0 (2026-09-02)


### Features

* add ci container images and release-please workflow ([#1](https://github.com/Faustvii/rreading-glasses/issues/1)) ([045d676](https://github.com/Faustvii/rreading-glasses/commit/045d676b4adfc72ab319fa48e02f73d19a2a383a))


### Bug Fixes

* **gr:** accept fractional ratingsSum ([5733880](https://github.com/Faustvii/rreading-glasses/commit/573388075f6d96e4b51fc5e75a9cbe7cd6c1cfd2))
