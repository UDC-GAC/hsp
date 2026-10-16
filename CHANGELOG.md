# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Next release: 2.0.0.

### Changed
- Minimum requirements raised to Java 11 and Hadoop 3.3. Use HSP 1.3 (tag `v1.3`) for Java 8 / earlier Hadoop versions.
- Paired-end splits now compute their locations when child splits are added, preferring hosts common to both files (falling back to the union) to improve data locality.
- Java requirement increased from 1.6 to 1.8 (superseded by the Java 11 minimum above).
