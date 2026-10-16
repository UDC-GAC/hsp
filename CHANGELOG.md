# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Next release: 2.0.0.

### Added
- Unit and integration tests (JUnit 4) for the line reader, the FASTQ/FASTA record readers, the single-end and paired-end input formats and splits (plain, gzip and bzip2 input), including an exhaustive split-boundary sweep and golden files pinning the output format.
- `coverage` Maven profile (JaCoCo): `mvn test -Pcoverage`.

### Changed
- Minimum requirements raised to Java 11 and Hadoop 3.3. Use HSP 1.3 (tag `v1.3`) for Java 8 / earlier Hadoop versions.
- Paired-end splits now compute their locations when child splits are added, preferring hosts common to both files (falling back to the union) to improve data locality.
- Java requirement increased from 1.6 to 1.8 (superseded by the Java 11 minimum above).
