# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Next release: 2.0.0.

### Added
- Unit and integration tests (JUnit 4) for the line reader, the FASTQ/FASTA record readers, the single-end and paired-end input formats and splits (plain, gzip and bzip2 input), including an exhaustive split-boundary sweep and golden files pinning the output format.
- `coverage` Maven profile (JaCoCo): `mvn test -Pcoverage`.

### Fixed
- FASTA reader failing with `IOException: Incorrect position` when the header of a record crossed a refill of the input buffer, which happened with any non-trivial file (plain, gzip or bzip2). The reader no longer seeks back in the line reader: it keeps the header it has already read for the next record.

- Duplicated records when an uncompressed split was shorter than a record (e.g. FASTA with sequences longer than the split size). A record starting after the end of a split now belongs to the following split.

- `PairText.readFields()` throwing `NullPointerException` on an instance created with the no-arg constructor, which is how Hadoop deserializes Writables.

### Changed
- Minimum requirements raised to Java 11 and Hadoop 3.3. Use HSP 1.3 (tag `v1.3`) for Java 8 / earlier Hadoop versions.
- Paired-end splits now compute their locations when child splits are added, preferring hosts common to both files (falling back to the union) to improve data locality.
- Java requirement increased from 1.6 to 1.8 (superseded by the Java 11 minimum above).
