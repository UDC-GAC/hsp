# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-10-19

### Added
- A `WARN` message (file and offset of the record) when a FASTQ file ends in the middle of a record. The incomplete record was already dropped, but silently.
- Unit and integration tests (JUnit 4) for the line reader, the FASTQ/FASTA record readers, the single-end and paired-end input formats and splits (plain, gzip and bzip2 input), including an exhaustive split-boundary sweep and golden files pinning the output format.
- `coverage` Maven profile (JaCoCo): `mvn test -Pcoverage`.
- `LineReader` gets the public methods `appendLine()`, `appendTwoLines()`, `skipLine()` and `peek()`, which the readers use to copy lines straight into the record and to skip them without copying.
- The README documents the configuration properties (`hsp.input.buffer.size`, `hsp.trim.sequence.name`), the logging and the keys of bzip2 splits.

### Fixed
- FASTQ reader failing with `IllegalArgumentException: newPosition > limit` on a file whose last line is a lone `@` without a line feed after a header. The incomplete record is now ignored with the usual warning, like any other truncated record.

- FASTA reader failing with `IOException: Incorrect position` when the header of a record crossed a refill of the input buffer, which happened with any non-trivial file (plain, gzip or bzip2). The reader no longer seeks back in the line reader: it keeps the header it has already read for the next record.

- Duplicated records when an uncompressed split was shorter than a record (e.g. FASTA with sequences longer than the split size). A record starting after the end of a split now belongs to the following split.

- `PairText.readFields()` throwing `NullPointerException` on an instance created with the no-arg constructor, which is how Hadoop deserializes Writables.

- `PairedEndInputSplit` losing its aggregate length (`getLength()` returned 0) after being deserialized; deserialized splits also return no locations instead of `null`.

- `PairedEndSequenceInputFormat.getSplits()` failing with `IllegalArgumentException` (empty path) instead of an `IOException` when the input paths or formats of a job have not been set.

- The key of the first record of an uncompressed split that starts in the middle of another record was the position where the reader started scanning, not the byte offset of the record as documented. It is now the offset of the record, so keys no longer depend on where the splits are cut. This also fixes spurious "Unexpected different keys" errors in paired-end datasets whose files differ in the length of the discarded lines.

- Paired-end input compressed with a splittable codec (bzip2) failed with `Number of file splits does not match` or `Unexpected different keys`, because the splits of each file are ranges of compressed bytes and the same range of the two files of a pair holds different reads. Each bzip2 file of a pair is now read in a single split (as gzip already was) and a `WARN` explains why and suggests decompressing the files for better performance. Single-end bzip2 input is still split.

### Changed
- **Behaviour change:** minimum requirements raised to Java 11 and Hadoop 3.3 (built against Hadoop 3.3.6, which stays a `provided` dependency). Use HSP 1.3 (tag `v1.3`) for Java 8 or earlier Hadoop versions.
- **Behaviour change:** the read name is no longer cut at its first space by default. The records keep the name exactly as it is in the file (e.g. `@ERR188245.1 HWI-ST... length=75`), and the third line of a FASTQ record is still normalized to `+`. To get the old behaviour (shorter records, less memory) set `hsp.trim.sequence.name` to `true`, or call `es.udc.gac.hadoop.sequence.parser.util.Configuration.setTrimSequenceName(conf, true)`. In HSP 1.3 the default was to trim.
- **Behaviour change:** invalid values of `hsp.input.buffer.size` and `hsp.trim.sequence.name` are no longer accepted silently. A buffer size that is not a positive integer (before: `0` made the readers return no records without any message and a negative number failed with `NegativeArraySizeException`) and a trim flag other than `true` or `false` (before: ignored, the default was used) now throw an `IllegalArgumentException` that names the property and its value, from the static setters and from `createRecordReader()`. `true` and `false` are still accepted in any case.
- The properties that `PairedEndSequenceInputFormat.setLeftInputPath()`/`setRightInputPath()` store in the configuration are now named `hsp.paired.*` instead of `hsra.paired.*` (the `hsra` prefix was a mistake). Nothing changes if you use the static methods, as you should; a job that sets the old properties by hand has to rename them. The constants of the class keep their names.
- Logging reworked. HSP now depends only on the SLF4J API (`provided`: Hadoop, Spark and Flink already ship it) and no longer bundles Log4j2 nor its `log4j2.xml`, so the logging backend, format and levels are the ones of your application or engine (logger `es.udc.gac.hadoop.sequence.parser`). The `[HSP]` lines that HSP used to print to the standard output are gone.
- Cutting the read name at the first space no longer allocates memory per record (48 B with the help of the JIT, 216 B without it, twice per pair in paired-end).
- The line reader looks for the end of each line eight bytes at a time instead of byte by byte, which was about 77 % of the CPU time spent reading a FASTQ file. The output is identical. Uncompressed FASTQ is read about 38 % faster than before this change (48 % faster than HSP 1.3), paired-end about 36 % (42 % faster than 1.3) and uncompressed FASTA about 29 %; gzip improves a few percent and bzip2 does not change, because the codec dominates the time.
- The FASTQ reader copies each record only once, straight from the input buffer into the value, instead of going through two temporary buffers and appending them to the value, and skips the comment line (`+`) without copying it. The output is identical, including for damaged files. Uncompressed FASTQ is read about 21 % faster than before this change (58 % faster than HSP 1.3) and paired-end about 23 % (56 % faster than 1.3); gzip improves slightly and bzip2 does not change.
- With the default configuration (names not trimmed), the FASTQ reader copies the header and the sequence of a record with a single operation when both lines are in the input buffer. The output is identical. Uncompressed FASTQ is read about 10 % faster and paired-end about 8 %; with trimming enabled nothing changes.
- The FASTA reader copies the lines of a sequence straight from the input buffer into the value, instead of through a temporary buffer. The output is identical. Uncompressed FASTA is read about 9-10 % faster, gzip about 2 %; bzip2 does not change.
- The record readers no longer log anything per record or per line (9 `trace` calls removed from `nextKeyValue()`: 5 in FASTQ and 4 in FASTA, one of them evaluated for every line). Even with the level disabled they built their arguments in the hot path, which cost up to 144 B of allocation per record in FASTQ and 360 B in FASTA (when the JIT does not eliminate it); it is now zero. Each split is described with two `DEBUG` messages (opened, closed) instead of an `INFO` one per split.
- Error messages of the paired-end classes now name the files and offsets involved (different keys or lengths, one file with more records, different number of splits), and `IOException`s wrapping another exception keep it as their cause.
- Build: project version 2.0.0, compiled with `maven.compiler.release` 11 against Hadoop 3.3.6 (`provided`); updated the compiler, Surefire, JaCoCo and build-helper Maven plugins.
- Paired-end splits now compute their locations when child splits are added, preferring hosts common to both files (falling back to the union) to improve data locality.

[Unreleased]: https://github.com/rreye/hsp/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/rreye/hsp/compare/v1.3...v2.0.0
