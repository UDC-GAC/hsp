Hadoop Sequence Parser (HSP)
----------------------------

Hadoop Sequence Parser (HSP) is a Java library that allows to parse DNA/RNA
sequence reads from FASTQ/FASTA datasets stored on the Hadoop Distributed 
File System (HDFS).

This tool is distributed as free software and is publicly available under 
the GPLv3 license at http://XXXXXXXXXXXX.


Quick User's Guide
------------------

- Prerequisites:

1) Make sure you have Java Develpment Environment (JDK) version 1.6 or above

2) Make sure you have a working Apache Maven distribution version 3 or above:

  See https://maven.apache.org/install.html


- Installation:

In order to compile, build and install the JAR distribution in your Maven
local repository, just execute the following Maven command from within the HSP
root directory:

  $ mvn install


- Usage:

In order to use HSP in your projects, add the following Maven dependency
in your pom file:

  <dependency>
    <groupId>es.udc.gac</groupId>
    <artifactId>hadoop-sequence-parser</artifactId>
    <version>1.0</version>
  </dependency>
