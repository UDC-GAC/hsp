# Hadoop Sequence Parser (HSP)

**Hadoop Sequence Parser (HSP)** is a Java library that allows to parse DNA sequence reads from FASTQ/FASTA datasets stored in the Hadoop Distributed File System (HDFS).

HSP supports the processing of input datasets compressed with Gzip (i.e., .gz extension) and BZip2 (i.e., .bz2 extension) codecs. However, when considering compressed data that will be later processed by Hadoop or any other data processing engine (e.g., Spark), it is important to understand whether the underlying compression format supports splitting, as many codecs need the whole input stream to uncompress successfully. On the one hand, Gzip does not support splitting and HSP will not split the gzipped input dataset. This will work, but probably at the expense of performance. On the other hand, BZip2 does compression on blocks of data and later these compressed blocks can be decompressed independent of each other, so it does support splitting. Therefore, BZip2 is the recommended codec to use with HSP for best performance.

## Getting Started

### Prerequisites

* Make sure you have Java Develpment Environment (JDK) version 1.6 or above

* Make sure you have a working Apache Maven distribution version 3 or above
  * See https://maven.apache.org/install.html

### Installation

In order to download, compile, build and install the HSP library in your Maven local repository (by default ~/.m2), just execute the following commands:

```
git clone https://github.com/rreye/hsp.git
cd hsp
mvn install
```

### Usage

In order to use the HSP library in your projects, add the following dependency section to your pom.xml:

```xml
<dependencies>
...
  <dependency>
   <groupId>es.udc.gac</groupId>
   <artifactId>hadoop-sequence-parser</artifactId>
   <version>1.0</version> <!-- or latest version -->
  </dependency>
...
</dependencies>
```

HSP generates <key,value> pairs of type <*LongWritable*, *Text*>. For single-end datasets, the key is a unique self-generated identifier for each read within the input split and the value is the text-based content of the read (e.g., read name, bases and qualities for FASTQ). The *Text* object representing the sequence can be converted to a *String* object using the static method *getRead()* provided by the *SingleEndSequenceRecordReader* class. For paired-end datasets, the key provides the length (in bytes) of a single read in the pair and the value is the merged content of both reads. HSP provides static methods in the *PairedEndSequenceRecordReader* class that allow obtaining "left" and "right" reads separately as *String* objects: *getLeftRead()* and *getRightRead()*, respectively.

### Hadoop examples

Setting up the input format of a Hadoop job for a single-end dataset in FASTQ format stored in "/path/to/file":

```java
Job job = Job.getInstance(new Configuration(), "Example");
Path inputFile = new Path("/path/to/file");

//Set input path and input format class for HSP
SingleEndSequenceInputFormat.addInputPath(job, inputFile);
job.setInputFormatClass(FastQInputFormat.class);        
```
Setting up the input format of a Hadoop job for a paired-end dataset in FASTQ format stored in "/path/to/file" and "/path/to/file2":

```java
Job job = Job.getInstance(new Configuration(), "Example");
Path inputFile1 = new Path("/path/to/file1");
Path inputFile2 = new Path("/path/to/file2");

//Set input format class and input paths for HSP
job.setInputFormatClass(PairedEndSequenceInputFormat.class);       
PairedEndSequenceInputFormat.setLeftInputPath(job, inputFile1, FastQInputFormat.class);
PairedEndSequenceInputFormat.setRightInputPath(job, inputFile2, FastQInputFormat.class);
```

### Spark examples

Creating a Spark RDD from a single-end dataset in FASTQ format stored in "/path/to/file" using Java:

```java
SparkSession sparkSession = SparkSession.builder().config(new SparkConf()).getOrCreate();		
JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
Configuration hadoopConfig = jsc.hadoopConfiguration();

// Create RDD
JavaPairRDD<LongWritable, Text> readsRDD = jsc.newAPIHadoopFile("/path/to/file", FastQInputFormat.class, LongWritable.class, Text.class, hadoopConfig);
```

Creating a Spark RDD from a paired-end dataset in FASTA format stored in "/path/to/file1" and "/path/to/file2" using Java:

```java
SparkSession sparkSession = SparkSession.builder().config(new SparkConf()).getOrCreate();		
JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
Configuration hadoopConfig = jsc.hadoopConfiguration();

// Set left and right input paths for HSP
PairedEndSequenceInputFormat.setLeftInputPath(config, "/path/to/file1", FastAInputFormat.class);
PairedEndSequenceInputFormat.setRightInputPath(config, "/path/to/file2", FastAInputFormat.class);

// Create RDD
JavaPairRDD<LongWritable, Text> readsRDD = jsc.newAPIHadoopFile("path/to/file1", PairedEndSequenceInputFormat.class, LongWritable.class, Text.class, hadoopConfig);
```

## Projects using HSP

* [MarDRe](http://mardre.des.udc.es)
* [HSRA](http://hsra.dec.udc.es)
* [SeQual](https://github.com/roigalegot/SeQual)
* [SMusket](https://github.com/rreye/smusket)
* [SparkEC](https://github.com/mscrocker/SparkEC)

## Authors

HSP is developed in the [Computer Architecture Group](http://gac.udc.es/english) at the [Universidade da Coruña](https://www.udc.es/en) by:

* **Roberto R. Expósito** (http://gac.udc.es/~rober)
* **Luis Lorenzo Mosquera** (https://github.com/luislorenzom)
* **Jorge González-Domínguez** (http://gac.udc.es/~jgonzalezd)

## License

This library is distributed as free software and is publicly available under the GPLv3 license (see the [LICENSE](LICENSE) file for more details)
