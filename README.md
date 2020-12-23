# Hadoop Sequence Parser (HSP)

**Hadoop Sequence Parser (HSP)** is a Java library that allows to parse DNA sequence reads from FASTQ/FASTA datasets stored in the Hadoop Distributed File System (HDFS).

HSP supports the processing of input datasets compressed with Gzip (i.e., .gz extension) and BZip2 (i.e., .bz2 extension) codecs. However, when considering compressed data that will be later processed by Hadoop or any other data processing engine (e.g., Spark), it is important to understand whether the underlying compression format supports splitting, as many codecs need the whole input stream to uncompress successfully. On the one hand, Gzip does not support splitting and HSP will not split the gzipped input dataset. This will work, but probably at the expense of lower performance. On the other hand, BZip2 does compression on blocks of data and later these blocks can be decompressed independent of each other (i.e.,  it supports splitting). Therefore, BZip2 is the recommended codec to use with HSP for better performance.

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
   <version>1.1</version> <!-- or latest version -->
  </dependency>
...
</dependencies>
```

For single-end datasets, HSP generates <key,value> pairs of type <*LongWritable*, *Text*>. The key is a unique self-generated identifier for each read and the value is the text-based content of the read (e.g., read name, bases and qualities for FASTQ). The *Text* object representing the sequence can be converted to a *String* object using the static method *getRead()* provided by the *SingleEndSequenceRecordReader* class.

For paired-end datasets, HSP generates <key,value> pairs of type <*LongWritable*, *PairText*>. The key is a unique self-generated identifier for each paired read and the value is a tuple containing the pair of *Text* objects that represent the paired sequence. HSP provides static methods in the *PairedEndSequenceRecordReader* class that allow obtaining "left" and "right" reads separately as *String* objects: *getLeftRead()* and *getRightRead()*, respectively.

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
Configuration config = jsc.hadoopConfiguration();

// Create RDD
JavaPairRDD<LongWritable, Text> readsRDD = jsc.newAPIHadoopFile("/path/to/file", FastQInputFormat.class, LongWritable.class, Text.class, config);
```

Creating a Spark RDD from a paired-end dataset in FASTQ format stored in "/path/to/file1" and "/path/to/file2" using Java:

```java
SparkSession sparkSession = SparkSession.builder().config(new SparkConf()).getOrCreate();
JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
Configuration config = jsc.hadoopConfiguration();

// Set left and right input paths for HSP
PairedEndSequenceInputFormat.setLeftInputPath(config, "/path/to/file1", FastQInputFormat.class);
PairedEndSequenceInputFormat.setRightInputPath(config, "/path/to/file2", FastQInputFormat.class);

// Create RDD
JavaPairRDD<LongWritable, PairText> readsRDD = jsc.newAPIHadoopFile("path/to/file1", PairedEndSequenceInputFormat.class, LongWritable.class, PairText.class, config);
```

### Flink examples

Creating a Flink DataSet from a single-end dataset in FASTQ format stored in "/path/to/file" using Java:

```java
ExecutionEnvironment flinkExecEnv = ExecutionEnvironment.getExecutionEnvironment();
Job hadoopJob = Job.getInstance();

// Set input path for HSP
SingleEndSequenceInputFormat.setInputPaths(hadoopJob, "/path/to/file");

// Create DataSet
DataSet<Tuple2<LongWritable,Text>> readsDS = flinkExecEnv.createInput(new HadoopInputFormat<LongWritable,Text>(FastQInputFormat.class, LongWritable.class, Text.class, hadoopJob));
```

Creating a Flink DataSet from a paired-end dataset in FASTQ format stored in "/path/to/file1" and "/path/to/file2" using Java:

```java
ExecutionEnvironment flinkExecEnv = ExecutionEnvironment.getExecutionEnvironment();
Job hadoopJob = Job.getInstance();
Configuration config = hadoopJob.getConfiguration();

// Set left and right input paths for HSP
PairedEndSequenceInputFormat.setLeftInputPath(config, "/path/to/file1", FastQInputFormat.class);
PairedEndSequenceInputFormat.setRightInputPath(config, "/path/to/file2", FastQInputFormat.class);

// Create DataSet
DataSet<Tuple2<LongWritable, PairText>> readsDS = flinkExecEnv.createInput(new HadoopInputFormat<LongWritable, PairText>(new PairedEndSequenceInputFormat(), LongWritable.class, PairText.class, hadoopJob));
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
