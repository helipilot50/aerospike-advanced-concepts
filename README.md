# Aerospike Advanced Concepts
[Aerospike](www.aerospike.com) is a high velocity and highly scalable NoSQL database of the key-value genre. The simple key value operations arequite straigh forward and several examples are available for each programming language [here](http://www.aerospike.com/docs/client). The is also a comprehensive [developer training course](http://www.aerospike.com/training/development/aerospike-for-developers.html) profided by Aerospike.

This repositoy contains courseware and exercises that cover advanced concepts not covered directly in the Aerospike training curriculum. These are:
- [Counters in Aerospike](counters/README.md) - How to create and use atomic counters in aerospike.
- [Lists in Aerospike](lists/README.md) - How to use list operations in Aerospike.
- [Sorted Maps in Aerospike](maps/README.md) - How to use map operations in Aerospike.
- [Geo operations in Aerospike](geo/README.md) - How to use Geo operations in Aerospike.
- [Aerospike Spark integration](spark/READ.md) - How to use Aerospike with Apache Spark.
 
## Server Setup
Before comencing theses workshops, you will need to have a running Aerospike cluster, a single node cluster is fine. 

The Aerospike server runs on Linux only, if you are using Linux for this workshop, follow the instructions here to install the aerospike server and create a single node cluster. 

If you are like 99% of the world and use Windows or OS X The best way to to create a personal cluster is to use vagrant and virtual box, they is very easy to setup. Follow these instructions:
- Aerospike server on [Windows](http://www.aerospike.com/docs/operations/install/vagrant/win)
- Aerospike server on [OS X](http://www.aerospike.com/docs/operations/install/vagrant/mac)
 
## Programming language setup
The exercises are available in Java, C# (some not available) and Scala for Spark. Java exercises use Maven, C# use NuGet and Scala use SBT. Each example will download the appropirate Aerospike client library (driver) as part of the build process. 

Make sure you have installed Maven, NuGet and/or SBT before you start the exercises.

