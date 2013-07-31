
-----------------------------------------------------

WELCOME
=======
This is a really simple MapReduce application 'Average' that illustrates how to use MRUnit and log4j (see AverageMapper.class) with Hadoop.

The application itself determines the 'average length' of words based upon the first character of the word
contained within a document (supplied as a command line parameter when the application is initiated).

./pom.xml:
A maven project descriptor that describes how to build this module.

TESTING
=======

This  project also contains test classes that can be run as part of a test
suite.

BUILD
=====

> mvn package

ADDITIONAL RESOURCES
====================
Everything you need to know about getting started with the Cloudera CDH distribution
(including Apache Hadoop) can be found here: http://www.cloudera.com

Why Cloudera:
http://www.cloudera.com/content/cloudera/en/why-cloudera.html

Download CDH & Cloudera Manager:
https://ccp.cloudera.com/display/SUPPORT/Downloads

