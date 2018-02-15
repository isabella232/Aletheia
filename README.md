<img src="/logo/Aletheia-logo.png" height="115"></img>
![](https://github.com/outbrain/Aletheia/blob/master/logo/Aletheia.png)

master branch: [![Build Status](https://travis-ci.org/outbrain/Aletheia.svg?branch=master)](https://travis-ci.org/outbrain/Aletheia)

Aletheia is a framework for implementing high volume, datum (event) based, producer-consumer data pipelines. Its key features are:
 * [Routing based data delivery](https://github.com/outbrain/Aletheia/wiki/Routing)
 * [Fine grained visibility (Breadcrumbs)](https://github.com/outbrain/Aletheia/wiki/Architectural-Overview#putting-things-together)
 * [Multiple serialization formats](https://github.com/outbrain/Aletheia/wiki/Serialization)
 * Schema evolution support    

Aletehia supports the following producers/consumers out-of-the-box:
* Kafka 0.10.2.1 (Producer, Consumer, Kafka Streams)
* Kafka 0.9 (production and consumption)
* Log files (production only)

Custom producer/consumer types are easy to write. See [Wiki - Endpoints](https://github.com/outbrain/Aletheia/wiki/EndPoints) for details.

Building Alethia
----------------
Clone the repo (or download the sources):

`git clone https://github.com/outbrain/Aletheia.git`

Build the project:

`mvn clean install -f Aletheia/pom.xml`

Further Info
------------

*  [Wiki](https://github.com/outbrain/Aletheia/wiki)
*  [Prezi](https://prezi.com/pqfbp7umqtvh/presentation)
*  [Talk at Java.IL (Hebrew)](https://www.parleys.com/tutorial/aletheia-outbrains-data-pipeline-backbone)
*  [Javadoc](http://outbrain.github.io/Aletheia/)


Aletheia has been developed by the Data Infrastructure team at [Outbrain](http://www.outbrain.com/).   
