YCSB-riak-binding
====================

Riak database interface for YCSB

Installation guide
==================

* Download the YCSB project as follows: git clone https://github.com/brianfrankcooper/YCSB.git
* Include the Riak DB binding within the YCSB directory: git clone https://github.com/arnaudsjs/YCSB-riak-binding.git riak
* Add <module>riak</module> to the list of modules in YCSB/pom.xml
* Add the following lines to the DATABASE section in YCSB/bin/ycsb: "riak" : "riakBinding.RiakClient"
* Compile everything by executing the following command within the YCSB directory: mvn clean package
