JCdmBuilder
==============

The JCdmBuilder is a Java program that offers various tools that can be used when contructing a data in the OMOP Common Data Model (CDM). It can also be used to transform some specific observational datasets from their native formats and schemas into the CDM.  

Features
========
* This builder supports the following ETLs:
* You can also load data in CDM format from CSV files into the database.
* Creating the database structure and indices for CDM version 5.0.1, 5.3.0, 5.3.1, 5.4.0, prepared for version 6.0.
* Loading the vocabulary from files into the database.
* Automatically creating condition and drug eras.
* Supports SQL Server, Oracle and PostgreSQL.
* Supports bulk load from CSV files for PostgreSQL and SQL Server.
* Command line interface for automated execution.

Screenshot
===========
<img src="https://github.com/mi-erasmusmc/JCdmBuilder/blob/master/man/Screenshot.png" alt="JCdmBuilder" title="JCdmBuilder" />

Technology
============
JCdmBuilder is a Java program to load data into a CDM database as part of an ETL.

System Requirements
============
* Java

Dependencies
============
None
 
Getting Started
===============

1. Under the [Releases](https://github.com/OHDSI/JCdmBuilder/releases) tab, download the latest JCDMBuilder jar file.
2. Double-click on the jar file to start the application.

The first three numbers in the version number correspond to the last accepted release of the CDM that is implemented in the JCDMBuilder. 
There is also a command-line-interface. Type `java -jar JCDMBuilder_v?.?.?.?.jar -usage` for more information.

Interface
=========

The interface consists of four tabs:

* The Locations tab contains the location of the working folder and all information for connecting to the target database.
* The Vocabulary tab contains all information on the location of the vocabulary files/schema and how to load it.
* The ETL tab contains information on the location of the data files and the format and how to load them.
* The Execute tab contains checkboxes where you can specify what you want to do. Be aware that the order of the checkboxes specifies the order the actions should be performed in. The interface allows you to perform them one by one but in tha case you should keep track yourself of which actions you already have done.

_The Locations tab_

| Field | Description |
| ------------------------ | ----------------------------------------------------------------------------- |
|Working folder | This is the folder where the log file (Console.txt) and the error files are written. |

The section "Target CDM Location" contains all information on how to connect to the target database. Currently the JCDMBuilder supports three database types for which we will describe how to fill the connection details.

Oracle:

| Field | Description |
| ------------------------ | ----------------------------------------------------------------------------- |
| Server location | \<server name/IP-address\>[\:\<port\>]/\<SID\> |
| User name | The user used to connect to the database. This user should have the right to create and remove a schema/user. |
| Password | The password of the user. This password will also be used for the CDM and Results schama's/users. |
| CDM Schema name | The name of the schema/user that will hold the CDM tables. |
| Results Schema name | The name of the schema/user that will hold the Results tables of Achilles/Atlas. |
| CDM version | The version of the OMOP CDM to be used. |

PostgreSQL:

| Field | Description |
| ------------------------ | ----------------------------------------------------------------------------- |
| Server location | \<server name/IP-address\>[\:\<port\>]/\<database name\> |
| User name | The user used to connect to the database. This user should have the right to create and remove a schema. |
| Password | The password of the user. |
| CDM Schema name | The name of the schema that will hold the CDM tables. |
| Results Schema name | The name of the schema that will hold the Results tables of Achilles/Atlas. |
| CDM version | The version of the OMOP CDM to be used. |

SQL Server:

| Field | Description |
| ------------------------ | ----------------------------------------------------------------------------- |
| Server location | \<server name/IP-address\>[\:\<port\>];database=\<database name\>; |
| User name | The user used to connect to the database. |
| Password | The password of the user. |
| CDM Schema name | The name of the schema that will hold the CDM tables. |
| Results Schema name | The name of the schema that will hold the Results tables of Achilles/Atlas. |
| CDM version | The version of the OMOP CDM to be used. |


_The Vocabulary tab_

At the top of the tab is a dropdown list containing three options. For each of the options different fields are shown.
The options an their corresponding fields are: 

1. Load ATHENA CSV files to server

With this option the records are inserted directly into the database.

| Field | Description |
| ------------------------ | ----------------------------------------------------------------------------- |
| Folder | The folder where the CSV files of the vocabulary are stored. |

2. Bulk Load ATHENA CSV files from server to server

With this option the files are first copied to a folder on the database server and then loaded into the database in bulk mode.
This option is not available for Oracle.

| Field | Description |
| ------------------------ | ----------------------------------------------------------------------------- |
| Folder | The folder where the CSV files of the vocabulary are stored. |
| Server folder | The folder that is mapped to a folder on the database server where the files can be copied to for the import. |
| Local path server folder | The path on the database server where the files are copied to. |

3. Load vocabulary from schema

With this option the vocabulary is copied from another schema in the database.
This option is not available for Oracle.

| Field | Description |
| ------------------------ | ----------------------------------------------------------------------------- |
| Schema | The schema where the source vocabulary is stored. The user specified in the Locations tab should also have access to this schema. |

_The ETL tab_


_The Execute tab_


Getting Involved
=============
* User Guide:  To be developed
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="../../issues">GitHub issue tracker</a> for all bugs/issues/enhancements

License
=======
JCdmBuilder is licensed under Apache License 2.0

Development
===========

### Development status
Alpha

Acknowledgements
================
Janssen Pharmaceutical Research & Development, LLC
