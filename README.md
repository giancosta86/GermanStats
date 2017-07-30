# GermanStats

*Applying Data Mining to German linguistics*


## Introduction

**GermanStats** is an experimental project dedicated to applying *Data Mining* ideas and techniques to *German linguistics*, in order to infer rules about words.

The project consists of 2 scripts:

* **Extractor.py**: a *Python 3* script that:

    * parses, line-by-line, a huge file named **source.xml**, which should be the [German Wiktionary source file](http://download.wikipedia.org/dewiktionary/latest/dewiktionary-latest-pages-articles.xml.bz2) (to be downloaded and extracted separately)

    * retrieves (via regexes) linguistic information from it, creating a **corpus.csv** file. Currently, the corpus only includes nouns, each having a line whose format is:

    > (singular nominative, gender (m, n or f), singular genitive, plural nominative)


* **GermanStats.scala**: a Scala script that reads the above corpus and creates aggregated **YAML** files - which are included, for example, into [Farbiges Deutsch](http://gianlucacosta.info/FarbigesDeutsch/)



## Running the scripts

1. Download the [German Wiktionary source file](http://download.wikipedia.org/dewiktionary/latest/dewiktionary-latest-pages-articles.xml.bz2) into the project's directory and decompress it; rename the **.xml** file to **source.xml**

1. Run **./Extractor.py** within such directory - with no arguments (it is an executable Python 3 script): it should create **corpus.csv**

1. Run **scala GermanStats.scala** within the same directory, to create several **.yml** files



## Further information

* [Farbiges Deutsch](http://gianlucacosta.info/FarbigesDeutsch/)

* [Scala](https://www.scala-lang.org/)

* [Python](https://www.python.org/)

* [Wiktionary](https://www.wiktionary.org/)
