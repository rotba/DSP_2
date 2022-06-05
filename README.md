# DSP_2
- id: fill
- username: rotba

## Artifacts
- [Steps statistics](https://s3.console.aws.amazon.com/s3/buckets/aws-logs-494081938343-us-east-1?region=us-east-1&prefix=elasticmapreduce/j-1DFJDUJD78PTU/steps/&showversions=false)
- statistics - is there a way to aggregate everything?

## How to run
- The jar is already in the an s3
- An EMR cluster should be manuly be started. Denote `CLUSTER_ID` the id of the started cluser
- build the emr jar:
 - `cd EMR_runner`
 - `mvn compile assembly:single`
 - `java -jar target/<the generated jar> CLUSTER_ID` <language>
 
## Job flow steps
 - Denote _Words_ the set of all the words in the 1-gram corpus
 #### 1-Gram to count & decade
 - (1-gram-data) -> OGWC: Decade * Words * {*} -> N
 #### 2-Gram to count & decade
 - (1-gram-data) -> TGWC: Decade * Words * Words -> N
 #### 1-Gram to words-per-decade
 - (1-gram-data) -> DECC: Decade -> N
