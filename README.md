# DSP_2
- id: fill
- username: rotba

## Artifacts
- [Results](https://s3.console.aws.amazon.com/s3/buckets/jarbucket1653138772100?region=us-east-1&prefix=fouts/&showversions=false)
- [Steps statistics](https://s3.console.aws.amazon.com/s3/buckets/aws-logs-494081938343-us-east-1?region=us-east-1&prefix=elasticmapreduce/j-1DFJDUJD78PTU/steps/&showversions=false)
- [English statistics]([url](https://s3.console.aws.amazon.com/s3/buckets/aws-logs-494081938343-us-east-1?region=us-east-1&prefix=elasticmapreduce/j-38IU6FVRC6OW9/steps/eng_run/&showversions=false))
- [Hebrew statistics]([url](https://s3.console.aws.amazon.com/s3/buckets/aws-logs-494081938343-us-east-1?region=us-east-1&prefix=elasticmapreduce/j-1DFJDUJD78PTU/steps/heb_run/&showversions=false))

## How to run
- The jar is already in the an s3
- An EMR cluster should be manuly be started. Denote `CLUSTER_ID` the id of the started cluser
- build the emr jar:
 - `cd EMR_runner`
 - `mvn compile assembly:single`
 - `java -jar target/<the generated jar> CLUSTER_ID <language>` 
 
## Job flow steps
 - Denote _Words_ the set of all the words in the 1-gram corpus
 #### 1-Gram to count & decade
 - (1-gram-data) -> OGWC: Decade * Words * {*} -> Decade * N * {-1} * {-1}
 #### 2-Gram to count & decade
 - (1-gram-data) -> TGWC: Decade * Words * Words -> Decade * {-1} * {-1} * N
 #### 1-Gram to words-per-decade
 - (1-gram-data) -> DECC: Decade -> N
 #### C1 and deccade count join
 - {OGWC, TGWC, DECC} -> C1_DECC_JOIN: Decade * N * {-1} * N
 #### C2 join
 - {OGWC, C1_DECC_JOIN} -> C2_DECC_JOIN: Decade * N * N * N
 #### Liklehood calculation
 - C2_DECC_JOIN -> FINAL_RESULT: Decade * N * N * N -> R
 

