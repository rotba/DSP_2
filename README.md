# DSP_2
- id: fill
- username: rotba

## Artifacts
- [link to results](broken)
- statistics - is there a way to aggregate everything?

## How to run
- The jar is already in the an s3
- An EMR cluster should be manuly be started. Denote `CLUSTER_ID` the id of the started cluser
- build the emr jar:
 - `cd EMR_runner`
 - `mvn compile assembly:single`
 - `java -jar target/<the generated jar> CLUSTER_ID` <language>
