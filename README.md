# Message_Processor
# Descrition
**input stream:** queueStream, created a static list of messages, and added to the queue. MessageAs and MessageBs are added to the list in random sequence. 
Fields: “eventID”, “attributeIndex” are random numbers.
* The input msg are saved in “input.json”
* Output streaming are saved in “./output/”
* Build by Maven

# Test 
The Dstream RDD is saved as text file in the folder “./output/” in 4 partitions, corresponding to the 4 nodes specified in the spark configuration.  The keys (attributeIndex) are partitioned byHash, shows the mapWithState() method works well.

## case1:
input stream: 
{"group":"update","events":[{"eventId":952,"attributeIndex":1},{"eventId":3015,"attributeIndex":1}]}
{"group":"update","events":[{"eventId":4135,"attributeIndex":0},{"eventId":2413,"attributeIndex":0}]}
{"attributeIndex":0,"origin":"Country76"}
{"group":"update","events":[{"eventId":5882,"attributeIndex":1},{"eventId":1783,"attributeIndex":1}]}
{"group":"update","events":[{"eventId":3328,"attributeIndex":1},{"eventId":9523,"attributeIndex":1}]}

output Stream:
{"attributeIndex":"0","group":"update","eventId":"4135","origin":"Country76"}Time:1559539808768


