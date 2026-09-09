# NATS Stream

JetStream

Creation:
- Stream names are case-sensitive
- Subjects bind to exactly one stream
Issues:
- By Default, unlimited limits(Max count, age, bytes) - Disk fills up and takes down the server with it
- Stream Name can't be editted after creation

Publishing:
- We get back PubAck different compared to core nats
- We publish to subjects and internally NATs checks stream decision




Things to check 
1. Complete Configuration of Stream Creation
2. Stream Edititing
2. Stream storage
3. How to split streams
4. How to manipulate stream sequences or pointers
5. Replication in streams
6. Cluster of streams


Demo Plan:

1. Creating a Stream with Configuration and explanation
2. Editing current stream
3. Stream Storage
4. Manipulation of Stream Pointers
5. Replication in Streams
6. Cluster of streams

