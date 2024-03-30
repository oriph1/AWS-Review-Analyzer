# **DistributedSystems Assigment1**

## Introduction
This project is about implementing a real-world application to distributively process a list of amazon
reviews. We used the aws services such as: S3, EC2, AMI and SQS for implementing the app.

## Project structure

This diagram represents the structure of the project and the flow of the data in the system:
![Diagram](C:\Users\OriHayoon\IdeaProjects\DistributedSystems_Assigment1\Readme Images\Archi.jpg)
(if the picture is not loading, you can look at the folder ReadMe images. The picture of the architacture is there)

**Explanation:**
1) A local application is checking if the manager is running. If not, it starts an ec2 instance with the manager script.
After that the local application uploads the txt file to S3 and then sends a message to the "global SQS" (The instance takes his jar file from the bucketjarsbucket123)
All the local applications do the same process but only one creates an ec2 instance for the Manager.

2) The manager is consists of three groups of Threads:
---- "Listening Thread": The listening thread is listening to the global sqs and receives the messages from the local applications.
Each message, he transfers to the QueueOfRequets so the "action threads" will get it and start to process the request.
---- "Action Thread pool": Each thread takes a message from the QueueOfRequets, takes the input file that is uploaded by the local application. Then 
he separates it to reviews, and then initiate a number of workers based on the number of the active ones. After that he sends to each worker his job.
---- "Receiving thread pool": The threads are receiving the answers from the workers from the "Message Receiver SQS".
Each thread process a message from a worker and update the "answer file" of the local application that sent this task.
After all the tasks from a specific local application were processed, The thread uploads the final answer to the s3 answer bucket and send a message to the local application
that the job is done and the answer is in S3.

3) The workers get the reviews from the "Worker To Manager SQS". The answer of the algorithms they run on the 
review is sent to the "Message receiver SQS" to the manager (to the Receiving threads)

4) The local application receive the message, download the answer file from the answers bucket 
and then create a html file out of it. It also deletes the local application bucket and the local application sqs.

***If the manager gets a terminate message from a local application, It does not receive any more tasks
from  local applications and after all the job is done, he sends a terminate message to the workers
and terminates himself.

## Dependencies
ami: ami-00e95a9222311e8ed type: large

## Running times
For a local application that sent one input to the manager, it took 5 minutes to finish the job.
For two local applications, the first sent one input file and the other one sent two inputs, it took 20 minutes to
finish the process.
The "n" we used was 100.

## Scalability
Each local application has one unique SQS for receiving the answer from the manager and for uploading the txt file.
The manager has: one queue for transferring the tasks from the local applications to the action threads.
In addition, it has two hashmaps, one save the local applications SQS url as a key and the number of reviews that needs to
be processed as a value, and the other one save the local application SQS url as a key and the answer file name for the 
local application as a value. It also creates two SQS for communicating with the workers.
The workers don't use any extra memory, just receiving the review and process it and sends the manager the answers.

## Persistence
If a worker is terminated for some reason, because he took the message with a visisbilty timeout and does  
not delete the message until he finished the job, a different worker will take his job.
In addition, each time a manager receive a task from a local application, it checks how many active workers there
are and initiate more as needed.

## Threads
We used threads in the manager in order to process more than one request at a time (action threads), 
also we used that for process more than one answer from the workers at a time (Receiving threads).

### Workers job
As we expected, there were some workers who processed more reviews than other workers. 
The reason was that all the workers took their tasks from the same queue, and there 
were some reviews that took more time to process and perform the sentiment analysis and named-entity recognition
(Because in input file 3 we had long reviews, it took more time to process these reviews).


### How to run the project
Add the next arguments as a command line arguments:
1) the input files you want to process (input1.txt input2.txt)
2) add the name of the final output file name (outputFile)
3) and the number of tasks per worker (100)
4) if you want to terminate, add the arrgument "terminate"

now you can run the local application :)







