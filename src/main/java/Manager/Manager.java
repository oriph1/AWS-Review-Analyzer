import com.amazonaws.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.Pair;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {
    //AWS of type manager
    final static AWSManager awsManager = AWSManager.getInstance();
    public static void main(String[] args) {
        //Check if the global sqs is created. If not create it
        String globalSQSURL = CheckglobalSQS();

        //Check if messageReceiverSQS is created. If not create it
        String messageReceiverSQSURL = CheckMassagesReciverSQS();


        //Check if managerToWorkersSQS is created. If not create it.
        String managerToWorkerSQSUrl = CheckAndCreateManagerToWorkerSQS();

        //Create bucket for the answers of the manager to the local applications
        createOutputFilesBucketBucketIfNotExists(awsManager.bucketName);

        ListeningThread listeningThread = new ListeningThread(globalSQSURL);
        Thread listening = new Thread(listeningThread);

        ExecutorService executorAction = Executors.newFixedThreadPool(4);
        for (int i = 0; i<5; i++){
            ActionThread actionThread = new ActionThread(globalSQSURL,messageReceiverSQSURL,managerToWorkerSQSUrl);
            executorAction.submit(actionThread);
        }

        ExecutorService executorReceiver = Executors.newFixedThreadPool(4);
        for (int i = 0; i<5; i++){
            RecievingThread recievingThread = new RecievingThread(messageReceiverSQSURL,managerToWorkerSQSUrl);
            executorReceiver.submit(recievingThread);
        }

        listening.start();

        //Wait for listening thread and Action threads to finish their job.
        try {
            listening.join();
            executorAction.shutdown();
            executorAction.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);
            System.out.println("[Debugger logs]finish to send the workers termination message");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Wait for the receivers to finish their jobs
        try {
            executorReceiver.shutdown();
            executorReceiver.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);

            //Terminate the manager
            shutdownInstance();
            System.out.println("[Debugger logs]Finish to close all the sqs and buckets");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method check if the global sqs for receiving messages from the local application exist. if not, it creates one.
     *
     * @return The global SQS Url
     */
    private static String CheckglobalSQS(){
        String globalQueueUrl;
        try {
            // Check if the global SQS queue exists
            GetQueueUrlResponse getQueueUrlResponse = awsManager.sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(awsManager.globalSQS)
                    .build());

            globalQueueUrl = getQueueUrlResponse.queueUrl();

        } catch (QueueDoesNotExistException e) {
            // If the global SQS queue does not exist, create one
            CreateQueueResponse createQueueResponse = awsManager.sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(awsManager.globalSQS)
                    .build());

            globalQueueUrl = createQueueResponse.queueUrl();
        }
        return globalQueueUrl;
    }

    /**
     * This method check if the Message receiver sqs for receiving answers from the workers exist. if not it creates one.
     *
     * @return The Message receiver SQS Url
     */
    private static String CheckMassagesReciverSQS(){
        String massageReciverSQS;
        try {
            // Check if the global SQS queue exists
            GetQueueUrlResponse getQueueUrlResponse = awsManager.sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(awsManager.MassagesReciverSQS)
                    .build());

            massageReciverSQS = getQueueUrlResponse.queueUrl();

        } catch (QueueDoesNotExistException e) {
            // If the global SQS queue does not exist, create one
            CreateQueueResponse createQueueResponse = awsManager.sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(awsManager.MassagesReciverSQS)
                    .build());

            massageReciverSQS = createQueueResponse.queueUrl();
        }
        return massageReciverSQS;
    }

    /**
     * This method checks if the Manager To workers SQS for delegating tasks exist. if not it creates one.
     *
     * @return The Manager To workers SQS Url
     */
    private static String CheckAndCreateManagerToWorkerSQS() {
        String managerToWorkerQueueUrl;
        try {
            // Check if the global SQS queue exists
            GetQueueUrlResponse getQueueUrlResponse = awsManager.sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(awsManager.managerToWorkerSQS)
                    .build());

            managerToWorkerQueueUrl = getQueueUrlResponse.queueUrl();

        } catch (QueueDoesNotExistException e) {
            // If the global SQS queue does not exist, create one
            CreateQueueResponse createQueueResponse = awsManager.sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(awsManager.managerToWorkerSQS)
                    .build());

            managerToWorkerQueueUrl = createQueueResponse.queueUrl();
        }
        return managerToWorkerQueueUrl;
    }


    /**
     * This method Creates a unique bucket for the Manager answer txt files to the local applications.
     *
     * @param bucketName the name of the bucket
     */
    private static void createOutputFilesBucketBucketIfNotExists(String bucketName) {
        try {
            awsManager.s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            awsManager.s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * This method Checks How many workers with the tag Worker are running
     *
     * @return the number of active workers
     */
    private static int checkHowManyWorkersRunning() {
        int counter = 0;
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder()
                                .name("tag:Name")
                                .values("Worker")
                                .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("running")
                                .build()
                )
                .build();
        DescribeInstancesResponse response = awsManager.ec2.describeInstances(request);
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                if (instance.tags().stream().anyMatch(tag -> tag.key().equals("Name") && tag.value().equals(awsManager.workerTag))
                        && instance.state().nameAsString().equals("running")) {
                    counter++;
                }
            }
        }
        return counter;
    }

    private static void deleteSqs(String sqsUrl) {
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(sqsUrl)
                .build();

        // Delete the SQS queue
        DeleteQueueResponse deleteQueueResponse = awsManager.sqs.deleteQueue(deleteQueueRequest);

        // Check the response status
        if (deleteQueueResponse.sdkHttpResponse().isSuccessful()) {
            System.out.printf("[Debugger log] Delete the sqs!. The sqs url:%s %n", sqsUrl);
        } else {
            System.out.printf("[Debugger log] Delete of sqs failed!. The sqs url:%s %n", sqsUrl);
        }
    }

    public static void shutdownInstance() {
        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(EC2MetadataUtils.getInstanceId())
                    .build();

            awsManager.ec2.terminateInstances(request);
        } catch (Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }
}
