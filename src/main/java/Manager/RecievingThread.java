import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecievingThread implements Runnable {
    final private AWSManager awsManager;
    final private String MassagesReceiverSQSURL;
    final private String managerToWorkerSQSURL;

    public RecievingThread(String MassagesReceiverSQSURL, String managerToWorkerSQSURL) {
        awsManager = AWSManager.getInstance();
        this.MassagesReceiverSQSURL = MassagesReceiverSQSURL;
        this.managerToWorkerSQSURL = managerToWorkerSQSURL;
    }

    @Override
    public void run() {
        while (true) {
            List<Message> requests = MassagesReceiverSQSMessages();
            if (requests.isEmpty()) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    System.out.println("[Debugger logs] Receiving Thread woke up from sleep");
                }
            } else {
                //Make messages visibility dynamic for the messages
                AtomicBoolean finishProcessRequests = new AtomicBoolean(false);
                makeMessagesVisibilityDynamic(requests, finishProcessRequests);

                //For each answer from a wroker
                for (Message message : requests) {
                    // get the local sqs url the link and the body of the message
                    Map<String, MessageAttributeValue> attributes = message.messageAttributes();
                    String localSQSUrl = attributes.get("localSQSUrl").stringValue();
                    String answer = message.body();

                    //Create a txt file with the name of the local application sqs (if not exist)
                    //Check if the localSQSUrl is in not exist
                    String fileName = "";

                    try {
                        synchronized (awsManager.createFilesLock) {
                            if (awsManager.MapOfNameFiles.get(localSQSUrl) == null) {
                                fileName = "Answer" + awsManager.filesCounter.get() + ".txt";
                                awsManager.MapOfNameFiles.put(localSQSUrl, fileName);
                                awsManager.filesCounter.getAndIncrement();
                            } else {
                                fileName = awsManager.MapOfNameFiles.get(localSQSUrl);
                            }
                        }
                    } catch (Exception e) {
                        System.out.printf("[Debugger log] Receiving thread Had problems with get/create the file it :  %n", fileName, localSQSUrl);
                        finishProcessRequests.set(true);
                        break;
                    }

                    try {
                        synchronized (awsManager.createFilesLock) {
                            writeToFile(fileName, answer);
                        }
                    } catch (IOException e) {
                        System.out.printf("[Debugger log] Receiving thread was unable to write to the file: %s of local url:  %n", fileName, localSQSUrl);
                        finishProcessRequests.set(true);
                        break;
                    }
                    //Update the value in the map (decrease by one)
                    try {
                        synchronized (awsManager.NumOfReviewsLock) {
                            awsManager.MapOfReviews.replace(localSQSUrl, awsManager.MapOfReviews.get(localSQSUrl) - 1);
                            int numOfReviews = awsManager.MapOfReviews.get(localSQSUrl);
                            if (numOfReviews == 0) {
                                //If the value is 0, upload the file to s3 and delete the key in the hashmap
                                awsManager.MapOfReviews.remove(localSQSUrl);
                                uploadFileToS3(fileName);
                                // Send message to the local sqs url with the name of the file and the bucket
                                SendToSQS(localSQSUrl, fileName);
                            }
                        }
                    } catch (Exception e) {
                        System.out.printf("[Debugger log] Error in updating the NumOfReviews .fileName: %s, localSqsUrl: %S  %n", fileName, localSQSUrl);
                        finishProcessRequests.set(true);
                        break;
                    }
                }
                //Set the finish timer to true;
                finishProcessRequests.set(true);
                for (Message m : requests) {
                    deleteMessageFromReceiverSQS(m);
                }
            }
            //Handle termination....
            try {
                synchronized (awsManager.NumOfReviewsLock) {
                    if (awsManager.MapOfReviews.isEmpty() && awsManager.terminate.get()) {
                        if (awsManager.WorkersCounter.get() == 0) {
                            //Send terminate to the workers
                            int numOfRunningWorkers = checkHowManyWorkersRunning();
                            terminateWorkers(numOfRunningWorkers);
                            awsManager.WorkersCounter.set(1);
                            Thread.sleep(5000);
                            return;
                        } else {
                            Thread.sleep(5000);
                            return;
                        }
                    }
                }
            } catch (Exception e) {
            }
        }
    }

    /**
     * This method gets finished tasks from the workers from the MassagesReceiver SQS
     *
     * @return list of messages available in the global sqs
     */
    private List<Message> MassagesReceiverSQSMessages() {
        try {
            synchronized (awsManager.receivingMessagesFromWorkers) {
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(MassagesReceiverSQSURL)
                        .visibilityTimeout(20)
                        .messageAttributeNames("All")
                        .maxNumberOfMessages(10)
                        .build();
                List<Message> messages = awsManager.sqs.receiveMessage(receiveRequest).messages();
                return messages;
            }
        } catch (Exception e) {
            System.out.println("[Debugger log] Error in gettingMessagesFromWorkers, returning an empty list");
            return new ArrayList<>();
        }
    }

    /**
     * This method create a file if he does not exist, and write lines to it. It Appends the lines to the end of the file
     *
     * @param fileName The file to writing the content to
     * @param content  The line to write to the file
     */
    private static void writeToFile(String fileName, String content) throws IOException {
        Path filePath = Paths.get(fileName);
        Files.write(filePath, content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * This method Upload the answer file to the S3 under the global bucketName of the manager.The key will be the file name
     *
     * @param answerFileName The file to upload to s3
     */
    private void uploadFileToS3(String answerFileName) {
        Path filePath = Paths.get(answerFileName);
        String key = filePath.getFileName().toString();

        awsManager.s3.putObject(PutObjectRequest.builder()
                .bucket(awsManager.bucketName)
                .key(key)
                .build(), RequestBody.fromFile(filePath));
    }

    /**
     * This method Sends a finish message to the local application sqs with the file name
     *
     * @param sqsUrl   The local application SQS url
     * @param fileName The name of the file that was uploaded
     */
    private void SendToSQS(String sqsUrl, String fileName) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(sqsUrl)
                .messageBody(fileName)
//                .delaySeconds(5)
                .build();
        awsManager.sqs.sendMessage(send_msg_request);
    }

    /**
     * This method deletes a list of Messages from the MassagesReceiver SQS
     *
     * @param message Messages to be deleted
     */
    private void deleteMessageFromReceiverSQS(Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(MassagesReceiverSQSURL)
                .receiptHandle(message.receiptHandle())
                .build();
        awsManager.sqs.deleteMessage(deleteRequest);
    }

    /**
     * This method set a timer thread That will update the visibility timeout of the messages.
     *
     * @param messages     list of Messages to update the visibility timeout for.
     * @param finishedWork finished or not to process the messages
     */
    private void makeMessagesVisibilityDynamic(List<Message> messages, AtomicBoolean finishedWork) {
        Thread timerThread = new Thread(() -> {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    for (Message message : messages) {
                        String receiptHandle = message.receiptHandle();
                        if (!finishedWork.get()) {
                            changeMessageVisibilityRequest(receiptHandle);
                        } else {
                            timer.cancel();
                            break; // Exit loop if finishedWork is true
                        }
                    }
                }
            }, 100, 15 * 1000);
        });
        timerThread.start();
    }

    /**
     * This method update the visibility timeout of the message to not letting the other threads to touch it.
     *
     * @param receiptHandle receiptHandle for updating the visibility for
     */
    public void changeMessageVisibilityRequest(String receiptHandle) {
        awsManager.sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(MassagesReceiverSQSURL)
                .visibilityTimeout(20)
                .receiptHandle(receiptHandle)
                .build());
    }

    /**
     * This method check how many workers are in "running" state.
     *
     * @return the number of running workers
     */
    private int checkHowManyWorkersRunning() {
        String nextToken = null;
        int counter = 0;

        do {
            DescribeInstancesRequest request = DescribeInstancesRequest
                    .builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = awsManager.ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.state().name().toString().equals("running")) {
                        for (software.amazon.awssdk.services.ec2.model.Tag tag : instance.tags()) {
                            if (tag.key().equals("Name") && tag.value().equals("Worker")) {
                                counter++;
                            }
                        }
                    }
                }
            }
            nextToken = response.nextToken();

        } while (nextToken != null);

        return counter;
    }

    /**
     * This method sends a terminate message to each active worker
     */
    private void terminateWorkers(int numOfWorkers) {
        for (int i = 0; i < numOfWorkers; i++) // Send Terminate message to all activate workers
            sendTerminateMessageToWorkers();
    }

    /**
     * This method sends a terminate message to a worker
     */
    private void sendTerminateMessageToWorkers() {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(managerToWorkerSQSURL)
                .messageBody("terminate!")
                .messageAttributes(
                        Map.of(
                                "Terminate", MessageAttributeValue.builder().dataType("String").stringValue("Terminate").build()
                        )
                )
                .build();
        awsManager.sqs.sendMessage(send_msg_request);
    }
}


