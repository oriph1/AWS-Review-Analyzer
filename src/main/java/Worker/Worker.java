import com.amazonaws.util.EC2MetadataUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Integer.parseInt;

public class Worker {
    static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();
    final static AWSWorker awsWorker = AWSWorker.getInstance();

    public static void main(String[] args) {
        //Get the managerToWorkerSQS Url
        String managerToWorker = CheckSQS(awsWorker.managerToWorkerSQS);

        //Get the MassagesReceiverSQS Url for sending the Manager the answer
        String MassagesReceiver = CheckSQS(awsWorker.MassagesReciverSQS);

        while (true) {
            List<Message> messages = ConnectAndReceiveFromManager(managerToWorker);
            if (messages.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("[Debugger logs] Worker Thread woke up from sleeping. Start a job");
                    continue;
                }
            } else {
                for (Message message : messages) {
                    AtomicBoolean finishedWork = new AtomicBoolean(false); // for visibility extend
                    //Extend the visibilty timout of each message
                    makeMessageVisibilityDynamic(message, MassagesReceiver, finishedWork);

                    //Process the message
                    String link;
                    String rating;
                    String sqsLocalUrl;
                    //receive the message of the name of the bucket and the name of the file
                    Map<String, MessageAttributeValue> attributes = message.messageAttributes();
                    // Access individual attributes by keys
                    MessageAttributeValue linkAttribute = attributes.get("Link");
                    MessageAttributeValue ratingAttribute = attributes.get("Rating");
                    MessageAttributeValue sqsLocalUrlAttribute = attributes.get("SQSLocalUrl");
                    link = (linkAttribute != null) ? linkAttribute.stringValue() : null;
                    rating = (ratingAttribute != null) ? ratingAttribute.stringValue() : null;
                    sqsLocalUrl = (sqsLocalUrlAttribute != null) ? sqsLocalUrlAttribute.stringValue() : null;

                    ///Check terminate
                    if (message.body().equals("terminate!")){
                        deleteMessageFromManagerToWorkerSQS(managerToWorker, message);
                        finishedWork.set(true);
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            System.out.println("[Debugger logs] Worker woke up from sleep");
                        }
                        shutdownInstance();
                        return;
                    }

                    int sentiment = processSentimentReview(message.body());
                    String entities = processEntitiesReview(message.body());
                    String sarcasm = processSarcasmReview(parseInt(rating), sentiment);

                    //Build the Response
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append("Sentiment: ").append(sentiment).append("\n");
                    stringBuilder.append("Link: ").append(link).append("\n");
                    stringBuilder.append("Entities: ").append(entities).append("\n");
                    stringBuilder.append("Sarcasm: ").append(sarcasm);
                    stringBuilder.append("\n");
                    stringBuilder.append("\n");
                    String response = stringBuilder.toString();


                    ///Send Message To the receiving thread of the manager
                    SendToManagerSQS(MassagesReceiver, sqsLocalUrl, response);

                    //Delete Message
                    deleteMessageFromManagerToWorkerSQS(managerToWorker, message);

                    //Set the visibility boolean to true and stop the timer
                    finishedWork.set(true);

                }
            }
        }
    }

    /**
     * This method Do the sentiment Analysis of the review.
     *
     * @param review review to analize
     * @return the sentiment of the review
     */
    private static int processSentimentReview(String review) {
        // Perform Sentiment Analysis
        return sentimentAnalysisHandler.findSentiment(review);
    }

    /**
     * This method Do the process entities Analysis of the review.
     *
     * @param review review to analize
     * @return a string of the entities of the review
     */
    private static String processEntitiesReview(String review) {
        // Perform Named Entity Recognition
        return namedEntityRecognitionHandler.printEntities(review);
    }

    /**
     * This method check if the message is sarcastic or not.
     *
     * @param rating    rating of the review
     * @param sentiment the sentiment of the algorithm
     * @return Sarcasm or not
     */
    private static String processSarcasmReview(int rating, int sentiment) {
        if (rating != sentiment) return "Sarcasm";
        return "No Sarcasm";
    }


    /**
     * This method check if a specific SQS is created. If not, It creates one
     *
     * @param sqsName the name of the SQS
     * @return The SQS URL
     */
    private static String CheckSQS(String sqsName) {
        String SQSQueueUrl;
        try {
            // Check if the global SQS queue exists
            GetQueueUrlResponse getQueueUrlResponse = awsWorker.sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(sqsName)
                    .build());

            SQSQueueUrl = getQueueUrlResponse.queueUrl();

        } catch (QueueDoesNotExistException e) {
            // If the global SQS queue does not exist, create one
            CreateQueueResponse createQueueResponse = awsWorker.sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(sqsName)
                    .build());

            SQSQueueUrl = createQueueResponse.queueUrl();
        }
        return SQSQueueUrl;
    }

    /**
     * This method gets tasks from the Manager to Workers SQS
     *
     * @param sqsUrl the sqs url
     * @return list of messages available in the Manager to Workers sqs
     */
    private static List<Message> ConnectAndReceiveFromManager(String sqsUrl) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(sqsUrl)
                .visibilityTimeout(15)
                .messageAttributeNames("All")
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = awsWorker.sqs.receiveMessage(receiveRequest).messages();
        return messages;
    }

    /**
     * This method Send an answer to the manager via the MassagesReceiver SQS.
     *
     * @param sqsUrl      the sqs url to send the message to
     * @param localSqsUrl the local application sqs url
     * @param response    the response to send
     */
    private static void SendToManagerSQS(String sqsUrl, String localSqsUrl, String response) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(sqsUrl)
                .messageBody(response)
                .messageAttributes(
                        Map.of(
                                "localSQSUrl", MessageAttributeValue.builder().dataType("String").stringValue(localSqsUrl).build()
                        )
                )
                .build();
        awsWorker.sqs.sendMessage(send_msg_request);
    }

    /**
     * This method deletes a list of Messages from the managerToWorker SQS
     *
     * @param message Messages to be deleted
     */
    private static void deleteMessageFromManagerToWorkerSQS(String managerToWorkerURL, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(managerToWorkerURL)
                .receiptHandle(message.receiptHandle())
                .build();
        awsWorker.sqs.deleteMessage(deleteRequest);
    }

    /**
     * This method set a timer thread That will update the visibility timeout of a message.
     *
     * @param message a message to update the visibility timeout for.
     * @param finishedWork finished or not to process the message
     */
    private static void makeMessageVisibilityDynamic(Message message, String workerQueueUrl, AtomicBoolean finishedWork) {
        String receiptHandle = message.receiptHandle();
        Thread timerThread = new Thread(() -> {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (!finishedWork.get())
                        changeMessageVisibilityRequest(workerQueueUrl, receiptHandle);
                    else {
                        timer.cancel();
                    }
                }
            }, 100, 10 * 1000);
        });
        timerThread.start();
    }

    /**
     * This method update the visibility timeout of the message to not letting the other threads to touch it.
     *
     * @param receiptHandle receiptHandle for updating the visibility for
     */
    public static void changeMessageVisibilityRequest(String queueUrl, String receiptHandle) {
        awsWorker.sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(15)
                .receiptHandle(receiptHandle)
                .build());
    }

    /**
     * This method is shutting down the current Instance
     *
     */
    public static void shutdownInstance() {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(EC2MetadataUtils.getInstanceId())
                .build();

        awsWorker.ec2.terminateInstances(request);
    }
}



