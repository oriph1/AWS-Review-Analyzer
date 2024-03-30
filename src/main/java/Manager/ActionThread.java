import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.Pair;

import java.util.stream.Collectors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.*;

import static java.lang.Integer.parseInt;

public class ActionThread implements Runnable {
    final private AWSManager awsManager;

    final private String messageReceiverSQSURL;
    final private String managerToWorkerSQSURL;
    final private String globalSQSURL;

    public ActionThread(String globalSQSURL, String messageReceiverSQSURL, String managerToWorkerSQSURL) {
        this.globalSQSURL = globalSQSURL;
        this.messageReceiverSQSURL = messageReceiverSQSURL;
        this.managerToWorkerSQSURL = managerToWorkerSQSURL;
        awsManager = AWSManager.getInstance();
    }

    @Override
    public void run() {
        while (!awsManager.terminate.get()) {
            if (awsManager.QueueOfRequets.isEmpty()) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    System.out.println("[Debugger logs] Actions Thread woke up from sleeping");
                }
            } else {
                //ActionThread thread takes a mission from the QueueOfRequests
                Message message = awsManager.QueueOfRequets.poll();
                String sqsLocalUrl = message.body();

                //receive the message of the name of the bucket and the name of the file
                Map<String, MessageAttributeValue> attributes = message.messageAttributes();
                // Access individual attributes by key
                MessageAttributeValue bucketAttribute = attributes.get("Bucket");
                MessageAttributeValue fileAttribute = attributes.get("File");
                MessageAttributeValue nAttribute = attributes.get("n");
                MessageAttributeValue terminateAttribute = attributes.get("Terminate");

                // Extract attribute values
                String bucketName = (bucketAttribute != null) ? bucketAttribute.stringValue() : null;
                String fileName = (fileAttribute != null) ? fileAttribute.stringValue() : null;
                String n = (nAttribute != null) ? nAttribute.stringValue() : null;
                String terminate = (terminateAttribute != null) ? terminateAttribute.stringValue() : null;

                //Parsing the reviews and count how many reviews there are.
                awsManager.MapOfReviews.put(sqsLocalUrl, 0);
                int sumOfReviews = ProcessRequest(bucketName, fileName, sqsLocalUrl);

                //Create Workers
                try {
                    synchronized (awsManager.updateWorkersLock) {
                        //Check if there are workers who already working
                        int numOfWorkers = checkActiveWorkers();
                        if (numOfWorkers < 8) {
                            //Calculate the number of workers that is needed
                            int num = sumOfReviews / parseInt(n);
                            if (num <= 8 && num > numOfWorkers) {
                                //Create workers
                                createWorkers(num - numOfWorkers);
                            }
                            else if (num > 8){
                                createWorkers(8-numOfWorkers);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("[Debugger logs] Action Thread was not able to create workers");
                    continue;
                }
                if (terminate.length() > 2){
                    awsManager.terminate.set(true);
                }

                //Delete the message from the global SQS
                deleteMessageFromGlobalSqs(globalSQSURL, message);
            }
        }
    }

    /**
     * This method responsible for process the request of the local application.
     * The function parses the reviews of each JSON and counts how many reviews there are.
     *
     * @param bucketName  the name of the local application bucket
     * @param fileName    the name of the file the local application uploaded
     * @param sqsLocalUrl the local application sqs url
     *                    //     * @param reviews    List that will contain all the data of the reviews
     * @return The number of reviews
     */
    private int ProcessRequest(String bucketName, String fileName, String sqsLocalUrl) {
        int sumOfReviews = 0;
        try {
            // Create request to get the object
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();

            // Get the S3 object
            ResponseInputStream<GetObjectResponse> s3Object = awsManager.s3.getObject(getObjectRequest);

            // Get the content of the object as an InputStream
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object))) {
                // Process each line
                String line;
                while ((line = reader.readLine()) != null) {
                    //Process each Json Object line and get the data
                    sumOfReviews += processReview(line, sqsLocalUrl);
                }

            } catch (IOException e) {
                System.out.printf("[Debugger log] Failed to read lines from the file, error message: %s %n", e.getMessage());
            }
        } catch (Exception e) {
            System.out.printf("[Debugger log] Failed to open the file answer from s3, error message: %s %n", e.getMessage());
        }
        return sumOfReviews;
    }

    /**
     * This method responsible for extract the reviews of each JSON line.
     * Add each review to the reviewsList, and increase the counter.
     *
     * @param jsonLine    The JSON line from the file.
     * @param sqsLocalUrl The local application sqs url
     *                    //     * @param reviewsList List that will contain all the data of the reviews
     * @return The number of reviews in this jsonLine
     */
    private int processReview(String jsonLine, String sqsLocalUrl) {
        int counter = 0;
        JsonParser parser = new JsonParser();
        JsonObject jsonObject = parser.parse(jsonLine).getAsJsonObject();
        // Extract the reviews array from the JSON object
        JsonArray reviews = jsonObject.getAsJsonArray("reviews");
        for (int i = 0; i < reviews.size(); i++) {
            // Extract the review Text and the review URL.
            JsonObject review = reviews.get(i).getAsJsonObject();
            String reviewText = review.getAsJsonPrimitive("text").getAsString();
            String reviewUrl = review.get("link").getAsString();
            String rating = review.get("rating").getAsString();
            Pair<String, String> urlReviewPair = Pair.of(reviewUrl, reviewText);
            SendToManagerToWorkerSQS(sqsLocalUrl, managerToWorkerSQSURL, urlReviewPair, rating);

            counter++;
        }
        try {
            synchronized (awsManager.NumOfReviewsLock) {
                awsManager.MapOfReviews.replace(sqsLocalUrl, awsManager.MapOfReviews.get(sqsLocalUrl) + counter);
            }
        } catch(Exception e) {
            System.out.println("[Debugger logs] Action Thread was not able to Change the MapOfReviews");
        }

        return counter;
    }

    /**
     * This method send a task to the workers contains all the details.
     *
     * @param localSQSUrl        the url of the local application that sent the review
     * @param sqsManagerToWorker the ManagerToWorker sqs url
     * @param urlLinkReview      the link of the review
     * @param rating             the rating of the review
     */
    private void SendToManagerToWorkerSQS(String localSQSUrl, String sqsManagerToWorker,
                                          Pair<String, String> urlLinkReview, String rating) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(sqsManagerToWorker)
                .messageBody(urlLinkReview.right())
                .messageAttributes(
                        Map.of(
                                "Link", MessageAttributeValue.builder().dataType("String").stringValue(urlLinkReview.left()).build(),
                                "Rating", MessageAttributeValue.builder().dataType("String").stringValue(rating).build(),
                                "SQSLocalUrl", MessageAttributeValue.builder().dataType("String").stringValue(localSQSUrl).build()
                        )
                )
                .delaySeconds(10)
                .build();
        awsManager.sqs.sendMessage(send_msg_request);
    }

    /**
     * This method deletes the message from the global sqs after finishing the process of the message
     *
     * @param sqsUrl  global sqs url
     * @param message message to delete
     */
    private void deleteMessageFromGlobalSqs(String sqsUrl, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(sqsUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        awsManager.sqs.deleteMessage(deleteRequest);
    }

    /**
     * This method Checks How many workers with the tag Worker are running
     *
     * @return the number of active workers
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
     * This returns the number of active workers
     *
     * @return the number of active workers
     */
    public int checkActiveWorkers() {
        return checkHowManyWorkersRunning();
    }


    /**
     * This method is used to create the workers with a given script and number of workers to crate
     * @param numOfWorkers the number of workers to create
     */
    private void createWorkers(int numOfWorkers) {
        String managerScript = "#!/bin/bash\n" +
                "sudo yum update -y \n" +
                "curl -s \"https://get.sdkman.io\" | bash\n" +
                "source \"$HOME/.sdkman/bin/sdkman-init.sh\"\n" +
                "sdk install java 17.0.1-open\n" +
                "sdk use java 17.0.1-open\n" +
                "echo 'Creating directory WorkerFiles'\n" +
                "sudo mkdir WorkersPath\n" +
                "echo 'Downloading WorkerJar from S3 bucket'\n" +
                "sudo aws s3 cp s3://" + awsManager.jarsBucket + "/" + awsManager.workerTag +" ./WorkersPath/" + "WorkerJar.jar" +"\n"+
                "echo 'Running WorkerJar program'\n" +
                "java -jar /WorkersPath/WorkerJar.jar" +"\n";
        createWorkers(managerScript, numOfWorkers);
    }

    /**
     * This method creates a Workers ec2 instances with a tag "Name" "Worker"
     *
     * @param script is a bash script for the manager to run
     * @param numOfWorkers the number of workers to create
     */
    public void createWorkers(String script, int numOfWorkers) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(awsManager.ami)
                .maxCount(numOfWorkers)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();
        RunInstancesResponse response = awsManager.ec2.runInstances(runRequest);

        Collection<String> instanceIDs = response.instances().stream().map(Instance::instanceId).collect(Collectors.toList());

        Tag tag = Tag.builder()
                .key("Name")
                .value("Worker")
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceIDs)
                .tags(tag)
                .build();

        try {
            awsManager.ec2.createTags(tagRequest);
            System.out.printf(
                    "[Debugger logs] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceIDs, awsManager.ami);

        } catch (Ec2Exception e) {
            System.out.println("[Debugger logs] Failed to create an EC2 instance");
            System.err.println("[ERROR] " + e.getMessage());
        }
    }

}
