import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.utils.Pair;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.Integer.parseInt;

public class LocalApplication {
    final static AWS aws = AWS.getInstance();
    private static final String QUEUE_NAME = "LocalApplicationSQS" + new Date().getTime();

    public static void main(String[] args) {
        //Parsing arguments form command line arguments (String[] args)
        extractArguments(args);
//        int numOfMessages = 50;
//        aws.n = String.valueOf(numOfMessages);
//        aws.terminate = "terminate";

        //Check if the global sqs is created. If not create it
        String globalSQSURL = checkGlobalSQS();

        //Upload the files to the s3
        //Step 1: Create bucket
        createBucketIfNotExists(aws.bucketName);
        //Step 2: Upload files to the bucket
//        List<String> filePaths = List.of("input1.txt");
//        uploadInputFilesToBucket(aws.bucketName, filePaths);
        uploadInputFilesToBucket(aws.bucketName, aws.inputFiles);

        //Create a local sqs for receiving an answer from the manager
        String LocalSQSURL = createLocalSQS();

        //Upload Jars to S3
//        createBucketIfNotExists(aws.jarsBucket);
//        uploadJarsToBucket();

        //Create a Manager if not exist.
        createManagerIfNotExists();

        // send to the global sqs queue with the local sqs url
        sendToGlobalSQS(globalSQSURL, LocalSQSURL, "combinedFiles.txt");

        //Listen to the sqs for a message from the manager that the work is done
        receiveMassagesFromSQS(LocalSQSURL);

        //Delete the local application SQS
        deleteLocalSQS(LocalSQSURL);

        ///Delete the bucket
        deleteBucketAndObjects(aws.bucketName, true);

//        deleteBucketAndObjects(aws.answersBucket, false);
    }

    /**
     * This method extract arguments from the command line arguments to the app;
     *
     * @param args  command line arguments
     */
    private static void extractArguments(String[] args) {
        if (args.length < 3) {
            System.out.println("[ERROR] Missing command line arguments.");
            System.exit(1);
        }
        int i = 0;
        while (i < args.length && args[i].startsWith("input")){
            aws.inputFiles.add(args[i]);
            i++;
        }
        //If there is no output file name and n
        if (i > args.length -2){
            System.out.println("[ERROR] command line arguments are missing output file name or n");
            System.exit(1);
        }
        aws.outputFile = args[i];
        i++;
        aws.n = args[i];
        i++;
        if (i == args.length -1){
            aws.terminate = args[i];
        }
    }
    /**
     * This method check if the global sqs for sending messages to the manager exist. if not it creates one
     *
     * @return The global SQS Url
     */
    private static String checkGlobalSQS() {
        String globalQueueUrl;
        try {
            // Check if the global SQS queue exists
            GetQueueUrlResponse getQueueUrlResponse = aws.sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(aws.globalSQS)
                    .build());

            globalQueueUrl = getQueueUrlResponse.queueUrl();

        } catch (QueueDoesNotExistException e) {
            // If the global SQS queue does not exist, create one
            CreateQueueResponse createQueueResponse = aws.sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(aws.globalSQS)
                    .build());

            globalQueueUrl = createQueueResponse.queueUrl();
        }
        return globalQueueUrl;
    }

    /**
     * This method Creates a unique bucket for the local application
     *
     * @param bucketName the name of the bucket
     */
    private static void createBucketIfNotExists(String bucketName) {
        try {
            aws.s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            aws.s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.printf("[Debugger log] Failed Create bucket: %s, error message: %s %n", bucketName, e.getMessage());
        }
    }

    /**
     * This method uploads the inputFiles to the bucket of the Local application.
     *
     * @param bucketName the name of the bucket to upload the files to.
     * @param files      List of files to merge to one and upload to s3
     */
    private static void uploadInputFilesToBucket(String bucketName, List<String> files) {
        String combinedFileName = "combinedFiles.txt";
        mergeFiles(files, combinedFileName);
        uploadFileToS3(bucketName, combinedFileName);
    }

    /**
     * This method merge all the inputFiles to one file.
     *
     * @param files            List of files to merge.
     * @param combinedFileName the name of the combined file.
     */
    private static void mergeFiles(List<String> files, String combinedFileName) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(combinedFileName))) {
            for (String file : files) {
                Path filePath = Paths.get(file);
                List<String> lines = Files.readAllLines(filePath);
                for (String line : lines) {
                    writer.println(line);
                }
            }
        } catch (IOException e) {
            System.out.println("[Debugger logs] Failed to merge files of the local application");
        }
    }

    /**
     * This method upload a file to a bucket in S3
     *
     * @param bucketName The bucket for uploading the file to.
     * @param fileName   the file name
     */
    private static void uploadFileToS3(String bucketName, String fileName) {
        Path filePath = Paths.get(fileName);
        String key = filePath.getFileName().toString();
        aws.s3.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(), RequestBody.fromFile(filePath));
    }

    /**
     * This method creates sqs for receiving the answer from the manager. Each local application has a unique LocalSQS
     *
     * @return The local SQS url
     */
    private static String createLocalSQS() {
        String localQueueUrl;
        CreateQueueResponse createQueueResponse = aws.sqs.createQueue(CreateQueueRequest.builder()
                .queueName(QUEUE_NAME)
                .build());
        localQueueUrl = createQueueResponse.queueUrl();
        return localQueueUrl;
    }

    /**
     * This method Uploads the jar files for the manager and the workers to s3.
     */
    private static void uploadJarsToBucket() {
        uploadFileToS3(aws.jarsBucket, "ManagerJar.jar");
        System.out.println("[Debugger logs] Upload Manager success");
        uploadFileToS3(aws.jarsBucket, "WorkerJar.jar");
        System.out.println("[Debugger logs] Upload Worker success");
    }

    /**
     * This method Check if the Manager exist,if not, It creates a manager with the appropriate script.
     */
    private static void createManagerIfNotExists() {
        if (!checkIfManagerExist()) {
            String managerScript = "#!/bin/bash\n" +
                    "sudo yum update -y \n" +
                    "curl -s \"https://get.sdkman.io\" | bash\n" +
                    "source \"$HOME/.sdkman/bin/sdkman-init.sh\"\n" +
                    "sdk install java 17.0.1-open\n" +
                    "sdk use java 17.0.1-open\n" +
                    "echo 'Creating directory ManagerFiles'\n" +
                    "sudo mkdir ManagerPath\n" +
                    "echo 'Downloading ManagerJar from S3 bucket'\n" +
                    "sudo aws s3 cp s3://" + aws.jarsBucket + "/" + aws.managerTag +" ./ManagerPath/" + "ManagerJar.jar" +"\n"+
                    "echo 'Running ManagerJar program'\n" +
                    "java -jar /ManagerPath/ManagerJar.jar" + "\n";
            createManager(managerScript);
        }
    }

    /**
     * This method Checks if the Manager ec2 instance exist and runs.
     *
     * @return true if it exists, otherwise false
     */
    private static boolean checkIfManagerExist() {
        String nextToken = null;

        do {
            DescribeInstancesRequest request = DescribeInstancesRequest
                    .builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = aws.ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.state().name().toString().equals("running")) {
                        for (software.amazon.awssdk.services.ec2.model.Tag tag : instance.tags()) {
                            if (tag.key().equals("Name") && tag.value().equals("Manager")) {
                                return true;
                            }
                        }
                    }
                }
            }
            nextToken = response.nextToken();

        } while (nextToken != null);

        return false;
    }

    /**
     * This method creates a Manager ec2 instance with a tag "Name" "Manager" and saves the instance id
     * in aws.managerInstanceId
     *
     * @param script is a bash script for the manager to run
     */
    public static void createManager(String script) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(aws.ami)
                .maxCount(1)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();
        RunInstancesResponse response = aws.ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value("Manager")
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            aws.ec2.createTags(tagRequest);
            System.out.printf(
                    "[Debugger logs] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, aws.ami);

        } catch (Ec2Exception e) {
            System.out.println("[Debugger logs] Failed to create an EC2 instance");
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        aws.managerInstanceId = instanceId;
    }

    /**
     * This method Sends a message to the Manager via the global SQS.
     * The message composed of the local application sqs url, and the attributes "Bucket" where the "File" is saved and
     * "n" as the number of reviews per worker
     *
     * @param globalSQSUrl the global SQS url
     * @param localSQSUrl  the local application SQS url
     * @param fileName     the name of the file in the bucket
     */
    private static void sendToGlobalSQS(String globalSQSUrl, String localSQSUrl, String fileName) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(globalSQSUrl)
                .messageBody(localSQSUrl)
                .messageAttributes(
                        Map.of(
                                "Bucket", MessageAttributeValue.builder().dataType("String").stringValue(aws.bucketName).build(),
                                "File", MessageAttributeValue.builder().dataType("String").stringValue(fileName).build(),
                                "n", MessageAttributeValue.builder().dataType("String").stringValue(aws.n).build(),
                                "Terminate", MessageAttributeValue.builder().dataType("String").stringValue(aws.terminate).build()
                        )
                )
                .delaySeconds(20)
                .build();
        aws.sqs.sendMessage(send_msg_request);
    }

    /**
     * This method Check every 30 seconds the local application sqs for messages from the manager.
     * After that it process the file answer, delete the message from the local application sqs
     *
     * @param localSQSURL the local application SQS url
     */
    private static void receiveMassagesFromSQS(String localSQSURL) {
        List<Message> messages = new ArrayList<>();
        boolean breakTheLoop = false;
        while (!breakTheLoop) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(localSQSURL)
                    .waitTimeSeconds(20)
                    .build();
            messages = aws.sqs.receiveMessage(receiveRequest).messages();
            if (!messages.isEmpty()) {
                breakTheLoop = true;
            } else {
                try {
                    Thread.sleep(20000); // Sleep for 30 seconds
                } catch (InterruptedException e) {
                    System.out.println("[Debugger logs] Failure when trying to put the thread to sleep");
                }
            }
        }
        for (Message message : messages) {
            String fileName = message.body();
            processFiles(fileName);
            System.out.println("[Debugger logs] OutputFile was created successfully. search for: output.html ");
            //Delete the message
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(localSQSURL)
                    .receiptHandle(message.receiptHandle())
                    .build();
            aws.sqs.deleteMessage(deleteRequest);
        }
    }

    /**
     * This method process the answer from the manager.
     * It reads the output file and create the html file.
     * The name of the output file is "output.html"
     * @param fileName the file name we need to extract and process
     */
    private static void processFiles(String fileName) {
        String outputFile = aws.outputFile+".html";
        try {
            // Create request to get the object
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(aws.answersBucket)
                    .key(fileName)
                    .build();
            // Get the S3 object
            ResponseInputStream<GetObjectResponse> s3Object = aws.s3.getObject(getObjectRequest);

            // Get the content of the object as an InputStream
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object))) {
                BufferedWriter writerToHtml = new BufferedWriter(new FileWriter(outputFile));
                writerToHtml.write("<html><head><title>Review Analysis</title></head><body> <h1>Review Analysis</h1>");
                // Process each line
                String line;
                String sentimentColor = "";
                String link = "";
                String entities = "";
                String sarcasm = "";
                StringBuilder reviewResult = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        // Empty line, add it to the HTML file
                        writerToHtml.write("<br/>");
                    } else if (line.startsWith("Sentiment:")) {
                        int sentiment = Integer.parseInt(line.substring(line.indexOf(":") + 2).trim());
                        sentimentColor = getColor(sentiment);
                    } else if (line.startsWith("Link:")) {
                        link = line.substring(line.indexOf(":") + 2).trim();
                    } else if (line.startsWith("Entities:")) {
                        entities = line.substring(line.indexOf("[")).trim();
                    } else if (line.startsWith("Sarcasm:")) {
                        sarcasm = line.substring(line.indexOf(":") + 2).trim();
                        // Generate HTML content for this review
                        writerToHtml.write("<div>");
                        writerToHtml.write("<p>Link: <a href=\"" + link + "\" style=\"color:" + sentimentColor + "\">" + link + "</a></p>");
                        writerToHtml.write("<p>Entities: " + entities.replace(", ", ",") + "</p>");
                        writerToHtml.write("<p>Sarcasm: " + sarcasm + "</p>");
                        writerToHtml.write("</div>");
                        // Reset variables for the next review
                        sentimentColor = "";
                        link = "";
                        entities = "";
                        sarcasm = "";
                    }
                }
                writerToHtml.write("</body></html>");
                writerToHtml.close();
                System.out.println("HTML file generated successfully!");
            } catch (IOException e) {
                System.out.printf("[Debugger log] Failed to read lines from the file, error message: %s %n", e.getMessage());
            }
        } catch (Exception e) {
            System.out.printf("[Debugger log] Failed to open the file answer from s3, error message: %s %n", e.getMessage());
        }
    }
    /**
     * This method decide the color of the link based of the sentiment
     * @param sentiment the sentiment of the given review
     */
    public static String getColor(int sentiment) {
        if (sentiment == 1) {
            return "darkred";
        } else if (sentiment == 2) {
            return "red";
        } else if (sentiment == 3) {
            return "black";
        } else if (sentiment == 4) {
            return "lightgreen";
        } else {
            return "darkgreen";
        }
    }


    /**
     * This method deletes the local application SQS after finishing the process of the messager
     *
     * @param sqsUrl the local application SQS url to be deleted
     */
    private static void deleteLocalSQS(String sqsUrl) {
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(sqsUrl)
                .build();

        // Delete the SQS queue
        DeleteQueueResponse deleteQueueResponse = aws.sqs.deleteQueue(deleteQueueRequest);

        // Check the response status
        if (deleteQueueResponse.sdkHttpResponse().isSuccessful()) {
            System.out.printf("[Debugger log] Delete of the local application sqs was successful!. The sqs url:%s %n", sqsUrl);
        } else {
            System.out.printf("[Debugger log] Delete of the local application sqs failed!. The sqs url:%s %n", sqsUrl);
        }
    }


    /**
     * This method deletes the local application Bucket after finishing the process of the messager
     *
     * @param bucketName the local application bucketName to be deleted
     */
    private static void deleteBucketAndObjects(String bucketName, boolean deletebucket) {
        boolean deletionSuccessful = false;
        // List all objects in the bucket
        aws.s3.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .stream()
                .flatMap(r -> r.contents().stream())
                .forEach(object -> {
                    // Delete each object
                    aws.s3.deleteObject(builder -> builder.bucket(bucketName).key(object.key()));
                });

        try {
            if (deletebucket) {
                aws.s3.deleteBucket(builder -> builder.bucket(bucketName).build());
                System.out.printf("[Debugger log] Delete of the local application bucket was successful!. The bucket name :%s %n", bucketName);
            }
        } catch (S3Exception e) {
            System.out.printf("[Debugger log] Delete of the local application bucket failed!. The bucket name :%s %n", bucketName);
            System.err.println("Error deleting bucket: " + e.getMessage());
        }
    }



}
