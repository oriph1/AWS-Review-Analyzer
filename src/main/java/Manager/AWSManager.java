import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Queue;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AWSManager {
    public final S3Client s3;
    public final SqsClient sqs;
    public final Ec2Client ec2;

    public String globalSQS;

    public String managerToWorkerSQS;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWSManager instance = new AWSManager();

    //Bucket for the output Files from the manager
    public String bucketName = "bucket" + "outputfilesbucket";


    //Name of the global sqs from the manager to the workers
    public String MassagesReciverSQS;

    //Queue of requests for the Action threads
    public ConcurrentLinkedQueue<Message> QueueOfRequets;

    //Concurrent hash map of <key=sqslocalurl, value=number of reviews> for the Receiver thread.
    public ConcurrentHashMap<String, Integer> MapOfReviews;

    //Concurrent hash map of the localSQSUrl as a key and the file name as a value
    ConcurrentHashMap<String, String> MapOfNameFiles = new ConcurrentHashMap<>();

    public AtomicBoolean terminate;

    //Global counter for creating the files
    public AtomicInteger filesCounter;

    //counter for the number of workers.
    public AtomicInteger WorkersCounter;

    public String managerTag = "Manager.jar";
    public String workerTag = "WorkerJar.jar";

    public String ami = "ami-00e95a9222311e8ed";

    public String jarsBucket = "bucket" + "jarsbucket123";

    final public Object updateWorkersLock = new Object();

    final public Object receivingMessagesFromWorkers = new Object();
    final public Object createFilesLock = new Object();
    final public Object NumOfReviewsLock = new Object();


    private AWSManager() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region2).build();
        ec2 = Ec2Client.builder().region(region2).build();
        globalSQS = "globalSQS";
        managerToWorkerSQS = "managerToWorkerSQS";
        MassagesReciverSQS = "MassagesReceiverSQS";
        QueueOfRequets = new ConcurrentLinkedQueue<>();
        MapOfReviews = new ConcurrentHashMap<>();
        terminate = new AtomicBoolean(false);
        filesCounter = new AtomicInteger(1);
        WorkersCounter = new AtomicInteger(0);

    }

    public static AWSManager getInstance() {
        return instance;
    }

}