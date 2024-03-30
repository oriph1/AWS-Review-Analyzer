import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.concurrent.atomic.AtomicBoolean;

public class AWSWorker {
    public final S3Client s3;
    public final SqsClient sqs;
    public final Ec2Client ec2;



    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWSWorker instance = new AWSWorker();

    public String MassagesReciverSQS;

    public String managerToWorkerSQS;


    public AtomicBoolean terminate;


    private AWSWorker() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region2).build();
        ec2 = Ec2Client.builder().region(region2).build();
        managerToWorkerSQS = "managerToWorkerSQS";
        MassagesReciverSQS = "MassagesReceiverSQS";
        terminate = new AtomicBoolean(false);

    }

    public static AWSWorker getInstance() {
        return instance;
    }

}
