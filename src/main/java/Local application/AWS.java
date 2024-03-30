import com.amazonaws.services.s3.AmazonS3Client;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;

public class AWS {
    public final S3Client s3;
    public final SqsClient sqs;
    public final Ec2Client ec2;

    public String globalSQS;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    public String bucketName = "bucket" + System.currentTimeMillis();

    public String answersBucket = "bucket" + "outputfilesbucket";

    public String jarsBucket = "bucket" + "jarsbucket123";

    public String managerTag = "ManagerJar.jar";
    public String workerTag = "Worker.jar";

    public static String ami = "ami-00e95a9222311e8ed";

    public String managerInstanceId = "";

    //command line arrguments
    public List<String> inputFiles;
    public String outputFile;
    public String n;

    public String terminate;





    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region2).build();
        ec2 = Ec2Client.builder().region(region2).build();
        globalSQS = "globalSQS";
        inputFiles = new ArrayList<>();
        outputFile = "";
        n = "";
        terminate = "no";
    }

    public static AWS getInstance() {
        return instance;
    }

}