import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;

/**
 * This class responsible for delegating tasks to the ActionThreads
 * The tasks are received via the global SQS
 */
public class ListeningThread implements Runnable{
    final private AWSManager awsManager;
    final private String globalSQSURL;
    public ListeningThread (String globalSQSURL){
        awsManager = AWSManager.getInstance();
        this.globalSQSURL = globalSQSURL;
    }
    @Override
    public void run() {
        while (!awsManager.terminate.get()){
            //listen to the sqs.
            List<Message> requests = GlobalSQSMessages();
            if(requests.isEmpty()) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    System.out.println("[Debugger logs] Listening Thread woke up from sleep");
                }
            }
            else {
                for (Message curr : requests) {
                    //Once receive a message push the "local sqs url" to the thread localsqs queue.
                    awsManager.QueueOfRequets.offer(curr);
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                System.out.println("[Debugger logs] Listening Thread woke up from sleep");
            }
        }
    }

    /**
     * This method gets tasks from the local applications through the global SQS
     * @return list of messages available in the global sqs
     */
    private List<Message> GlobalSQSMessages(){
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(globalSQSURL)
                .messageAttributeNames("All")
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = awsManager.sqs.receiveMessage(receiveRequest).messages();
        return messages;
    }

}
