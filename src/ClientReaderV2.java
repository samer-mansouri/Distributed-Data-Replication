import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class ClientReaderV2 {
    private static final String READ_ALL_EXCHANGE = "read_all_exchange";
    private static final String READ_ALL_RESPONSE_QUEUE = "read_all_response_queue";
    private static final int NUM_REPLICAS = 3;

    public static void main(String[] args) {
        ClientReaderGUI gui = new ClientReaderGUI(); // Start GUI
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("user");
        factory.setPassword("password");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Declare the exchange and the response queue
            channel.exchangeDeclare(READ_ALL_EXCHANGE, BuiltinExchangeType.FANOUT);
            String queueName = channel.queueDeclare(READ_ALL_RESPONSE_QUEUE, false, false, false, null).getQueue();
            channel.queueBind(queueName, READ_ALL_EXCHANGE, "");

            // Publish a message to request data
            String message = "Read All";
            channel.basicPublish(READ_ALL_EXCHANGE, "", null, message.getBytes());
            System.out.println(" [x] Sent 'Read All' request to replicas");

            // Consume responses
            Map<String, Integer> lineCountMap = new HashMap<>();
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String response = new String(delivery.getBody(), "UTF-8");
                updateLineCountMap(response, lineCountMap);
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

            // Give some time to receive messages
            Thread.sleep(500);  // Increase or decrease based on your setup

            // Process received messages and update GUI
            List<String> majorityLines = getMajorityLines(lineCountMap);
            gui.displayLines(majorityLines);

        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void updateLineCountMap(String line, Map<String, Integer> lineCountMap) {
        lineCountMap.put(line, lineCountMap.getOrDefault(line, 0) + 1);
    }

    private static List<String> getMajorityLines(Map<String, Integer> lineCountMap) {
        List<String> majorityLines = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : lineCountMap.entrySet()) {
            if (entry.getValue() > NUM_REPLICAS / 2) {
                majorityLines.add(entry.getKey());
            }
        }
        return majorityLines;
    }
}
