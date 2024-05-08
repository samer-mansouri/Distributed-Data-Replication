import com.rabbitmq.client.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;

public class Replica extends JFrame {
    private static final String EXCHANGE_NAME = "file_exchange";
    private static final String READ_LAST_EXCHANGE = "read_last_exchange";
    private static final String RESPONSE_QUEUE = "response_queue";
    private static final int NUM_REPLICAS = 3;
    private static final String READ_ALL_EXCHANGE = "read_all_exchange";
    private static final String READ_ALL_RESPONSE_QUEUE = "read_all_response_queue";

    private static DefaultTableModel tableModel;
    private static JTable table;


    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: java Replica <replica_number>");
            System.exit(1);
        }

        int replicaNumber = Integer.parseInt(args[0]);

        if (replicaNumber < 1 || replicaNumber > NUM_REPLICAS) {
            System.out.println("Replica number must be between 1 and " + NUM_REPLICAS);
            System.exit(1);
        }

        String filePath = "file_replica_" + replicaNumber + ".txt";

        List<String> lines = Files.readAllLines(Paths.get(filePath));
        Object[][] initialData = new Object[lines.size()][2];
        for (int i = 0; i < lines.size(); i++) {
            String[] parts = lines.get(i).split(" ");
            initialData[i] = new Object[]{parts[0], parts[1]};
        }

        JFrame frame = new JFrame("Replica " + replicaNumber);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setLayout(new BorderLayout());

        tableModel = new DefaultTableModel(initialData, new String[]{"Line Number", "Message"});
        table = new JTable(tableModel);
        JScrollPane scrollPane = new JScrollPane(table);
        frame.add(scrollPane, BorderLayout.CENTER);

        frame.setSize(400, 300);
        frame.setVisible(true);


     

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("user");
        factory.setPassword("password");

        try {


            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();


            //1: RECEIVE MESSAGES FROM CLIENTWRITER
            String queueName =  channel.queueDeclare().getQueue();
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            channel.queueBind(queueName, EXCHANGE_NAME, "");

            System.out.println(" [*] Replica " + replicaNumber + " waiting for messages.");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
                List<String> allLines = Files.readAllLines(Paths.get(filePath));
                // writeToTextFile(message, replicaNumber);
                lineInsert(allLines, message, replicaNumber);
                updateTable(filePath); 
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });


            //2: RECEIVE QUERIES FROM CLIENTREADER
            channel.exchangeDeclare(READ_LAST_EXCHANGE, BuiltinExchangeType.FANOUT);
            String readLastQueueName = channel.queueDeclare().getQueue();
            channel.queueBind(readLastQueueName, READ_LAST_EXCHANGE, "");

            channel.queueDeclare(RESPONSE_QUEUE, false, false, false, null);

            DeliverCallback readLastCallback = (consumerTag, delivery) -> {
                System.out.println(" [x] Received 'Read Last' request from ClientReader");

                // Process 'Read Last' request (example: read last line from file)
                String lastLine = readLastLine("file_replica_" + replicaNumber + ".txt");

                // Publish response to response queue
                channel.basicPublish("", RESPONSE_QUEUE, null, lastLine.getBytes("UTF-8"));
            };

            // Consume 'Read Last' requests
            channel.basicConsume(readLastQueueName, true, readLastCallback, consumerTag -> {
            });




            // 3: RECEIVE QUERIES FROM CLIENTREADER FOR READ ALL
            channel.exchangeDeclare(READ_ALL_EXCHANGE, BuiltinExchangeType.FANOUT);
            String readAllQueueName = channel.queueDeclare().getQueue();
            channel.queueBind(readAllQueueName, READ_ALL_EXCHANGE, "");

            // Start consuming 'Read All' requests
            DeliverCallback readAllCallback = (consumerTag, delivery) -> {
                System.out.println(" [x] Received 'Read All' request from ClientReader");

                // Read the entire file and send each line as a separate message to the response queue
                try (BufferedReader reader = new BufferedReader(new FileReader("file_replica_" + replicaNumber + ".txt"))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        channel.basicPublish("", READ_ALL_RESPONSE_QUEUE, null, line.getBytes("UTF-8"));
                        System.out.println(" [x] Sent line to response queue: " + line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println("Error reading file: " + e.getMessage());
                }
            };

            channel.basicConsume(readAllQueueName, true, readAllCallback, consumerTag -> {});



        } catch (IOException | TimeoutException e) {
            System.err.println(" [!] Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static final void lineInsert(List<String> lines, String message, int replicaId) {
        HashMap<Integer, String> map = new HashMap<>();

        // Parsing existing lines to map
        for (String line : lines) {
            try {
                String[] parts = line.split(" ", 2);
                Integer lineNumber = Integer.parseInt(parts[0]);
                if (parts.length > 1) {
                    map.put(lineNumber, parts[1]);
                } else {
                    map.put(lineNumber, ""); // Handle lines without text properly
                }
            } catch (NumberFormatException e) {
                System.err.println("Skipping line due to bad format: " + line);
            }
        }

        // Parsing incoming message
        try {
            String[] parts = message.split(" ", 2);
            Integer lineNumber = Integer.parseInt(parts[0]);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Message format is incorrect. It must be 'line_number text'. Received: " + message);
            }
            map.put(lineNumber, parts[1]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid line number in message: " + message);
            return;
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return;
        }

        // Writing all lines back to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("file_replica_" + replicaId + ".txt", false))) {
            map.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    try {
                        writer.write(entry.getKey() + " " + entry.getValue() + "\n");
                    } catch (IOException e) {
                        System.err.println("Failed to write line: " + entry.getKey() + " " + entry.getValue());
                    }
                });
        } catch (IOException e) {
            System.err.println("Failed to open file for writing: " + e.getMessage());
        }
    }

    public static String readLastLine(String fileName) {
        String lastLine = null;
        try (RandomAccessFile file = new RandomAccessFile(new File(fileName), "r")) {
            long fileLength = file.length();
            if (fileLength == 0) {
                return null; // File is empty
            }
            long pos = fileLength - 2; // Start at the end of the file
            String sb = "";
            boolean foundNewLine = false;
            // Read characters backward until a newline character is found or we reach the beginning of the file
            while (pos >= 0) {
                file.seek(pos);
                char c = (char) file.read();
                if (c == '\n') {
                    // If newline character is found, stop reading
                    foundNewLine = true;
                    break;
                }
                sb = c +sb;
                pos--;
            }
            lastLine = sb;
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error reading file: " + e.getMessage());
        }
        return lastLine;
    }

    private static void updateTable(String filePath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        Object[][] newData = new Object[lines.size()][2];
        for (int i = 0; i < lines.size(); i++) {
            String[] parts = lines.get(i).split(" ");
            if (parts.length >= 2) {
                newData[i] = new Object[]{parts[0], parts[1]};
            } else {
                newData[i] = new Object[]{parts[0], ""}; 
            }
        }
        tableModel.setDataVector(newData, new String[]{"Line Number", "Message"});
    }

}
