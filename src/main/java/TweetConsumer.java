import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetConsumer {

    // --- We use a simple regex to parse the JSON value ---
    // This looks for "message":"<the_content_we_want>"
    private static final Pattern MESSAGE_PATTERN = Pattern.compile("\"message\":\"(.*?)\"");

    public static void main(String[] args) {

        // 1. Create Consumer Properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // --- These properties match your project requirements ---
        // Use StringDeserializer for both key and value
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // --- Consumer Group ---
        // A group ID is required for a consumer
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "twitter-consumer-group-1");

        // This makes the consumer read messages from the very beginning of the topic
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Create the Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 3. Subscribe to the topic
        String topic = "tweets-input";
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("--- Consumer started, listening for messages... ---");

        try {
            // 4. Poll for new data
            while (true) {
                // Poll for records, waiting up to 1 second
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty()) {
                    // If no records after 1 sec, just continue polling
                    continue;
                }

                // --- Process each record ---
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("\n--- NEW TWEET RECEIVED ---");

                    // The full value is: {"message":"user_id=...;..."}
                    String jsonValue = record.value();

                    // --- 1. Parse the JSON to get the inner string ---
                    Matcher matcher = MESSAGE_PATTERN.matcher(jsonValue);
                    if (matcher.find()) {
                        String innerMessage = matcher.group(1);

                        // --- 2. Parse the inner string using split() as required ---
                        String[] fields = innerMessage.split(";\\s*"); // Split on semicolon and any optional space

                        System.out.println("Raw Message: " + innerMessage);
                        System.out.println("--- Parsed Fields ---");

                        for (String field : fields) {
                            // Split each field (e.g., "user_id=u123") into key and value
                            String[] parts = field.split("=", 2);
                            if (parts.length == 2) {
                                // Print the fields as required by the assignment
                                System.out.println("  " + parts[0].trim() + ": " + parts[1].trim());
                            }
                        }
                    } else {
                        System.out.println("Received message in unexpected format: " + jsonValue);
                    }
                }

                // We've processed the batch, let's stop for this demo.
                // In a real app, you would remove this break.
                // We break here so it doesn't loop forever.
                System.out.println("\n--- Finished processing batch, stopping consumer. ---");
                break;
            }

        } catch (Exception e) {
            System.err.println("Error in consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 5. Close the consumer
            consumer.close();
            System.out.println("--- Consumer closed ---");
        }
    }
}