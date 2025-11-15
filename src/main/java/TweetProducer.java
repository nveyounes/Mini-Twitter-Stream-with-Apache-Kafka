import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TweetProducer {

    public static void main(String[] args) {

        // 1. Create Producer Properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // --- These properties match your project requirements ---
        // Use StringSerializer for both key and value
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // ---

        // 2. Create the Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 3. Define the topic
        String topic = "tweets-input";

        // For generating random data
        Random random = new Random();
        String[] users = {"u123", "u456", "u789", "u101", "u202"};
        String[] hashtags = {"kafka", "java", "bigdata", "spark", "isga"};

        try {
            // --- Generate 100 random tweets ---
            for (int i = 0; i < 100; i++) {

                // --- Build the inner message string ---
                String user = users[random.nextInt(users.length)];
                String hashtag = hashtags[random.nextInt(hashtags.length)];
                String text = "This is a random tweet #" + hashtag + " from user " + user;
                String timestamp = Instant.now().toString();

                String innerMessage = String.format(
                        "user_id=%s; timestamp=%s; tweet_text=%s; hashtags=%s",
                        user, timestamp, text, hashtag
                );

                // --- Build the final JSON as required ---
                // {"message": "user_id=...; timestamp=...; ..."}
                String jsonMessage = "{\"message\":\"" + innerMessage + "\"}";

                // We use the user_id as the Kafka message key. This is good practice
                // as it ensures all messages from the same user go to the same partition.
                // This is critical for the Stateful count later.
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, user, jsonMessage);

                // 4. Send the record
                producer.send(record);

                System.out.println("Sent tweet: " + jsonMessage);

                // Add a small delay
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 500));
            }
        } catch (InterruptedException e) {
            System.err.println("Error during thread sleep: " + e.getMessage());
            Thread.currentThread().interrupt(); // Restore the interrupted status
        } catch (Exception e) {
            System.err.println("Error sending message: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 5. Close the producer
            producer.flush();
            producer.close();
            System.out.println("--- Producer finished sending 100 messages ---");
        }
    }
}