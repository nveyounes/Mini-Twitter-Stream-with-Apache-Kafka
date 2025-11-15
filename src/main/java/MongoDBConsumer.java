import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document; // Import MongoDB's Document class

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongoDBConsumer {

    // Regex to get the inner message
    private static final Pattern MESSAGE_PATTERN = Pattern.compile("\"message\":\"(.*?)\"");

    public static void main(String[] args) {

        // --- 1. MongoDB Setup ---
        // Use the default connection string for a local server
        String mongoUri = "mongodb://localhost:27017";

        // This try-with-resources block declares and auto-closes the mongoClient.
        // This solves the "effectively final" error.
        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {

            MongoDatabase database = mongoClient.getDatabase("bigdata_project"); // DB name
            MongoCollection<Document> collection = database.getCollection("tweets"); // Collection name
            System.out.println("--- Successfully connected to MongoDB ---");

            // --- 2. Kafka Consumer Setup (now nested inside) ---
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "mongodb-consumer-group-1"); // New group ID
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // This nested try-with-resources auto-closes the consumer
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

                consumer.subscribe(Collections.singletonList("tweets-input"));
                System.out.println("--- MongoDBConsumer started, listening for messages... ---");

                // --- 3. Poll and Insert Loop ---
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                    if (records.isEmpty()) {
                        continue;
                    }

                    for (ConsumerRecord<String, String> record : records) {
                        String jsonValue = record.value();
                        Matcher matcher = MESSAGE_PATTERN.matcher(jsonValue);

                        if (matcher.find()) {
                            String innerMessage = matcher.group(1);

                            // --- Parse fields from the inner message ---
                            Document tweetDocument = parseTweetToDocument(innerMessage);

                            if (tweetDocument != null) {
                                // --- 4. Insert into MongoDB ---
                                collection.insertOne(tweetDocument);
                                System.out.println("Inserted tweet for user: " + tweetDocument.getString("user_id"));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in Kafka consumer loop: " + e.getMessage());
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.err.println("Error connecting to MongoDB: " + e.getMessage());
            e.printStackTrace();
            // If MongoDB connection fails, the program will exit.
        }
    }

    /**
     * Helper method to parse the inner message string into a MongoDB Document.
     * (This implements the parsing/mapping required by the bonus question)
     */
    private static Document parseTweetToDocument(String innerMessage) {
        try {
            Document doc = new Document();
            String[] fields = innerMessage.split(";\\s*");

            for (String field : fields) {
                String[] parts = field.split("=", 2);
                if (parts.length == 2) {
                    // Add key/value pair to the BSON document
                    doc.append(parts[0].trim(), parts[1].trim());
                }
            }
            // Ensure we have the required fields
            if (doc.containsKey("user_id") && doc.containsKey("tweet_text")) {
                return doc;
            }
        } catch (Exception e) {
            System.err.println("Failed to parse message string: " + innerMessage);
        }
        return null;
    }
}