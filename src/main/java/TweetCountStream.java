import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetCountStream {

    // Regex to get the inner message
    private static final Pattern MESSAGE_PATTERN = Pattern.compile("\"message\":\"(.*?)\"");
    // Regex to get the user_id from the inner message
    private static final Pattern USER_ID_PATTERN = Pattern.compile("user_id=([^;]+)");

    private static final String INPUT_TOPIC = "tweets-input";
    private static final String OUTPUT_TOPIC = "tweet-counts-output";

    public static void main(String[] args) {

        // 1. Create Stream Properties
        Properties props = new Properties();
        // --- CRITICAL: This ID must be different from your filter app ---
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-count-app-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 2. Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // 3. Get the stream from the input topic
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        // 4. --- DEFINE THE PROCESSING LOGIC (Stateful Count) ---

        KStream<String, Long> countStream = inputStream
                // --- a. Extract user_id and set it as the new key ---
                // This is the "parsing split" step your project requires (source 112)
                .map((key, jsonValue) -> {
                    String newKey = "UNKNOWN_USER"; // Default if parsing fails
                    Matcher jsonMatcher = MESSAGE_PATTERN.matcher(jsonValue);
                    if (jsonMatcher.find()) {
                        String innerMessage = jsonMatcher.group(1);
                        Matcher userMatcher = USER_ID_PATTERN.matcher(innerMessage);
                        if (userMatcher.find()) {
                            newKey = userMatcher.group(1); // e.g., "u123"
                        }
                    }
                    // We change the key to user_id and pass the original value along
                    return new KeyValue<>(newKey, jsonValue);
                })

                // --- b. Group by the new key (user_id) ---
                .groupByKey() // (source 114)

                // --- c. Define the 30-second window ---
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30))) // (source 46)

                // --- d. Count the occurrences ---
                .count(Materialized.as("tweet-counts-store")) // (source 115)

                // --- e. Convert the windowed stream back to a regular KStream ---
                .toStream()

                // --- f. Format the output for the new topic ---
                // The key is currently (user_id, window_info)
                // We just want (user_id, count)
                .map((windowedKey, count) -> {
                    String userId = windowedKey.key(); // Get the user_id back from the windowed key
                    return new KeyValue<>(userId, count);
                });

        // 5. Send the count stream to the output topic
        // We must specify the Serdes for the Long value
        countStream.to(OUTPUT_TOPIC, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.Long()));

        // 6. Build and start the Kafka Stream
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("--- Kafka Count Stream started. Listening... ---");
        streams.start();
    }
}