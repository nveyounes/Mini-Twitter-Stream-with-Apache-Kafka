import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetFilterStream {

    // Regex to get the inner message, just like the Consumer
    private static final Pattern MESSAGE_PATTERN = Pattern.compile("\"message\":\"(.*?)\"");
    private static final String INPUT_TOPIC = "tweets-input";
    private static final String OUTPUT_TOPIC = "tweets-kafka-filtered";

    public static void main(String[] args) {

        // 1. Create Stream Properties
        Properties props = new Properties();
        // --- This Application ID must be unique for each stream app ---
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-filter-app-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // --- Specify default serdes (Serializers/Deserializers) ---
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 2. Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // 3. Get the stream from the input topic
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        // 4. --- DEFINE THE PROCESSING LOGIC (Stateless Filter) ---
        KStream<String, String> filteredStream = inputStream.filter((key, jsonValue) -> {
            // We must parse the JSON to inspect the inner message
            Matcher matcher = MESSAGE_PATTERN.matcher(jsonValue);
            if (matcher.find()) {
                String innerMessage = matcher.group(1);

                // --- This is the filter logic required by the project --- [cite: 38]
                return innerMessage.contains("hashtags=kafka");
            }
            // If format is bad, filter it out
            return false;
        });

        // 5. Send the filtered stream to the output topic [cite: 37]
        filteredStream.to(OUTPUT_TOPIC);

        // 6. Build and start the Kafka Stream
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Add a shutdown hook for graceful exit
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("--- Kafka Filter Stream started. Listening... ---");
        streams.start();
    }
}