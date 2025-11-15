# ISGA Big Data: Mini-Twitter Stream with Apache Kafka

![Java](https://img.shields.io/badge/Java-17-ED8B00?style=for-the-badge&logo=openjdk)
![Kafka](https://img.shields.io/badge/Apache_Kafka-4.1-231F20?style=for-the-badge&logo=apachekafka)
![Maven](https://img.shields.io/badge/Maven-3.9-C71A36?style=for-the-badge&logo=apachemaven)
![MongoDB](https://img.shields.io/badge/MongoDB-5.1-4EA94B?style=for-the-badge&logo=mongodb)

## 📝 Project Overview

This is a mini-project for the **Big Data** course at **ISGA - Marrakech** (2024-2025), under the supervision of **M. SNINEH Sidi Mohamed**.

The objective is to build a simplified, real-time "Mini-Twitter" application to demonstrate the core concepts of Apache Kafka. The project involves ingesting, consuming, and processing a stream of "tweets" using Kafka's Producer/Consumer APIs and the Kafka Streams API.

### Core Objectives

1.  **Ingestion:** A Java producer that generates 100 random tweets and sends them to a Kafka topic.
2.  **Consumption:** A Java consumer that reads from the topic and parses the messages.
3.  **Stateless Processing:** A Kafka Streams application that filters tweets based on a specific hashtag.
4.  **Stateful Processing:** A Kafka Streams application that counts the number of tweets per user in 30-second windows.
5.  **Bonus:** A consumer that stores the raw tweets in a MongoDB collection for permanent storage.

## 🛠️ Tech Stack

* **Java:** Version 17
* **Message Broker:** Apache Kafka 4.1.0 (running in KRaft mode)
* **Stream Processing:** Kafka Streams API
* **Database:** MongoDB Community Edition
* **Build Tool:** Apache Maven

## 🚀 Getting Started

Follow these instructions to set up and run the project locally on a macOS environment.

### Prerequisites

* **Java 17:** Ensure you have Java 17 installed.
    ```bash
    brew install --cask temurin17
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
    ```
* **Apache Kafka:**
    ```bash
    brew install kafka
    ```
* **MongoDB:**
    ```bash
    brew tap mongodb/brew
    brew install mongodb-community
    ```

### ⚙️ Installation & Setup

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/](https://github.com/)[YOUR-USERNAME]/[YOUR-REPOSITORY-NAME].git
    cd [YOUR-REPOSITORY-NAME]
    ```

2.  **Build with Maven:**
    (This will download all the required dependencies from the `pom.xml`)
    ```bash
    mvn clean install
    ```

3.  **Start MongoDB:**
    (This starts the database as a background service)
    ```bash
    brew services start mongodb/brew/mongodb-community
    ```

4.  **Start Apache Kafka (KRaft Mode):**
    You will need two terminals for this.

    * **Terminal 1: Format Storage & Start Server**
        First, generate a cluster ID:
        ```bash
        kafka-storage random-uuid
        ```
        (Copy the ID it outputs)

        Next, format the storage directory (paste your ID):
        ```bash
        kafka-storage format -t YOUR_CLUSTER_ID -c /usr/local/etc/kafka/server.properties --standalone
        ```

        Finally, start the Kafka server. **Leave this terminal running.**
        ```bash
        kafka-server-start /usr/local/etc/kafka/server.properties
        ```

    * **Terminal 2: Create Kafka Topics**
        While the server is running, open a new terminal and create the three required topics:
        ```bash
        kafka-topics --bootstrap-server localhost:9092 --topic tweets-input --create
        kafka-topics --bootstrap-server localhost:9092 --topic tweets-kafka-filtered --create
        kafka-topics --bootstrap-server localhost:9092 --topic tweet-counts-output --create
        ```

Your environment is now ready!

## ▶️ How to Run

All Java classes can be run directly from your IDE (like IntelliJ or VS Code) or using Maven.

### Scenario 1: Simple Produce & Consume (Tasks 1 & 2)

1.  Run the `TweetProducer.java` class. It will send 100 messages and stop.
2.  Run the `TweetConsumer.java` class. It will start, read all 100 messages, parse them, and stop.

### Scenario 2: Stateless Filter Stream (Task 3)

1.  **Run the Stream App:** Run `TweetFilterStream.java`. It will start and stay running.
2.  **Open a Console Consumer:** In a new terminal, start a consumer to watch the *filtered* topic:
    ```bash
    kafka-console-consumer --bootstrap-server localhost:9092 --topic tweets-kafka-filtered
    ```
3.  **Run the Producer:** Run `TweetProducer.java` again.
4.  **Observe:** Watch the console consumer terminal. You will **only** see tweets that contain `hashtags=kafka`.

### Scenario 3: Stateful Count Stream (Task 4)

1.  **Stop other streams** (like the filter) if they are running.
2.  **Run the Stream App:** Run `TweetCountStream.java`. It will start and stay running.
3.  **Open a Console Consumer:** In a new terminal, start a consumer to watch the *counts* topic. This command is special because it deserializes the `Long` value:
    ```bash
    kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic tweet-counts-output \
    --from-beginning \
    --property "print.key=true" \
    --property "key.separator= : " \
    --property "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
    --property "value.deserializer=org.apache.kafka.common.serialization.LongDeserializer"
    ```
4.  **Run the Producer:** Run `TweetProducer.java` again.
5.  **Observe:** After 30 seconds, watch the console consumer terminal. You will see the final counts appear for each user (e.g., `u123 : 21`).

### Scenario 4: MongoDB Storage (Task 5 - Bonus)

1.  **Stop other consumers** if they are running.
2.  **Run the MongoDB App:** Run `MongoDBConsumer.java`. It will connect to MongoDB and start listening.
3.  **Run the Producer:** Run `TweetProducer.java` again.
4.  **Observe:** You will see the `MongoDBConsumer` console print "Inserted tweet for user: ..."
5.  **Verify in Database:** Open a new terminal and use the MongoDB Shell (`mongosh`):
    ```bash
    mongosh
    use bigdata_project;
    db.tweets.countDocuments();
    db.tweets.findOne();
    ```

## 📂 Project Structure
