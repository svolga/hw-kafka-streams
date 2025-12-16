package ru.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kafka.domain.Message;
import ru.kafka.processor.MessageFilterProcessor;
import ru.kafka.serialization.MessageSerdes;
import util.RemoveAll;

public class KafkaStreamApp {

    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "kafka-stream-filter-app";
    private static final String STORE_DIR = "kafka-streams-state-store";
    private static final String BLOCKED_USERS_TOPIC = "blocked-users";
    private static final String FORBIDDEN_WORDS_TOPIC = "forbidden-words";
    private static final String FILTERED_MESSAGES_TOPIC = "filtered-messages";
    private static final String MESSAGES_TOPIC = "messages";
    private static final String BLOCKED_USERS_STORE = "blocked-users-store";
    private static final String FORBIDDEN_WORDS_STORE = "forbidden-words-store";

    private static final String[] FORBIDDEN_WORDS = {"Политика", "1C", "Алкоголь"};
    private static final String[] BLOCKED_USERS = {"login1:login2", "login1:login3", "login2:login4"};

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamApp.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static void clearStateDir() {

        File fileStateDir = new File("kafka-streams-state-store");
        File fileStateTmpDir = new File(System.getProperty("java.io.tmpdir") + "/kafka-streams");

        try {
            FileUtils.deleteDirectory(fileStateDir);
            FileUtils.deleteDirectory(fileStateTmpDir);
        } catch (IOException e) {
            log.error("Ошибка удаления state каталога {}", e.getMessage());
        }
    }

    public static void main(String[] args) {

        try {
            // Удаляем топики
            RemoveAll.removeAll(BOOTSTRAP_SERVERS);

            // Пересоздаем каталоги для store
            clearStateDir();

            // Создаем топики
            createTopics();

            // Строим топологию
            KafkaStreams streams = buildStreamsApplication();
            final CountDownLatch latch = new CountDownLatch(1);

            // Обработка завершения работы
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Завершение работы приложения...");
                streams.close();
                latch.countDown();
            }));

            // Запускаем Kafka Streams
            streams.start();
            log.info("Приложение Sales Processor запущено");

            // Ждем, пока приложение полностью запустится
            waitUntilKafkaStreamsIsRunning(streams);

            // Отправляем тестовые данные
            createBlockedUsers();
            createForbiddenWords();
            createMessages();

            // Даем время на обработку данных
            log.info("Ожидание обработки данных...");
            TimeUnit.SECONDS.sleep(5);

            // Ожидаем сигнала завершения
            latch.await();
        } catch (Exception e) {
            log.error("Ошибка при запуске приложения -> {} ", e.getMessage());
            System.exit(1);
        }

    }

    private static KafkaStreams buildStreamsApplication() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, STORE_DIR);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topology topology = buildTopology();
        log.info("Топология:\n{}", topology.describe());

        return new KafkaStreams(topology, props);
    }

    private static Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> blockedUsers = builder.globalTable(
                BLOCKED_USERS_TOPIC,
                Materialized.<String, String>as(Stores.persistentKeyValueStore(BLOCKED_USERS_STORE))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        builder.globalTable(
                FORBIDDEN_WORDS_TOPIC,
                Materialized.<String, String>as(Stores.persistentKeyValueStore(FORBIDDEN_WORDS_STORE))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        builder.stream(MESSAGES_TOPIC, Consumed.with(Serdes.String(), new MessageSerdes()))
                .peek((key, value) -> log.info("Получено сообщение: key={}, value={}", key, value))
                // Фильтрация заблокированных пользователей
                .leftJoin(blockedUsers,
                        (key, message) -> message.receiver() + ":" + key, // Формат: "receiver:sender"
                        (message, blockReason) -> {
                            if (blockReason != null) {
                                return null;
                            }
                            return message;
                        }
                )
                .filter((key, message) -> message != null)
                .transformValues(MessageFilterProcessor::new)
                .peek((key, value) -> log.info("Фильтрация сообщений: key={}, value={}", key, value))
                .to(FILTERED_MESSAGES_TOPIC, Produced.with(Serdes.String(), new MessageSerdes()));

        return builder.build();
    }

    private static void createTopics() {
        Properties adminProps = new Properties();
        adminProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            Set<String> alreadyCreatedTopics = adminClient.listTopics().names().get();

            createTopic(adminClient, alreadyCreatedTopics, MESSAGES_TOPIC);
            createTopic(adminClient, alreadyCreatedTopics, FILTERED_MESSAGES_TOPIC);
            createTopic(adminClient, alreadyCreatedTopics, BLOCKED_USERS_TOPIC);
            createTopic(adminClient, alreadyCreatedTopics, FORBIDDEN_WORDS_TOPIC);
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Ошибка при создании топиков (возможно они уже существуют): " + e.getMessage());
        }
    }

    private static void createTopic(AdminClient adminClient, Set<String> alreadyCreatedTopics,
                                    String topicName)
            throws InterruptedException, ExecutionException {
        if (!alreadyCreatedTopics.contains(topicName)) {
            NewTopic topic = new NewTopic(topicName, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(topic)).all().get();
            log.info("Топик {} создан", topicName);
        }
    }

    private static void createBlockedUsers() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String blockedUser : Set.of(BLOCKED_USERS)) {
                producer.send(new ProducerRecord<>(BLOCKED_USERS_TOPIC, blockedUser, "blocked"));
            }
            producer.flush();
            log.info("Данные о заблокированных пользователях отправлены");
        }
    }

    private static void createForbiddenWords() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String word : Set.of(FORBIDDEN_WORDS)) {
                producer.send(new ProducerRecord<>(FORBIDDEN_WORDS_TOPIC, word, "ban"));
            }
            producer.flush();
            log.info("Запрещенные слова отправлены");
        }
    }

    private static void createMessages() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            sendMessage(producer, "login4", new Message("Java", "login1"));
            sendMessage(producer, "login2", new Message("Spring", "login1"));
            sendMessage(producer, "login3", new Message("1С", "login1"));
            sendMessage(producer, "login5", new Message("Политика React", "login1"));

            producer.flush();
            log.info("Все сообщения успешно отправлены");
        } catch (Exception e) {
            log.error("Ошибка при отправке сообщений: -> {} ", e.getMessage());
        }
    }

    private static void sendMessage(Producer<String, String> producer,
                                    String sender, Message message) {
        try {
            String jsonMessage = objectMapper.writeValueAsString(message);
            producer.send(new ProducerRecord<>(MESSAGES_TOPIC, sender, jsonMessage),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Ошибка при отправке: " + exception.getMessage());
                        } else {
                            System.out.printf("Отправлено: key=%s, value=%s, partition=%d, offset=%d%n",
                                    sender, jsonMessage, metadata.partition(), metadata.offset());
                        }
                    });
        } catch (Exception e) {
            System.err.println("Ошибка сериализации сообщения: " + e.getMessage());
        }
    }

    /**
     * Ожидает, пока Kafka Streams перейдет в состояние RUNNING
     */
    private static void waitUntilKafkaStreamsIsRunning(KafkaStreams streams) throws Exception {
        int maxRetries = 10;
        int retryIntervalMs = 1000;
        int attempt = 0;

        while (attempt < maxRetries) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                log.info("Kafka Streams успешно запущен");
                return;
            }

            log.info("Ожидание запуска Kafka Streams... Текущее состояние -> {}", streams.state());
            Thread.sleep(retryIntervalMs);
            attempt++;
        }

        throw new RuntimeException("Превышено время ожидания запуска Kafka Streams");
    }

}