package util;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Удаление всех созданных топиков
 */
public class RemoveAll {
    private static final Logger log = LoggerFactory.getLogger(RemoveAll.class);

    private static Collection<String> sync(Admin client) throws Exception {
        Collection<String> topics = client.listTopics()
                .listings()
                .get() // вот тот самый переход
                .stream()
                .map(TopicListing::name)
                .toList();

        log.info("External topics: {}", topics);

        client.deleteTopics(topics).all()
                .get(); // а вот еще раз

        log.info("SUCCESS");

        return topics;
    }

    public static void removeAll(String booStrapServers) {
        try (Admin client = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, booStrapServers))) {
            removeAll(client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void removeAll(Admin client) throws Exception {
        Collection<String> topics = sync(client);

        var newTopics = topics.stream().map(t -> new NewTopic(t, 1, (short) 1)).toList();
        var options = new CreateTopicsOptions().validateOnly(true);

        while (true) {
            try {
                client.createTopics(newTopics, options).all().get();
                break;
            } catch (ExecutionException ex) {
                if (ex.getCause() == null || ex.getCause().getClass() != TopicExistsException.class)
                    throw ex;
                Thread.sleep(100);
            }
        }
    }
}

