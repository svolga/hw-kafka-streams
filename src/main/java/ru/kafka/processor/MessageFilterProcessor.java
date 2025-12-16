package ru.kafka.processor;

import java.util.regex.Pattern;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import ru.kafka.domain.Message;

public class MessageFilterProcessor implements ValueTransformer<Message, Message> {

  private KeyValueStore<String, String> forbiddenWordsStore;

  @Override
  public void init(ProcessorContext context) {
    this.forbiddenWordsStore = context.getStateStore("forbidden-words-store");
  }

  @Override
  public Message transform(Message message) {
    if (message == null || message.text() == null) {
      return message;
    }

    // Создаем копию контента для модификации
    String censoredContent = message.text();

    // Получаем все запрещенные слова из хранилища
    try (KeyValueIterator<String, String> iter = forbiddenWordsStore.all()) {
      while (iter.hasNext()) {

        KeyValue<String, String> entry = iter.next();

        String wordValue = getStringValue(entry.value);
        if ("ban".equals(wordValue)) {
          censoredContent = censoredContent.replaceAll(
              "(?i)" + Pattern.quote(entry.key),
              "*".repeat(entry.key.length())
          );
        }
      }
    }

    return new Message(censoredContent, message.receiver());
  }

  private String getStringValue(Object value) {
    if (value == null) return null;
    if (value instanceof ValueAndTimestamp) {
      return ((ValueAndTimestamp<?>) value).value().toString();
    }
    return value.toString();
  }

  @Override
  public void close() {
  }
}