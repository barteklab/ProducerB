package trades;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class ProducerJob implements Job {
    @Override
    public void execute(JobExecutionContext context) {
        log.info("Quartz job started");
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        final Consumer<String, Trade> consumer = createConsumer(dataMap);

        try {
            List<Trade> tradeList = new ArrayList<>();
            Integer possibleDelay = dataMap.getIntegerFromString(Constants.KAFKA_DELAY);
            final ConsumerRecords<String, Trade> consumerRecords = consumer.poll(Duration.ofSeconds(possibleDelay));

            processConsumerRecords(tradeList, consumerRecords, dataMap.getString(Constants.OUTPUT_FILES_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private void processConsumerRecords(List<Trade> tradeList, ConsumerRecords<String, Trade> consumerRecords, String path) throws IOException {
        for (ConsumerRecord<String, Trade> record : consumerRecords) {
            Trade trade = record.value();
            tradeList.add(trade);
            log.info("Consumed trade: {}", trade.getTradeReference());
        }

        List<TradeOutput> tradeOutputList = tradeList.stream()
                .map(trade -> TradeOutput.builder()
                        .tradeReference(trade.getTradeReference())
                        .accountNumber(trade.getAccountNumber())
                        .stockCode(trade.getStockCode())
                        .quantity(trade.getQuantity())
                        .currency(trade.getCurrency())
                        .price(trade.getPrice())
                        .broker(trade.getBroker())
                        .amount(trade.getAmount())
                        .receivedTimestamp(trade.getReceivedTimestamp().toLocalDateTime())
                        .build())
                .collect(Collectors.toList());

        if (tradeOutputList.size() > 0) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
            String formatDateTime = LocalDateTime.now().format(formatter);
            String fileName = path + formatDateTime + ".json";
            File file = new File(fileName);
            writer.writeValue(file, tradeOutputList);
        }
    }

    private static Consumer<String, Trade> createConsumer(JobDataMap dataMap) {
        final Properties kafkaConsumerProperties = getKafkaConfiguration(dataMap);
        final Consumer<String, Trade> consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        consumer.subscribe(Collections.singleton(dataMap.getString(Constants.KAFKA_TOPIC)));
        return consumer;
    }

    private static Properties getKafkaConfiguration(JobDataMap dataMap) {
        final Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, dataMap.getString(Constants.KAFKA_BOOTSTRAP_SERVERS));
        kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, dataMap.getString(Constants.KAFKA_CONSUMERGROUP));

        kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TradeDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Constants.TRUE);
        return kafkaConsumerProperties;
    }
}
