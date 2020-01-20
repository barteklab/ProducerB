package trades;

import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static trades.Constants.*;

@Slf4j
public class ProducerB {
    public static final String PRODUCER_A_PROPERTIES = "producerB.properties";
    public static final String JOB_ID = "producerBJob";
    public static final String TRIGGER = "trigger";
    public static final String GROUP_1 = "group1";
    public static final String WRITE_TIMESPAN = "write.timespan";

    public static void main(String[] args) throws SchedulerException {
        ProducerB producerB = new ProducerB();
        producerB.start();
    }

    private void start() throws SchedulerException {
        Properties properties = loadProperties();

        Scheduler scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.start();

        JobDetail job = getJobDetail(properties);
        Trigger trigger = getTrigger(properties.getProperty(WRITE_TIMESPAN));
        scheduler.scheduleJob(job, trigger);
    }

    private JobDetail getJobDetail(Properties properties) {
        return JobBuilder.newJob(ProducerJob.class)
                .withIdentity(JOB_ID, GROUP_1)
                .usingJobData(KAFKA_TOPIC, properties.getProperty(KAFKA_TOPIC))
                .usingJobData(KAFKA_BOOTSTRAP_SERVERS, properties.getProperty(KAFKA_BOOTSTRAP_SERVERS))
                .usingJobData(KAFKA_CONSUMERGROUP, properties.getProperty(KAFKA_CONSUMERGROUP))
                .usingJobData(KAFKA_DELAY, properties.getProperty(KAFKA_DELAY))
                .usingJobData(OUTPUT_FILES_PATH, properties.getProperty(OUTPUT_FILES_PATH))
                .build();
    }

    private Trigger getTrigger(String timespan) {
        Integer timespanInt = Integer.valueOf(timespan);
        return TriggerBuilder.newTrigger()
                .withIdentity(TRIGGER, GROUP_1)
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(timespanInt)
                        .repeatForever())
                .build();
    }

    private Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = ProducerB.class.getClassLoader().getResourceAsStream(PRODUCER_A_PROPERTIES)) {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
