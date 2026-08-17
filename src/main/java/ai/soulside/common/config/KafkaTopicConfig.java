package ai.soulside.common.config;

import ai.soulside.common.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics so they are auto-created on startup via {@code KafkaAdmin}.
 *
 * <p>Disabled under the {@code test} profile where the embedded broker provisions
 * topics through {@code @EmbeddedKafka}.
 */
@Configuration
@Profile("!test")
public class KafkaTopicConfig {

    @Bean
    public NewTopic meetingEventsTopic() {
        return TopicBuilder.name(KafkaTopics.MEETING_EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic transcriptReconstructTopic() {
        return TopicBuilder.name(KafkaTopics.TRANSCRIPT_RECONSTRUCT)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic meetingEventsDltTopic() {
        return TopicBuilder.name(KafkaTopics.MEETING_EVENTS_DLT)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic transcriptReconstructDltTopic() {
        return TopicBuilder.name(KafkaTopics.TRANSCRIPT_RECONSTRUCT_DLT)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
