package ai.soulside;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableAsync
public class MeetingWebhookServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MeetingWebhookServiceApplication.class, args);
    }
}
