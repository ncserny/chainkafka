import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;

@SpringBootApplication
public class BlockchainStreamApplication {

    @Value("${kafka.bootstrap.servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @Bean
    public BlockchainStreamManager blockchainStreamManager() {
        return new BlockchainStreamManager(kafkaBootstrapServers);
    }

    public static void main(String[] args) {
        SpringApplication.run(BlockchainStreamApplication.class, args);
    }
} 