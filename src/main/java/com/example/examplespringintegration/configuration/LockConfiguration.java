package com.example.examplespringintegration.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.jdbc.lock.LockRepository;
import org.springframework.integration.support.leader.LockRegistryLeaderInitiator;

import javax.sql.DataSource;

import java.time.Duration;

import static com.example.examplespringintegration.Application.INSTANCE_ID;

@Configuration
public class LockConfiguration {

    @Bean
    LockRepository lockRepository(DataSource dataSource) {
        var lockRepository = new DefaultLockRepository(dataSource, INSTANCE_ID);
        lockRepository.setPrefix("APPLICATION_");
        return lockRepository;
    }

    @Bean
    JdbcLockRegistry jdbcLockRegistry(LockRepository lockRepository) {
        return new JdbcLockRegistry(lockRepository, Duration.ofSeconds(30));
    }

    @Bean
    LockRegistryLeaderInitiator lockRegistryLeaderInitiator(JdbcLockRegistry jdbcLockRegistry) {
        var lockRegistryLeaderInitiator = new LockRegistryLeaderInitiator(jdbcLockRegistry);
        lockRegistryLeaderInitiator.setHeartBeatMillis(Duration.ofSeconds(10).toMillis());
        return lockRegistryLeaderInitiator;
    }
}
