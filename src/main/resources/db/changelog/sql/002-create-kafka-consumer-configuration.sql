--liquibase formatted sql

--changeset michal.paukszto:002-create-kafka-consumer-configuration
-- Each running instance claims one free slot and consumes that slot's topics.
CREATE TABLE KAFKA_CONSUMER_CONFIGURATION (
    SLOT        INTEGER      NOT NULL,
    TOPICS      VARCHAR(500) NOT NULL, -- comma-separated topic names
    ACTIVE      BOOLEAN      NOT NULL DEFAULT TRUE, -- only active slots are claimed
    INSTANCE_ID VARCHAR(36), -- optional: reserves the slot for the instance with this id; NULL = any instance
    CONSTRAINT KAFKA_CONSUMER_CONFIGURATION_PK PRIMARY KEY (SLOT)
);

INSERT INTO KAFKA_CONSUMER_CONFIGURATION (SLOT, TOPICS, ACTIVE) VALUES
    (1, 'example-orders,example-payments', FALSE),
    (2, 'example-orders,example-payments', FALSE),
    (3, 'example-orders,example-payments', FALSE),
    (4, 'example-notifications', FALSE);
--rollback DROP TABLE KAFKA_CONSUMER_CONFIGURATION;
