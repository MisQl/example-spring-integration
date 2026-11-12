--liquibase formatted sql

--changeset michal.paukszto:002-create-kafka-consumer-slot
-- Each running instance claims one free slot and consumes that slot's topics.
CREATE TABLE KAFKA_CONSUMER_SLOT (
    SLOT   INTEGER      NOT NULL,
    TOPICS VARCHAR(500) NOT NULL, -- comma-separated topic names
    CONSTRAINT KAFKA_CONSUMER_SLOT_PK PRIMARY KEY (SLOT)
);

INSERT INTO KAFKA_CONSUMER_SLOT (SLOT, TOPICS) VALUES
    (1, 'example-orders,example-payments'),
    (2, 'example-orders,example-payments'),
    (3, 'example-orders,example-payments'),
    (4, 'example-notifications');
--rollback DROP TABLE KAFKA_CONSUMER_SLOT;
