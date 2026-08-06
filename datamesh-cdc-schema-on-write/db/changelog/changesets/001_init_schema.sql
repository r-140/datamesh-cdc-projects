--liquibase formatted sql
--changeset author:id
CREATE TABLE example (id BIGSERIAL PRIMARY KEY);
--rollback DROP TABLE example;