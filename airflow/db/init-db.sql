CREATE DATABASE crm;
GRANT ALL PRIVILEGES ON DATABASE crm TO airflow;

\c crm
CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) UNIQUE NOT NULL,
    date_of_birth DATE NOT NULL,
    email VARCHAR(50) UNIQUE
);

INSERT INTO users (id, username, date_of_birth, email) VALUES
('00000000-0000-0000-0000-000000000001','user1', '1995-01-01', 'user1@mail.com'),
('00000000-0000-0000-0000-000000000002', 'user2', '1996-01-01', 'user2@mail.com'),
('00000000-0000-0000-0000-000000000003', 'user3', '1997-01-01', 'user3@mail.com');

CREATE DATABASE telemetry;
GRANT ALL PRIVILEGES ON DATABASE telemetry TO airflow;

\c telemetry

CREATE TABLE sensor_data (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    sensor_value NUMERIC
);

INSERT INTO sensor_data (user_id, timestamp, sensor_value) VALUES
('00000000-0000-0000-0000-000000000001', '2026-01-04 12:00:00', 10.5),
('00000000-0000-0000-0000-000000000002', '2026-01-04 12:01:00', 20.5),
('00000000-0000-0000-0000-000000000003', '2026-01-04 12:02:00', 27.5);

CREATE DATABASE olap;
GRANT ALL PRIVILEGES ON DATABASE olap TO airflow;

\c olap
CREATE TABLE report (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(50),
    date_of_birth DATE,
    timestamp TIMESTAMP,
    sensor_value NUMERIC
);
