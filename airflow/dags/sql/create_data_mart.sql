DROP TABLE IF EXISTS data_mart;

CREATE TABLE data_mart (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    patient_name TEXT NOT NULL,
    device_id TEXT NOT NULL,
    amputation_type TEXT,
    prosthesis_type TEXT,
    order_date DATE,
    delivery_date DATE,
    status TEXT,
    training_sessions INT,
    response_time_target INT,
    avg_battery_level NUMERIC(5,2),
    avg_response_time NUMERIC(6,2),
    avg_muscle_signal_strength NUMERIC(6,3),
    avg_actuator_power NUMERIC(6,2),
    avg_device_temperature NUMERIC(5,2),
    total_usage_duration INT,
    action_count INT,
    last_activity_ts TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_data_mart_user_id ON data_mart(user_id);
CREATE INDEX idx_data_mart_device_id ON data_mart(device_id);
CREATE INDEX idx_data_mart_status ON data_mart(status);
