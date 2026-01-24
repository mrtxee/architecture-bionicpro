INSERT INTO crm_data (
    id, order_number, total, discount, buyer_id, patient_name, 
    amputation_type, prosthesis_type, order_date, delivery_date, 
    status, training_sessions, response_time_target
) VALUES 
(1, '12345', 150000.00, 15000.00, 'user1', 'User One', 'above_elbow', 'myoelectric', '2024-01-10', '2024-03-15', 'delivered', 8, 95),
(2, '123456', 120000.00, 5000.00, 'user2', 'User Two', 'below_elbow', 'myoelectric', '2024-01-12', '2024-03-20', 'delivered', 6, 88),
(3, '123457', 180000.00, 10000.00, 'admin1', 'Admin One', 'above_knee', 'myoelectric', '2024-01-15', '2024-04-01', 'delivered', 10, 105),
(4, '123458', 80000.00, 0.00, 'prothetic1', 'Prothetic One', 'below_knee', 'mechanical', '2024-01-18', '2024-03-25', 'delivered', 4, 120),
(5, '123459', 250000.00, 25000.00, 'prothetic2', 'Prothetic Two', 'above_elbow', 'myoelectric', '2024-01-20', '2024-04-10', 'delivered', 12, 85)
ON CONFLICT (id) DO UPDATE SET
    order_number = EXCLUDED.order_number,
    total = EXCLUDED.total,
    discount = EXCLUDED.discount,
    buyer_id = EXCLUDED.buyer_id,
    patient_name = EXCLUDED.patient_name,
    amputation_type = EXCLUDED.amputation_type,
    prosthesis_type = EXCLUDED.prosthesis_type,
    order_date = EXCLUDED.order_date,
    delivery_date = EXCLUDED.delivery_date,
    status = EXCLUDED.status,
    training_sessions = EXCLUDED.training_sessions,
    response_time_target = EXCLUDED.response_time_target,
    updated_at = CURRENT_TIMESTAMP;
