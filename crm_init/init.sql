CREATE TABLE IF NOT EXISTS crm_user_prosthesis (
    user_id VARCHAR(255),
    prosthesis_id VARCHAR(255),
    is_active BOOLEAN
);

INSERT INTO crm_user_prosthesis (user_id, prosthesis_id, is_active)
VALUES ('1b37e8e6-16d3-4449-b18e-f0b3be2ce022', 'prosthesis_001', true);
