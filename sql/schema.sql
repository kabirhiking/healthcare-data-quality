-- Healthcare Data Quality Dashboard - Database Schema
-- Designed for comprehensive quality assurance and validation

-- ================================================
-- PATIENT RECORDS TABLE
-- ================================================
CREATE TABLE IF NOT EXISTS patient_records (
    patient_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    date_of_birth DATE,
    gender VARCHAR(10),
    insurance_id VARCHAR(100),
    insurance_provider VARCHAR(100),
    primary_provider VARCHAR(255),
    contact_phone VARCHAR(20),
    contact_email VARCHAR(255),
    address_line1 VARCHAR(255),
    address_city VARCHAR(100),
    address_state VARCHAR(2),
    address_zip VARCHAR(10),
    emergency_contact_name VARCHAR(255),
    emergency_contact_phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_visit_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);

-- ================================================
-- PROVIDERS TABLE
-- ================================================
CREATE TABLE IF NOT EXISTS providers (
    provider_id VARCHAR(50) PRIMARY KEY,
    provider_name VARCHAR(255) NOT NULL,
    specialty VARCHAR(100),
    npi_number VARCHAR(10) UNIQUE,
    license_number VARCHAR(50),
    license_state VARCHAR(2),
    license_expiry_date DATE,
    contact_email VARCHAR(255),
    contact_phone VARCHAR(20),
    practice_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================================================
-- CLAIMS TABLE
-- ================================================
CREATE TABLE IF NOT EXISTS claims (
    claim_id VARCHAR(50) PRIMARY KEY,
    patient_id VARCHAR(50) REFERENCES patient_records(patient_id),
    provider_id VARCHAR(50) REFERENCES providers(provider_id),
    claim_total_amount DECIMAL(10, 2),
    service_date DATE NOT NULL,
    submission_date DATE NOT NULL,
    processing_date DATE,
    status VARCHAR(20) CHECK (status IN ('SUBMITTED', 'PENDING', 'PROCESSED', 'DENIED', 'PAID')),
    denial_reason TEXT,
    procedure_code VARCHAR(10),
    diagnosis_code VARCHAR(10),
    insurance_paid_amount DECIMAL(10, 2),
    patient_responsibility DECIMAL(10, 2),
    adjustment_amount DECIMAL(10, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================================================
-- CLAIMS LINE ITEMS TABLE
-- ================================================
CREATE TABLE IF NOT EXISTS claims_line_items (
    line_item_id SERIAL PRIMARY KEY,
    claim_id VARCHAR(50) REFERENCES claims(claim_id),
    procedure_code VARCHAR(10) NOT NULL,
    description TEXT,
    quantity INTEGER DEFAULT 1,
    unit_price DECIMAL(10, 2),
    line_item_amount DECIMAL(10, 2),
    modifier VARCHAR(10),
    service_date DATE
);

-- ================================================
-- ENCOUNTERS TABLE
-- ================================================
CREATE TABLE IF NOT EXISTS encounters (
    encounter_id VARCHAR(50) PRIMARY KEY,
    patient_id VARCHAR(50) REFERENCES patient_records(patient_id),
    provider_id VARCHAR(50) REFERENCES providers(provider_id),
    encounter_date DATE NOT NULL,
    encounter_type VARCHAR(50),
    chief_complaint TEXT,
    diagnosis_codes TEXT[], -- Array of diagnosis codes
    procedure_codes TEXT[], -- Array of procedure codes
    documentation_complete BOOLEAN DEFAULT FALSE,
    billing_submitted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================================================
-- DATA QUALITY AUDIT LOG
-- ================================================
CREATE TABLE IF NOT EXISTS quality_audit_log (
    audit_id SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    check_type VARCHAR(100),
    table_name VARCHAR(100),
    record_id VARCHAR(50),
    issue_type VARCHAR(100),
    issue_description TEXT,
    severity VARCHAR(20) CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    status VARCHAR(20) CHECK (status IN ('OPEN', 'IN_PROGRESS', 'RESOLVED', 'ACKNOWLEDGED')) DEFAULT 'OPEN',
    assigned_to VARCHAR(100),
    resolution_notes TEXT,
    resolved_at TIMESTAMP
);

-- ================================================
-- QUALITY METRICS TABLE
-- ================================================
CREATE TABLE IF NOT EXISTS quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_date DATE DEFAULT CURRENT_DATE,
    metric_name VARCHAR(100),
    metric_value DECIMAL(10, 2),
    target_value DECIMAL(10, 2),
    category VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================================================
-- INDEXES FOR PERFORMANCE
-- ================================================
CREATE INDEX idx_patient_insurance ON patient_records(insurance_id);
CREATE INDEX idx_patient_provider ON patient_records(primary_provider);
CREATE INDEX idx_patient_created ON patient_records(created_at);
CREATE INDEX idx_claims_patient ON claims(patient_id);
CREATE INDEX idx_claims_provider ON claims(provider_id);
CREATE INDEX idx_claims_status ON claims(status);
CREATE INDEX idx_claims_service_date ON claims(service_date);
CREATE INDEX idx_claims_submission_date ON claims(submission_date);
CREATE INDEX idx_line_items_claim ON claims_line_items(claim_id);
CREATE INDEX idx_encounters_patient ON encounters(patient_id);
CREATE INDEX idx_encounters_date ON encounters(encounter_date);
CREATE INDEX idx_audit_table ON quality_audit_log(table_name);
CREATE INDEX idx_audit_status ON quality_audit_log(status);
CREATE INDEX idx_audit_severity ON quality_audit_log(severity);
CREATE INDEX idx_audit_timestamp ON quality_audit_log(check_timestamp);

-- ================================================
-- TRIGGERS FOR UPDATED_AT
-- ================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_patient_records_updated_at
    BEFORE UPDATE ON patient_records
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_claims_updated_at
    BEFORE UPDATE ON claims
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_encounters_updated_at
    BEFORE UPDATE ON encounters
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
