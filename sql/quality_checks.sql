-- ================================================
-- COMPREHENSIVE DATA QUALITY CHECKS
-- SQL queries for identifying data issues and discrepancies
-- ================================================

-- ================================================
-- 1. PATIENT RECORD COMPLETENESS CHECKS
-- ================================================

-- Check for missing critical patient information
SELECT 
    'Patient Record Completeness' AS check_name,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN name IS NULL THEN 1 END) AS missing_name,
    COUNT(CASE WHEN date_of_birth IS NULL THEN 1 END) AS missing_dob,
    COUNT(CASE WHEN insurance_id IS NULL THEN 1 END) AS missing_insurance,
    COUNT(CASE WHEN primary_provider IS NULL THEN 1 END) AS missing_provider,
    COUNT(CASE WHEN contact_phone IS NULL AND contact_email IS NULL THEN 1 END) AS missing_all_contact,
    ROUND(COUNT(CASE WHEN name IS NOT NULL 
                     AND date_of_birth IS NOT NULL 
                     AND insurance_id IS NOT NULL 
                     AND primary_provider IS NOT NULL 
                THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) AS completeness_percentage
FROM patient_records
WHERE is_active = TRUE;

-- Detailed list of incomplete patient records
SELECT 
    patient_id,
    name,
    date_of_birth,
    insurance_id,
    primary_provider,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN name IS NULL THEN 'name' END,
        CASE WHEN date_of_birth IS NULL THEN 'date_of_birth' END,
        CASE WHEN insurance_id IS NULL THEN 'insurance_id' END,
        CASE WHEN primary_provider IS NULL THEN 'primary_provider' END,
        CASE WHEN contact_phone IS NULL AND contact_email IS NULL THEN 'contact_info' END
    ], NULL) AS missing_fields,
    created_at,
    updated_at
FROM patient_records
WHERE is_active = TRUE
  AND (name IS NULL 
       OR date_of_birth IS NULL 
       OR insurance_id IS NULL 
       OR primary_provider IS NULL
       OR (contact_phone IS NULL AND contact_email IS NULL))
ORDER BY created_at DESC;

-- ================================================
-- 2. DATA VALIDATION CHECKS
-- ================================================

-- Invalid date of birth (future dates or unrealistic ages)
SELECT 
    patient_id,
    name,
    date_of_birth,
    EXTRACT(YEAR FROM AGE(date_of_birth)) AS age,
    'Invalid DOB' AS issue_type
FROM patient_records
WHERE date_of_birth > CURRENT_DATE
   OR date_of_birth < '1900-01-01'
   OR EXTRACT(YEAR FROM AGE(date_of_birth)) > 120
ORDER BY date_of_birth;

-- Invalid phone numbers (not matching standard format)
SELECT 
    patient_id,
    name,
    contact_phone,
    'Invalid Phone Format' AS issue_type
FROM patient_records
WHERE contact_phone IS NOT NULL
  AND LENGTH(REGEXP_REPLACE(contact_phone, '[^0-9]', '', 'g')) != 10
  AND is_active = TRUE;

-- Invalid email addresses
SELECT 
    patient_id,
    name,
    contact_email,
    'Invalid Email Format' AS issue_type
FROM patient_records
WHERE contact_email IS NOT NULL
  AND contact_email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
  AND is_active = TRUE;

-- ================================================
-- 3. CLAIMS DISCREPANCY DETECTION
-- ================================================

-- Claims where line items don't sum to claim total
WITH claim_line_totals AS (
    SELECT 
        c.claim_id,
        c.patient_id,
        c.provider_id,
        c.claim_total_amount,
        c.service_date,
        c.status,
        COALESCE(SUM(cli.line_item_amount), 0) AS calculated_total,
        COUNT(cli.line_item_id) AS line_item_count
    FROM claims c
    LEFT JOIN claims_line_items cli ON c.claim_id = cli.claim_id
    GROUP BY c.claim_id, c.patient_id, c.provider_id, c.claim_total_amount, c.service_date, c.status
)
SELECT 
    claim_id,
    patient_id,
    provider_id,
    service_date,
    status,
    claim_total_amount,
    calculated_total,
    line_item_count,
    ABS(claim_total_amount - calculated_total) AS discrepancy_amount,
    ROUND((ABS(claim_total_amount - calculated_total) / NULLIF(claim_total_amount, 0)) * 100, 2) AS discrepancy_percentage,
    'Claims Amount Mismatch' AS issue_type
FROM claim_line_totals
WHERE ABS(claim_total_amount - calculated_total) > 0.01
ORDER BY discrepancy_amount DESC;

-- Claims with missing or mismatched amounts
SELECT 
    claim_id,
    patient_id,
    provider_id,
    claim_total_amount,
    insurance_paid_amount,
    patient_responsibility,
    adjustment_amount,
    (COALESCE(insurance_paid_amount, 0) + COALESCE(patient_responsibility, 0) + COALESCE(adjustment_amount, 0)) AS sum_of_parts,
    ABS(claim_total_amount - (COALESCE(insurance_paid_amount, 0) + COALESCE(patient_responsibility, 0) + COALESCE(adjustment_amount, 0))) AS difference,
    'Payment Distribution Mismatch' AS issue_type
FROM claims
WHERE status IN ('PROCESSED', 'PAID')
  AND ABS(claim_total_amount - (COALESCE(insurance_paid_amount, 0) + COALESCE(patient_responsibility, 0) + COALESCE(adjustment_amount, 0))) > 0.01;

-- ================================================
-- 4. TEMPORAL ANOMALIES
-- ================================================

-- Claims submitted before service date
SELECT 
    claim_id,
    patient_id,
    service_date,
    submission_date,
    (submission_date - service_date) AS days_difference,
    'Submission Before Service' AS issue_type
FROM claims
WHERE submission_date < service_date;

-- Claims processed before submission
SELECT 
    claim_id,
    patient_id,
    submission_date,
    processing_date,
    'Processing Before Submission' AS issue_type
FROM claims
WHERE processing_date IS NOT NULL
  AND processing_date < submission_date;

-- Old pending claims (over 30 days)
SELECT 
    claim_id,
    patient_id,
    provider_id,
    submission_date,
    status,
    CURRENT_DATE - submission_date AS days_pending,
    'Long Pending Claim' AS issue_type
FROM claims
WHERE status IN ('SUBMITTED', 'PENDING')
  AND submission_date < CURRENT_DATE - INTERVAL '30 days'
ORDER BY submission_date;

-- ================================================
-- 5. PROVIDER DATA QUALITY
-- ================================================

-- Providers with expired licenses
SELECT 
    provider_id,
    provider_name,
    license_number,
    license_state,
    license_expiry_date,
    CURRENT_DATE - license_expiry_date AS days_expired,
    'Expired License' AS issue_type
FROM providers
WHERE license_expiry_date < CURRENT_DATE
  AND is_active = TRUE;

-- Providers with missing critical information
SELECT 
    provider_id,
    provider_name,
    npi_number,
    license_number,
    specialty,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN npi_number IS NULL THEN 'npi_number' END,
        CASE WHEN license_number IS NULL THEN 'license_number' END,
        CASE WHEN specialty IS NULL THEN 'specialty' END,
        CASE WHEN contact_email IS NULL THEN 'contact_email' END
    ], NULL) AS missing_fields
FROM providers
WHERE is_active = TRUE
  AND (npi_number IS NULL 
       OR license_number IS NULL 
       OR specialty IS NULL
       OR contact_email IS NULL);

-- ================================================
-- 6. DUPLICATE DETECTION
-- ================================================

-- Potential duplicate patients (same name and DOB)
SELECT 
    name,
    date_of_birth,
    COUNT(*) AS duplicate_count,
    ARRAY_AGG(patient_id) AS patient_ids,
    'Potential Duplicate Patient' AS issue_type
FROM patient_records
WHERE name IS NOT NULL 
  AND date_of_birth IS NOT NULL
  AND is_active = TRUE
GROUP BY name, date_of_birth
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Duplicate claims (same patient, provider, service date, and amount)
SELECT 
    patient_id,
    provider_id,
    service_date,
    claim_total_amount,
    COUNT(*) AS duplicate_count,
    ARRAY_AGG(claim_id) AS claim_ids,
    'Potential Duplicate Claim' AS issue_type
FROM claims
GROUP BY patient_id, provider_id, service_date, claim_total_amount
HAVING COUNT(*) > 1;

-- ================================================
-- 7. REFERENTIAL INTEGRITY CHECKS
-- ================================================

-- Claims with invalid patient references
SELECT 
    c.claim_id,
    c.patient_id,
    c.service_date,
    'Orphaned Claim - Invalid Patient' AS issue_type
FROM claims c
LEFT JOIN patient_records p ON c.patient_id = p.patient_id
WHERE p.patient_id IS NULL;

-- Claims with invalid provider references
SELECT 
    c.claim_id,
    c.provider_id,
    c.service_date,
    'Orphaned Claim - Invalid Provider' AS issue_type
FROM claims c
LEFT JOIN providers p ON c.provider_id = p.provider_id
WHERE p.provider_id IS NULL;

-- ================================================
-- 8. BUSINESS RULE VIOLATIONS
-- ================================================

-- Claims with amounts exceeding reasonable thresholds
SELECT 
    claim_id,
    patient_id,
    provider_id,
    service_date,
    claim_total_amount,
    'Unusually High Claim Amount' AS issue_type
FROM claims
WHERE claim_total_amount > 50000
ORDER BY claim_total_amount DESC;

-- Patients with unusually high claim frequency
WITH patient_claim_counts AS (
    SELECT 
        patient_id,
        COUNT(*) AS claim_count,
        MIN(service_date) AS first_claim_date,
        MAX(service_date) AS last_claim_date
    FROM claims
    WHERE service_date >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY patient_id
)
SELECT 
    p.patient_id,
    p.name,
    p.date_of_birth,
    pcc.claim_count,
    pcc.first_claim_date,
    pcc.last_claim_date,
    'High Claim Frequency' AS issue_type
FROM patient_claim_counts pcc
JOIN patient_records p ON pcc.patient_id = p.patient_id
WHERE pcc.claim_count > 50
ORDER BY pcc.claim_count DESC;

-- ================================================
-- 9. DATA FRESHNESS CHECKS
-- ================================================

-- Stale patient records (not updated in over 2 years)
SELECT 
    patient_id,
    name,
    last_visit_date,
    updated_at,
    CURRENT_DATE - updated_at::DATE AS days_since_update,
    'Stale Patient Record' AS issue_type
FROM patient_records
WHERE updated_at < CURRENT_TIMESTAMP - INTERVAL '2 years'
  AND is_active = TRUE
ORDER BY updated_at;

-- ================================================
-- 10. ENCOUNTER DOCUMENTATION COMPLETENESS
-- ================================================

-- Encounters with incomplete documentation
SELECT 
    encounter_id,
    patient_id,
    provider_id,
    encounter_date,
    encounter_type,
    documentation_complete,
    billing_submitted,
    CURRENT_DATE - encounter_date AS days_since_encounter,
    'Incomplete Documentation' AS issue_type
FROM encounters
WHERE documentation_complete = FALSE
  AND encounter_date < CURRENT_DATE - INTERVAL '7 days'
ORDER BY encounter_date;

-- Encounters documented but not billed
SELECT 
    e.encounter_id,
    e.patient_id,
    e.provider_id,
    e.encounter_date,
    e.documentation_complete,
    e.billing_submitted,
    CURRENT_DATE - e.encounter_date AS days_since_encounter,
    'Documented But Not Billed' AS issue_type
FROM encounters e
WHERE e.documentation_complete = TRUE
  AND e.billing_submitted = FALSE
  AND e.encounter_date < CURRENT_DATE - INTERVAL '14 days'
ORDER BY e.encounter_date;
