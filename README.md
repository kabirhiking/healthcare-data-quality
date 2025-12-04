# Healthcare Data Quality Dashboard

## Project Overview
A comprehensive data quality assurance system for healthcare operations, demonstrating SQL proficiency, attention to detail, and process documentation skills required for Growth Operations at Commure.

## Business Context
This project simulates real-world healthcare data quality challenges:
- Patient record accuracy validation
- Claims processing error detection
- Provider documentation completeness checks
- Billing discrepancy identification
- Operational KPI monitoring

## Key Features
1. **Automated Data Quality Checks**: SQL-based validation rules for patient records, claims, and provider data
2. **Error Detection System**: Identifies discrepancies, missing data, and anomalies
3. **Quality Metrics Dashboard**: Tracks data completeness, accuracy, and timeliness
4. **Audit Trail Documentation**: Comprehensive logging of all quality checks
5. **Process Documentation**: Detailed SOPs for data validation procedures

## Technical Stack
- **Database**: PostgreSQL
- **Analysis**: Python (Pandas, NumPy)
- **Visualization**: Matplotlib, Seaborn
- **SQL**: Complex queries for data validation
- **Documentation**: Markdown, Jupyter Notebooks

## Project Structure
```
project1-healthcare-data-quality/
├── README.md
├── requirements.txt
├── data/
│   ├── sample_patient_records.csv
│   ├── sample_claims_data.csv
│   └── sample_provider_data.csv
├── sql/
│   ├── schema.sql
│   ├── quality_checks.sql
│   ├── discrepancy_detection.sql
│   └── metrics_queries.sql
├── scripts/
│   ├── data_generator.py
│   ├── quality_checker.py
│   ├── dashboard_generator.py
│   └── audit_logger.py
├── notebooks/
│   ├── 01_data_quality_analysis.ipynb
│   └── 02_discrepancy_investigation.ipynb
├── documentation/
│   ├── quality_assurance_sop.md
│   ├── error_handling_procedures.md
│   └── metrics_definitions.md
└── outputs/
    ├── quality_report.html
    └── audit_logs/
```

## Key SQL Queries Demonstrated

### 1. Patient Record Completeness Check
```sql
-- Identifies incomplete patient records
SELECT 
    patient_id,
    CASE WHEN name IS NULL THEN 1 ELSE 0 END +
    CASE WHEN date_of_birth IS NULL THEN 1 ELSE 0 END +
    CASE WHEN insurance_id IS NULL THEN 1 ELSE 0 END +
    CASE WHEN primary_provider IS NULL THEN 1 ELSE 0 END AS missing_fields_count,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN name IS NULL THEN 'name' END,
        CASE WHEN date_of_birth IS NULL THEN 'date_of_birth' END,
        CASE WHEN insurance_id IS NULL THEN 'insurance_id' END,
        CASE WHEN primary_provider IS NULL THEN 'primary_provider' END
    ], NULL) AS missing_fields
FROM patient_records
WHERE name IS NULL 
   OR date_of_birth IS NULL 
   OR insurance_id IS NULL 
   OR primary_provider IS NULL;
```

### 2. Claims Discrepancy Detection
```sql
-- Detects billing amount discrepancies
WITH claim_totals AS (
    SELECT 
        claim_id,
        SUM(line_item_amount) AS calculated_total,
        claim_total_amount
    FROM claims_line_items cli
    JOIN claims c ON cli.claim_id = c.claim_id
    GROUP BY claim_id, claim_total_amount
)
SELECT 
    claim_id,
    calculated_total,
    claim_total_amount,
    ABS(calculated_total - claim_total_amount) AS discrepancy,
    ROUND((ABS(calculated_total - claim_total_amount) / claim_total_amount) * 100, 2) AS discrepancy_pct
FROM claim_totals
WHERE ABS(calculated_total - claim_total_amount) > 0.01
ORDER BY discrepancy DESC;
```

### 3. Data Quality Metrics Dashboard
```sql
-- Comprehensive quality metrics
WITH quality_metrics AS (
    SELECT 
        'Patient Records' AS data_type,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN name IS NOT NULL 
                    AND date_of_birth IS NOT NULL 
                    AND insurance_id IS NOT NULL 
                    AND primary_provider IS NOT NULL 
              THEN 1 END) AS complete_records,
        COUNT(CASE WHEN created_at >= CURRENT_DATE - INTERVAL '30 days' 
              THEN 1 END) AS recent_records
    FROM patient_records
    
    UNION ALL
    
    SELECT 
        'Claims' AS data_type,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN status = 'PROCESSED' THEN 1 END) AS complete_records,
        COUNT(CASE WHEN submission_date >= CURRENT_DATE - INTERVAL '30 days' 
              THEN 1 END) AS recent_records
    FROM claims
)
SELECT 
    data_type,
    total_records,
    complete_records,
    ROUND((complete_records::NUMERIC / total_records) * 100, 2) AS completeness_pct,
    recent_records,
    ROUND((recent_records::NUMERIC / total_records) * 100, 2) AS recency_pct
FROM quality_metrics;
```

## Installation & Setup

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Git

### Steps
```bash
# Clone the repository
git clone https://github.com/yourusername/healthcare-data-quality-dashboard.git
cd healthcare-data-quality-dashboard

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up PostgreSQL database
createdb healthcare_qa
psql healthcare_qa -f sql/schema.sql

# Generate sample data
python scripts/data_generator.py

# Run quality checks
python scripts/quality_checker.py

# Generate dashboard
python scripts/dashboard_generator.py
```

## Usage Examples

### Running Quality Checks
```python
from scripts.quality_checker import HealthcareQualityChecker

# Initialize checker
checker = HealthcareQualityChecker(db_connection)

# Run all quality checks
results = checker.run_all_checks()

# Generate report
checker.generate_quality_report(results, output_path='outputs/quality_report.html')
```

### Investigating Discrepancies
```python
from scripts.quality_checker import investigate_discrepancies

# Find claims with billing discrepancies
discrepancies = investigate_discrepancies(
    table='claims',
    check_type='billing_validation'
)

# Export for review
discrepancies.to_csv('outputs/billing_discrepancies.csv', index=False)
```

## Quality Assurance Process

### Daily Checks
1. **Data Completeness**: Verify all required fields are populated
2. **Data Accuracy**: Cross-reference values against source systems
3. **Data Consistency**: Check for logical inconsistencies
4. **Timeliness**: Ensure data is updated within SLA windows

### Weekly Audits
1. Review error trends and patterns
2. Update validation rules based on new findings
3. Generate executive summary reports
4. Document process improvements

## Key Metrics Tracked
- **Completeness Rate**: % of records with all required fields
- **Accuracy Rate**: % of records passing validation rules
- **Error Rate**: # of errors per 1000 records
- **Resolution Time**: Average time to resolve discrepancies
- **Process Adherence**: % of processes following SOP

## Skills Demonstrated
✅ **SQL Proficiency**: Complex queries, window functions, CTEs, data validation  
✅ **Attention to Detail**: Comprehensive error detection and validation rules  
✅ **Process Documentation**: Detailed SOPs and instructions  
✅ **Data Analysis**: Statistical analysis and pattern recognition  
✅ **Quality Assurance**: Multi-layered validation framework  
✅ **Cross-functional Communication**: Clear documentation for technical and non-technical stakeholders  

## Future Enhancements
- Real-time alerting system
- Machine learning-based anomaly detection
- Integration with healthcare APIs (HL7 FHIR)
- Automated remediation workflows
- Advanced visualization dashboard

## Contact
Created for Commure Data Analyst application - demonstrating quality assurance, SQL proficiency, and healthcare domain knowledge.

## License
MIT License
