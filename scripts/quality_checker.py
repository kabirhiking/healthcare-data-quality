"""
Healthcare Data Quality Checker
Comprehensive quality assurance system for healthcare data
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from datetime import datetime
import json
from typing import Dict, List, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HealthcareQualityChecker:
    """Main class for running data quality checks on healthcare data"""
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the quality checker
        
        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self.connection = None
        self.quality_issues = []
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("Database connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame
        
        Args:
            query: SQL query to execute
            
        Returns:
            DataFrame with query results
        """
        try:
            return pd.read_sql_query(query, self.connection)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def log_quality_issue(self, 
                         check_type: str,
                         table_name: str,
                         record_id: str,
                         issue_type: str,
                         issue_description: str,
                         severity: str):
        """
        Log a quality issue to the audit table
        
        Args:
            check_type: Type of quality check
            table_name: Table where issue was found
            record_id: ID of the problematic record
            issue_type: Classification of the issue
            issue_description: Detailed description
            severity: LOW, MEDIUM, HIGH, or CRITICAL
        """
        query = """
        INSERT INTO quality_audit_log 
        (check_type, table_name, record_id, issue_type, issue_description, severity)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    check_type, table_name, record_id, 
                    issue_type, issue_description, severity
                ))
                self.connection.commit()
        except Exception as e:
            logger.error(f"Failed to log quality issue: {e}")
            self.connection.rollback()
    
    def check_patient_completeness(self) -> Dict[str, Any]:
        """
        Check completeness of patient records
        
        Returns:
            Dictionary with check results
        """
        logger.info("Running patient completeness check...")
        
        query = """
        SELECT 
            patient_id,
            name,
            date_of_birth,
            insurance_id,
            primary_provider,
            contact_phone,
            contact_email,
            ARRAY_REMOVE(ARRAY[
                CASE WHEN name IS NULL THEN 'name' END,
                CASE WHEN date_of_birth IS NULL THEN 'date_of_birth' END,
                CASE WHEN insurance_id IS NULL THEN 'insurance_id' END,
                CASE WHEN primary_provider IS NULL THEN 'primary_provider' END,
                CASE WHEN contact_phone IS NULL AND contact_email IS NULL THEN 'contact_info' END
            ], NULL) AS missing_fields
        FROM patient_records
        WHERE is_active = TRUE
          AND (name IS NULL 
               OR date_of_birth IS NULL 
               OR insurance_id IS NULL 
               OR primary_provider IS NULL
               OR (contact_phone IS NULL AND contact_email IS NULL))
        """
        
        results = self.execute_query(query)
        
        # Log each incomplete record
        for _, row in results.iterrows():
            missing = ', '.join(row['missing_fields'])
            self.log_quality_issue(
                check_type='completeness_check',
                table_name='patient_records',
                record_id=row['patient_id'],
                issue_type='incomplete_record',
                issue_description=f"Missing fields: {missing}",
                severity='HIGH' if len(row['missing_fields']) > 2 else 'MEDIUM'
            )
        
        return {
            'check_name': 'Patient Completeness',
            'issues_found': len(results),
            'details': results
        }
    
    def check_claims_discrepancies(self) -> Dict[str, Any]:
        """
        Check for discrepancies in claims amounts
        
        Returns:
            Dictionary with check results
        """
        logger.info("Running claims discrepancy check...")
        
        query = """
        WITH claim_line_totals AS (
            SELECT 
                c.claim_id,
                c.patient_id,
                c.provider_id,
                c.claim_total_amount,
                COALESCE(SUM(cli.line_item_amount), 0) AS calculated_total,
                COUNT(cli.line_item_id) AS line_item_count
            FROM claims c
            LEFT JOIN claims_line_items cli ON c.claim_id = cli.claim_id
            GROUP BY c.claim_id, c.patient_id, c.provider_id, c.claim_total_amount
        )
        SELECT 
            claim_id,
            patient_id,
            provider_id,
            claim_total_amount,
            calculated_total,
            line_item_count,
            ABS(claim_total_amount - calculated_total) AS discrepancy_amount,
            ROUND((ABS(claim_total_amount - calculated_total) / NULLIF(claim_total_amount, 0)) * 100, 2) AS discrepancy_percentage
        FROM claim_line_totals
        WHERE ABS(claim_total_amount - calculated_total) > 0.01
        ORDER BY discrepancy_amount DESC
        """
        
        results = self.execute_query(query)
        
        # Log each discrepancy
        for _, row in results.iterrows():
            severity = 'CRITICAL' if row['discrepancy_amount'] > 1000 else 'HIGH'
            self.log_quality_issue(
                check_type='discrepancy_check',
                table_name='claims',
                record_id=row['claim_id'],
                issue_type='amount_mismatch',
                issue_description=f"Claim total: ${row['claim_total_amount']:.2f}, "
                                f"Line items sum: ${row['calculated_total']:.2f}, "
                                f"Discrepancy: ${row['discrepancy_amount']:.2f}",
                severity=severity
            )
        
        return {
            'check_name': 'Claims Discrepancies',
            'issues_found': len(results),
            'total_discrepancy': results['discrepancy_amount'].sum() if len(results) > 0 else 0,
            'details': results
        }
    
    def check_temporal_anomalies(self) -> Dict[str, Any]:
        """
        Check for temporal anomalies in claims
        
        Returns:
            Dictionary with check results
        """
        logger.info("Running temporal anomaly check...")
        
        query = """
        SELECT 
            claim_id,
            patient_id,
            service_date,
            submission_date,
            processing_date,
            CASE 
                WHEN submission_date < service_date THEN 'Submission before service'
                WHEN processing_date < submission_date THEN 'Processing before submission'
            END AS anomaly_type
        FROM claims
        WHERE submission_date < service_date
           OR (processing_date IS NOT NULL AND processing_date < submission_date)
        """
        
        results = self.execute_query(query)
        
        # Log each anomaly
        for _, row in results.iterrows():
            self.log_quality_issue(
                check_type='temporal_check',
                table_name='claims',
                record_id=row['claim_id'],
                issue_type='temporal_anomaly',
                issue_description=row['anomaly_type'],
                severity='HIGH'
            )
        
        return {
            'check_name': 'Temporal Anomalies',
            'issues_found': len(results),
            'details': results
        }
    
    def check_duplicate_records(self) -> Dict[str, Any]:
        """
        Check for potential duplicate records
        
        Returns:
            Dictionary with check results
        """
        logger.info("Running duplicate record check...")
        
        query = """
        SELECT 
            name,
            date_of_birth,
            COUNT(*) AS duplicate_count,
            ARRAY_AGG(patient_id) AS patient_ids
        FROM patient_records
        WHERE name IS NOT NULL 
          AND date_of_birth IS NOT NULL
          AND is_active = TRUE
        GROUP BY name, date_of_birth
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        """
        
        results = self.execute_query(query)
        
        # Log each duplicate group
        for _, row in results.iterrows():
            patient_ids = ', '.join(row['patient_ids'])
            self.log_quality_issue(
                check_type='duplicate_check',
                table_name='patient_records',
                record_id=row['patient_ids'][0],
                issue_type='potential_duplicate',
                issue_description=f"Duplicate patients found: {patient_ids}",
                severity='MEDIUM'
            )
        
        return {
            'check_name': 'Duplicate Records',
            'issues_found': len(results),
            'total_duplicates': results['duplicate_count'].sum() if len(results) > 0 else 0,
            'details': results
        }
    
    def check_provider_credentials(self) -> Dict[str, Any]:
        """
        Check provider license and credential validity
        
        Returns:
            Dictionary with check results
        """
        logger.info("Running provider credentials check...")
        
        query = """
        SELECT 
            provider_id,
            provider_name,
            license_number,
            license_state,
            license_expiry_date,
            CURRENT_DATE - license_expiry_date AS days_expired
        FROM providers
        WHERE license_expiry_date < CURRENT_DATE
          AND is_active = TRUE
        ORDER BY license_expiry_date
        """
        
        results = self.execute_query(query)
        
        # Log expired licenses
        for _, row in results.iterrows():
            self.log_quality_issue(
                check_type='credential_check',
                table_name='providers',
                record_id=row['provider_id'],
                issue_type='expired_license',
                issue_description=f"License expired {row['days_expired']} days ago",
                severity='CRITICAL'
            )
        
        return {
            'check_name': 'Provider Credentials',
            'issues_found': len(results),
            'details': results
        }
    
    def run_all_checks(self) -> Dict[str, Any]:
        """
        Run all quality checks
        
        Returns:
            Dictionary with all check results
        """
        logger.info("=" * 60)
        logger.info("Starting comprehensive quality assurance checks")
        logger.info("=" * 60)
        
        self.connect()
        
        try:
            results = {
                'timestamp': datetime.now().isoformat(),
                'checks': {
                    'patient_completeness': self.check_patient_completeness(),
                    'claims_discrepancies': self.check_claims_discrepancies(),
                    'temporal_anomalies': self.check_temporal_anomalies(),
                    'duplicate_records': self.check_duplicate_records(),
                    'provider_credentials': self.check_provider_credentials()
                }
            }
            
            # Calculate summary statistics
            total_issues = sum(
                check['issues_found'] 
                for check in results['checks'].values()
            )
            
            results['summary'] = {
                'total_issues_found': total_issues,
                'checks_performed': len(results['checks']),
                'status': 'FAIL' if total_issues > 0 else 'PASS'
            }
            
            logger.info("=" * 60)
            logger.info(f"Quality checks completed. Total issues found: {total_issues}")
            logger.info("=" * 60)
            
            return results
            
        finally:
            self.disconnect()
    
    def generate_quality_report(self, results: Dict[str, Any], output_path: str):
        """
        Generate HTML quality report
        
        Args:
            results: Results from run_all_checks()
            output_path: Path to save HTML report
        """
        logger.info(f"Generating quality report: {output_path}")
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Healthcare Data Quality Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #2c3e50; }}
                h2 {{ color: #34495e; margin-top: 30px; }}
                .summary {{ background: #ecf0f1; padding: 15px; border-radius: 5px; }}
                .pass {{ color: #27ae60; font-weight: bold; }}
                .fail {{ color: #e74c3c; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
                th {{ background: #3498db; color: white; padding: 10px; text-align: left; }}
                td {{ border: 1px solid #ddd; padding: 8px; }}
                tr:nth-child(even) {{ background: #f9f9f9; }}
            </style>
        </head>
        <body>
            <h1>Healthcare Data Quality Report</h1>
            <div class="summary">
                <h2>Executive Summary</h2>
                <p><strong>Report Generated:</strong> {results['timestamp']}</p>
                <p><strong>Total Checks Performed:</strong> {results['summary']['checks_performed']}</p>
                <p><strong>Total Issues Found:</strong> {results['summary']['total_issues_found']}</p>
                <p><strong>Overall Status:</strong> 
                    <span class="{'pass' if results['summary']['status'] == 'PASS' else 'fail'}">
                        {results['summary']['status']}
                    </span>
                </p>
            </div>
        """
        
        # Add details for each check
        for check_name, check_data in results['checks'].items():
            html_content += f"""
            <h2>{check_data['check_name']}</h2>
            <p><strong>Issues Found:</strong> {check_data['issues_found']}</p>
            """
            
            if check_data['issues_found'] > 0 and not check_data['details'].empty:
                html_content += check_data['details'].head(10).to_html(index=False)
        
        html_content += """
        </body>
        </html>
        """
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Report generated successfully: {output_path}")


def main():
    """Main execution function"""
    
    # Database configuration
    db_config = {
        'dbname': 'healthcare_qa',
        'user': 'postgres',
        'password': 'your_password',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Initialize checker
    checker = HealthcareQualityChecker(db_config)
    
    # Run all quality checks
    results = checker.run_all_checks()
    
    # Generate report
    checker.generate_quality_report(results, 'outputs/quality_report.html')
    
    # Save results as JSON
    with open('outputs/quality_check_results.json', 'w') as f:
        # Convert DataFrames to dict for JSON serialization
        json_results = results.copy()
        for check_name in json_results['checks']:
            if 'details' in json_results['checks'][check_name]:
                json_results['checks'][check_name]['details'] = \
                    json_results['checks'][check_name]['details'].to_dict('records')
        
        json.dump(json_results, f, indent=2, default=str)
    
    logger.info("Quality assurance process completed successfully!")


if __name__ == '__main__':
    main()
