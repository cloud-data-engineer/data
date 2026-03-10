#!/usr/bin/env python3
"""
Quick setup script for topics and schemas only
Use this when you already have a Confluent Cloud cluster
"""

import subprocess
import sys
import os

def run_cmd(cmd):
    """Run command and check result"""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    print(result.stdout)
    return True

def create_topics():
    """Create all Kafka topics"""
    print("\n=== Creating Topics ===")
    topics = [
        "telco-users",
        "telco-voice-cdrs",
        "telco-data-cdrs",
        "telco-sms-cdrs",
        "telco-voip-cdrs",
        "telco-ims-cdrs",
        "telco-mms-cdrs",
        "telco-roaming-cdrs",
        "telco-wifi-calling-cdrs"
    ]
    
    for topic in topics:
        print(f"\nCreating: {topic}")
        run_cmd([
            "confluent", "kafka", "topic", "create", topic,
            "--partitions", "3",
            "--config", "retention.ms=604800000"
        ])

def register_schemas():
    """Register all Avro schemas"""
    print("\n=== Registering Schemas ===")
    
    schema_dir = "schemas"
    schemas = [
        ("telco-users-key", "user_key.avsc"),
        ("telco-users-value", "user_value.avsc"),
        ("telco-voice-cdrs-key", "cdr_key.avsc"),
        ("telco-voice-cdrs-value", "voice_cdr_value.avsc"),
        ("telco-data-cdrs-key", "cdr_key.avsc"),
        ("telco-data-cdrs-value", "data_cdr_value.avsc"),
        ("telco-sms-cdrs-key", "cdr_key.avsc"),
        ("telco-sms-cdrs-value", "sms_cdr_value.avsc"),
        ("telco-voip-cdrs-key", "cdr_key.avsc"),
        ("telco-voip-cdrs-value", "voip_cdr_value.avsc"),
        ("telco-ims-cdrs-key", "cdr_key.avsc"),
        ("telco-ims-cdrs-value", "ims_cdr_value.avsc"),
        ("telco-mms-cdrs-key", "cdr_key.avsc"),
        ("telco-mms-cdrs-value", "mms_cdr_value.avsc"),
        ("telco-roaming-cdrs-key", "cdr_key.avsc"),
        ("telco-roaming-cdrs-value", "roaming_cdr_value.avsc"),
        ("telco-wifi-calling-cdrs-key", "cdr_key.avsc"),
        ("telco-wifi-calling-cdrs-value", "wifi_calling_cdr_value.avsc")
    ]
    
    for subject, schema_file in schemas:
        schema_path = os.path.join(schema_dir, schema_file)
        if not os.path.exists(schema_path):
            print(f"Warning: {schema_path} not found")
            continue
        
        print(f"\nRegistering: {subject}")
        run_cmd([
            "confluent", "schema-registry", "schema", "create",
            "--subject", subject,
            "--schema", schema_path,
            "--type", "AVRO"
        ])

def main():
    print("=== Telco CDR - Quick Setup ===")
    print("\nMake sure you have:")
    print("1. Confluent CLI installed")
    print("2. Logged in: confluent login")
    print("3. Selected environment: confluent environment use <env-id>")
    print("4. Selected cluster: confluent kafka cluster use <cluster-id>")
    
    response = input("\nReady to proceed? (y/n): ")
    if response.lower() != 'y':
        print("Aborted")
        sys.exit(0)
    
    create_topics()
    register_schemas()
    
    print("\n=== Setup Complete ===")

if __name__ == "__main__":
    main()
