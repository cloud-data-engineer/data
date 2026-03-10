#!/usr/bin/env python3
"""
Confluent Cloud Setup Automation

This script automates the setup of Confluent Cloud resources:
1. Creates environment (if not exists)
2. Creates Kafka cluster (if not exists)
3. Creates API keys
4. Creates topics
5. Registers Avro schemas

Prerequisites:
- Confluent CLI installed (https://docs.confluent.io/confluent-cli/current/install.html)
- Confluent Cloud account with API credentials
"""

import subprocess
import json
import sys
import os
import argparse
import time

class ConfluentSetup:
    def __init__(self, cloud="aws", region="us-east-1"):
        self.cloud = cloud
        self.region = region
        self.env_id = None
        self.cluster_id = None
        self.api_key = None
        self.api_secret = None
        self.schema_registry_id = None
        self.sr_api_key = None
        self.sr_api_secret = None
        
    def run_command(self, cmd, capture_output=True):
        """Run shell command and return output"""
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=capture_output, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return None
        return result.stdout.strip() if capture_output else None
    
    def login(self):
        """Login to Confluent Cloud"""
        print("\n=== Logging in to Confluent Cloud ===")
        result = self.run_command(["confluent", "login", "--save"])
        if result is None:
            print("Login failed. Please check your credentials.")
            sys.exit(1)
        print("✓ Logged in successfully")
    
    def get_or_create_environment(self, env_name="telco-cdr-env"):
        """Get existing or create new environment"""
        print(f"\n=== Setting up environment: {env_name} ===")
        
        # List environments
        output = self.run_command(["confluent", "environment", "list", "-o", "json"])
        if output:
            envs = json.loads(output)
            for env in envs:
                if env.get("name") == env_name:
                    self.env_id = env.get("id")
                    print(f"✓ Found existing environment: {self.env_id}")
                    self.run_command(["confluent", "environment", "use", self.env_id])
                    return self.env_id
        
        # Create new environment
        print(f"Creating new environment: {env_name}")
        output = self.run_command(["confluent", "environment", "create", env_name, "-o", "json"])
        if output:
            env = json.loads(output)
            self.env_id = env.get("id")
            print(f"✓ Created environment: {self.env_id}")
            self.run_command(["confluent", "environment", "use", self.env_id])
            return self.env_id
        
        print("Failed to create environment")
        sys.exit(1)
    
    def get_or_create_cluster(self, cluster_name="telco-cdr-cluster"):
        """Get existing or create new Kafka cluster"""
        print(f"\n=== Setting up Kafka cluster: {cluster_name} ===")
        
        # List clusters
        output = self.run_command(["confluent", "kafka", "cluster", "list", "-o", "json"])
        if output:
            clusters = json.loads(output)
            for cluster in clusters:
                if cluster.get("name") == cluster_name:
                    self.cluster_id = cluster.get("id")
                    print(f"✓ Found existing cluster: {self.cluster_id}")
                    self.run_command(["confluent", "kafka", "cluster", "use", self.cluster_id])
                    return self.cluster_id
        
        # Create new cluster (Basic tier)
        print(f"Creating new Basic cluster: {cluster_name}")
        print(f"Cloud: {self.cloud}, Region: {self.region}")
        output = self.run_command([
            "confluent", "kafka", "cluster", "create", cluster_name,
            "--cloud", self.cloud,
            "--region", self.region,
            "--type", "basic",
            "-o", "json"
        ])
        
        if output:
            cluster = json.loads(output)
            self.cluster_id = cluster.get("id")
            print(f"✓ Created cluster: {self.cluster_id}")
            print("Waiting for cluster to be ready...")
            time.sleep(30)  # Wait for cluster provisioning
            self.run_command(["confluent", "kafka", "cluster", "use", self.cluster_id])
            return self.cluster_id
        
        print("Failed to create cluster")
        sys.exit(1)
    
    def create_api_key(self):
        """Create API key for Kafka cluster"""
        print("\n=== Creating API key ===")
        
        output = self.run_command([
            "confluent", "api-key", "create",
            "--resource", self.cluster_id,
            "-o", "json"
        ])
        
        if output:
            key_data = json.loads(output)
            self.api_key = key_data.get("key")
            self.api_secret = key_data.get("secret")
            print(f"✓ Created API key: {self.api_key}")
            
            # Use the API key
            self.run_command([
                "confluent", "api-key", "use",
                self.api_key,
                "--resource", self.cluster_id
            ])
            
            return self.api_key, self.api_secret
        
        print("Failed to create API key")
        sys.exit(1)
    
    def enable_schema_registry(self):
        """Enable Schema Registry"""
        print("\n=== Setting up Schema Registry ===")
        
        # Check if Schema Registry exists
        output = self.run_command(["confluent", "schema-registry", "cluster", "describe", "-o", "json"])
        if output:
            sr = json.loads(output)
            self.schema_registry_id = sr.get("cluster_id")
            print(f"✓ Found existing Schema Registry: {self.schema_registry_id}")
        else:
            # Enable Schema Registry
            print("Enabling Schema Registry...")
            output = self.run_command([
                "confluent", "schema-registry", "cluster", "enable",
                "--cloud", self.cloud,
                "--geo", "us",
                "-o", "json"
            ])
            if output:
                sr = json.loads(output)
                self.schema_registry_id = sr.get("cluster_id")
                print(f"✓ Enabled Schema Registry: {self.schema_registry_id}")
                time.sleep(10)
        
        # Create Schema Registry API key
        print("Creating Schema Registry API key...")
        output = self.run_command([
            "confluent", "api-key", "create",
            "--resource", self.schema_registry_id,
            "-o", "json"
        ])
        
        if output:
            key_data = json.loads(output)
            self.sr_api_key = key_data.get("key")
            self.sr_api_secret = key_data.get("secret")
            print(f"✓ Created Schema Registry API key: {self.sr_api_key}")
            return self.sr_api_key, self.sr_api_secret
        
        print("Warning: Failed to create Schema Registry API key")
        return None, None
    
    def create_topics(self):
        """Create Kafka topics"""
        print("\n=== Creating Kafka topics ===")
        
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
            print(f"Creating topic: {topic}")
            self.run_command([
                "confluent", "kafka", "topic", "create", topic,
                "--partitions", "3",
                "--config", "retention.ms=604800000"  # 7 days
            ])
        
        print("✓ All topics created")
    
    def register_schemas(self, schema_dir="schemas"):
        """Register Avro schemas"""
        print("\n=== Registering Avro schemas ===")
        
        if not self.sr_api_key:
            print("Warning: No Schema Registry API key available, skipping schema registration")
            return
        
        # Get Schema Registry endpoint
        output = self.run_command(["confluent", "schema-registry", "cluster", "describe", "-o", "json"])
        if not output:
            print("Failed to get Schema Registry endpoint")
            return
        
        sr_info = json.loads(output)
        sr_endpoint = sr_info.get("endpoint_url")
        
        schema_mappings = [
            ("telco-users-key", f"{schema_dir}/user_key.avsc"),
            ("telco-users-value", f"{schema_dir}/user_value.avsc"),
            ("telco-voice-cdrs-key", f"{schema_dir}/cdr_key.avsc"),
            ("telco-voice-cdrs-value", f"{schema_dir}/voice_cdr_value.avsc"),
            ("telco-data-cdrs-key", f"{schema_dir}/cdr_key.avsc"),
            ("telco-data-cdrs-value", f"{schema_dir}/data_cdr_value.avsc"),
            ("telco-sms-cdrs-key", f"{schema_dir}/cdr_key.avsc"),
            ("telco-sms-cdrs-value", f"{schema_dir}/sms_cdr_value.avsc"),
            ("telco-voip-cdrs-key", f"{schema_dir}/cdr_key.avsc"),
            ("telco-voip-cdrs-value", f"{schema_dir}/voip_cdr_value.avsc"),
            ("telco-ims-cdrs-key", f"{schema_dir}/cdr_key.avsc"),
            ("telco-ims-cdrs-value", f"{schema_dir}/ims_cdr_value.avsc"),
            ("telco-mms-cdrs-key", f"{schema_dir}/cdr_key.avsc"),
            ("telco-mms-cdrs-value", f"{schema_dir}/mms_cdr_value.avsc"),
            ("telco-roaming-cdrs-key", f"{schema_dir}/cdr_key.avsc"),
            ("telco-roaming-cdrs-value", f"{schema_dir}/roaming_cdr_value.avsc"),
            ("telco-wifi-calling-cdrs-key", f"{schema_dir}/cdr_key.avsc"),
            ("telco-wifi-calling-cdrs-value", f"{schema_dir}/wifi_calling_cdr_value.avsc")
        ]
        
        for subject, schema_file in schema_mappings:
            if not os.path.exists(schema_file):
                print(f"Warning: Schema file not found: {schema_file}")
                continue
            
            print(f"Registering schema: {subject}")
            self.run_command([
                "confluent", "schema-registry", "schema", "create",
                "--subject", subject,
                "--schema", schema_file,
                "--type", "AVRO"
            ])
        
        print("✓ All schemas registered")
    
    def save_config(self, output_file="confluent_config.json"):
        """Save configuration to file"""
        print(f"\n=== Saving configuration to {output_file} ===")
        
        # Get cluster bootstrap servers
        output = self.run_command(["confluent", "kafka", "cluster", "describe", "-o", "json"])
        bootstrap_servers = None
        if output:
            cluster_info = json.loads(output)
            bootstrap_servers = cluster_info.get("endpoint")
            if bootstrap_servers and bootstrap_servers.startswith("SASL_SSL://"):
                bootstrap_servers = bootstrap_servers.replace("SASL_SSL://", "")
        
        config = {
            "environment_id": self.env_id,
            "cluster_id": self.cluster_id,
            "bootstrap_servers": bootstrap_servers,
            "api_key": self.api_key,
            "api_secret": self.api_secret,
            "schema_registry_id": self.schema_registry_id,
            "schema_registry_api_key": self.sr_api_key,
            "schema_registry_api_secret": self.sr_api_secret,
            "cloud": self.cloud,
            "region": self.region
        }
        
        with open(output_file, "w") as f:
            json.dump(config, f, indent=2)
        
        print(f"✓ Configuration saved to {output_file}")
        print("\nYou can use these credentials with:")
        print(f"  export CONFLUENT_KAFKA_BOOTSTRAP_SERVERS='{bootstrap_servers}'")
        print(f"  export CONFLUENT_API_KEY='{self.api_key}'")
        print(f"  export CONFLUENT_API_SECRET='{self.api_secret}'")

def main():
    parser = argparse.ArgumentParser(description="Automate Confluent Cloud setup for Telco CDR project")
    parser.add_argument("--env-name", default="telco-cdr-env", help="Environment name")
    parser.add_argument("--cluster-name", default="telco-cdr-cluster", help="Cluster name")
    parser.add_argument("--cloud", default="aws", choices=["aws", "gcp", "azure"], help="Cloud provider")
    parser.add_argument("--region", default="us-east-1", help="Cloud region")
    parser.add_argument("--skip-login", action="store_true", help="Skip login (if already logged in)")
    parser.add_argument("--schema-dir", default="schemas", help="Directory containing Avro schemas")
    
    args = parser.parse_args()
    
    setup = ConfluentSetup(cloud=args.cloud, region=args.region)
    
    if not args.skip_login:
        setup.login()
    
    setup.get_or_create_environment(args.env_name)
    setup.get_or_create_cluster(args.cluster_name)
    setup.create_api_key()
    setup.enable_schema_registry()
    setup.create_topics()
    setup.register_schemas(args.schema_dir)
    setup.save_config()
    
    print("\n=== Setup Complete ===")
    print("Your Confluent Cloud environment is ready!")

if __name__ == "__main__":
    main()
