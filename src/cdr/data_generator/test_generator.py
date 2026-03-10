#!/usr/bin/env python3
"""
Test script to demonstrate new CDR types and anomaly generation
"""
import json
from user_generator import UserGenerator
from cdr_generator import CDRGenerator
from anomaly_generator import AnomalyGenerator

def main():
    print("=== Telco CDR Generator Test ===\n")
    
    # Generate users
    print("1. Generating 100 users...")
    user_gen = UserGenerator(num_users=100)
    print(f"   ✓ Generated {len(user_gen.users)} users\n")
    
    # Generate normal CDRs
    print("2. Generating 50 normal CDRs...")
    cdr_gen = CDRGenerator(user_gen)
    normal_cdrs = cdr_gen.generate_cdrs(count=50)
    
    # Count CDR types
    type_counts = {}
    for cdr in normal_cdrs:
        cdr_type = cdr["cdr_type"]
        type_counts[cdr_type] = type_counts.get(cdr_type, 0) + 1
    
    print("   Normal CDR Distribution:")
    for cdr_type, count in sorted(type_counts.items()):
        print(f"   - {cdr_type}: {count} ({count/len(normal_cdrs)*100:.1f}%)")
    print()
    
    # Generate CDRs with anomalies
    print("3. Generating 50 CDRs with 20% anomaly rate...")
    anomaly_gen = AnomalyGenerator(cdr_gen, anomaly_rate=0.2)
    anomaly_cdrs = anomaly_gen.generate_cdrs(count=50)
    
    # Count anomalies
    anomaly_count = sum(1 for cdr in anomaly_cdrs if cdr.get("is_anomaly", False))
    print(f"   ✓ Generated {anomaly_count} anomalies ({anomaly_count/len(anomaly_cdrs)*100:.1f}%)\n")
    
    # Show anomaly breakdown
    anomaly_types = {}
    for cdr in anomaly_cdrs:
        if cdr.get("is_anomaly", False):
            anomaly_type = cdr.get("anomaly_type", "unknown")
            anomaly_types[anomaly_type] = anomaly_types.get(anomaly_type, 0) + 1
    
    if anomaly_types:
        print("   Anomaly Type Breakdown:")
        for anomaly_type, count in sorted(anomaly_types.items()):
            print(f"   - {anomaly_type}: {count}")
        print()
    
    # Show example anomalies
    print("4. Example Anomalies:\n")
    examples_shown = 0
    for cdr in anomaly_cdrs:
        if cdr.get("is_anomaly", False) and examples_shown < 3:
            print(f"   [{cdr['cdr_type'].upper()}] {cdr.get('anomaly_type', 'unknown')}")
            
            # Show relevant fields based on CDR type
            if cdr['cdr_type'] == 'voice':
                print(f"   - Duration: {cdr.get('callDuration', 0)}s")
                print(f"   - Termination: {cdr.get('causeForTermination', 'N/A')}")
            elif cdr['cdr_type'] == 'data_usage':
                print(f"   - Duration: {cdr.get('duration', 0)}s")
                print(f"   - Download: {cdr.get('dataVolumeDownlink', 0):,} bytes")
                print(f"   - Closing: {cdr.get('causeForRecordClosing', 'N/A')}")
            elif cdr['cdr_type'] == 'voip':
                print(f"   - Packet Loss: {cdr.get('packetLoss', 0)}%")
                print(f"   - Jitter: {cdr.get('jitter', 0)}ms")
                print(f"   - Quality: {cdr.get('callQualityIndicator', 0)}/5")
            elif cdr['cdr_type'] == 'sms' or cdr['cdr_type'] == 'mms':
                print(f"   - Status: {cdr.get('deliveryStatus', 'N/A')}")
                print(f"   - Reason: {cdr.get('failureReason', 'N/A')}")
            
            print()
            examples_shown += 1
    
    # Save samples
    print("5. Saving sample files...")
    with open("sample_normal_cdrs.json", "w") as f:
        json.dump(normal_cdrs[:10], f, indent=2)
    print("   ✓ Saved sample_normal_cdrs.json (10 records)")
    
    with open("sample_anomaly_cdrs.json", "w") as f:
        anomaly_samples = [cdr for cdr in anomaly_cdrs if cdr.get("is_anomaly", False)][:10]
        json.dump(anomaly_samples, f, indent=2)
    print(f"   ✓ Saved sample_anomaly_cdrs.json ({len(anomaly_samples)} anomalies)")
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main()
