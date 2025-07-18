import json
import random
import time
from datetime import datetime, timedelta
from user_generator import UserGenerator

class CDRGenerator:
    def __init__(self, user_generator):
        self.user_generator = user_generator
        self.cdr_types = ["voice", "data_usage", "sms", "voip", "ims"]
        self.cdr_type_weights = [0.4, 0.3, 0.15, 0.1, 0.05]  # Probability weights for each type
        
    def generate_timestamp(self, days_back=7):
        """Generate a random timestamp within the last N days"""
        now = datetime.now()
        random_days = random.uniform(0, days_back)
        random_time = now - timedelta(days=random_days)
        return int(random_time.timestamp())
    
    def generate_voice_cdr(self, user):
        """Generate a voice CDR"""
        start_time = self.generate_timestamp()
        duration = random.randint(10, 3600)  # 10 seconds to 1 hour
        end_time = start_time + duration
        
        # Get another random user for the called party
        called_user = self.user_generator.get_random_user()
        
        return {
            "cdr_type": "voice",
            "callEventStartTime": start_time,
            "callEventEndTime": end_time,
            "callDuration": duration,
            "callReferenceNumber": f"CRN{random.randint(1, 10000)}",
            "callingPartyNumber": user["msisdn"],
            "calledPartyNumber": called_user["msisdn"],
            "imsi": user["imsi"],
            "imei": user["imei"],
            "mscID": f"MSC{random.randint(1, 20)}",
            "lac": str(random.randint(1000, 9999)),
            "ci": str(random.randint(1000, 9999)),
            "causeForTermination": str(random.randint(1, 20)),
            "chargeID": f"CHG{random.randint(1, 10000)}",
            "callChargeableDuration": duration,
            "tariffClass": random.choice(["standard_voice", "premium_voice", "international", "roaming"]),
            "user_id": user["user_id"]
        }
    
    def generate_data_cdr(self, user):
        """Generate a data usage CDR"""
        start_time = self.generate_timestamp()
        duration = random.randint(60, 7200)  # 1 minute to 2 hours
        end_time = start_time + duration
        
        # Data volumes - biased towards more download than upload
        uplink = random.randint(10000, 5000000)  # 10KB to 5MB
        downlink = random.randint(50000, 50000000)  # 50KB to 50MB
        
        return {
            "cdr_type": "data_usage",
            "recordType": random.randint(40, 60),
            "recordOpeningTime": start_time,
            "recordClosingTime": end_time,
            "duration": duration,
            "imsi": user["imsi"],
            "imei": user["imei"],
            "msisdn": user["msisdn"],
            "dataVolumeUplink": uplink,
            "dataVolumeDownlink": downlink,
            "servingNodeAddress": f"10.10.10.{random.randint(1, 254)}",
            "apn": "internet.example.com",
            "pdnType": random.choice(["IPv4", "IPv6", "IPv4v6"]),
            "ratType": random.choice(["UTRAN", "GERAN", "WLAN", "GAN", "HSPA", "EUTRAN", "NR"]),
            "chargingID": f"CHG{random.randint(1, 10000)}",
            "qci": random.randint(1, 9),
            "user_id": user["user_id"]
        }
    
    def generate_sms_cdr(self, user):
        """Generate an SMS CDR"""
        timestamp = self.generate_timestamp()
        
        # Get another random user for the recipient
        recipient_user = self.user_generator.get_random_user()
        
        return {
            "cdr_type": "sms",
            "eventTimestamp": timestamp,
            "smsReferenceNumber": f"SMS{random.randint(1, 100000)}",
            "originatingNumber": user["msisdn"],
            "destinationNumber": recipient_user["msisdn"],
            "imsi": user["imsi"],
            "smscAddress": f"smsc{random.randint(1, 10)}.example.com",
            "messageSize": random.randint(1, 160),
            "deliveryStatus": random.choice(["delivered", "failed", "pending"]),
            "chargeAmount": round(random.uniform(0.01, 0.1), 2),
            "user_id": user["user_id"]
        }
    
    def generate_voip_cdr(self, user):
        """Generate a VoIP CDR"""
        start_time = self.generate_timestamp()
        duration = random.randint(30, 3600)  # 30 seconds to 1 hour
        end_time = start_time + duration
        
        # Get another random user for the called party
        called_user = self.user_generator.get_random_user()
        
        return {
            "cdr_type": "voip",
            "sessionStartTime": start_time,
            "sessionEndTime": end_time,
            "sessionDuration": duration,
            "callingPartyURI": f"sip:{user['msisdn']}@example.com",
            "calledPartyURI": f"sip:{called_user['msisdn']}@example.com",
            "codec": random.choice(["G.711", "G.722", "G.729", "OPUS", "AMR-WB"]),
            "callQualityIndicator": random.randint(1, 5),
            "bytesTransferred": random.randint(100000, 10000000),
            "packetLoss": round(random.uniform(0, 5), 2),
            "jitter": round(random.uniform(0, 100), 2),
            "latency": round(random.uniform(10, 500), 2),
            "chargeID": f"VOIP{random.randint(1, 10000)}",
            "user_id": user["user_id"]
        }
    
    def generate_ims_cdr(self, user):
        """Generate an IMS CDR"""
        start_time = self.generate_timestamp()
        duration = random.randint(10, 1800)  # 10 seconds to 30 minutes
        end_time = start_time + duration
        
        # Get another random user for the called party
        called_user = self.user_generator.get_random_user()
        
        return {
            "cdr_type": "ims",
            "sessionStartTime": start_time,
            "sessionEndTime": end_time,
            "sessionDuration": duration,
            "originatingURI": f"sip:{user['msisdn']}@ims.example.com",
            "destinationURI": f"sip:{called_user['msisdn']}@ims.example.com",
            "serviceType": random.choice(["voice", "video", "messaging", "presence", "file_transfer"]),
            "mediaTypes": random.choice(["audio", "video", "audio+video", "text", "application"]),
            "networkType": random.choice(["LTE", "5G", "WiFi"]),
            "qosClass": random.choice(["conversational", "streaming", "interactive", "background"]),
            "chargeID": f"IMS{random.randint(1, 10000)}",
            "user_id": user["user_id"]
        }
    
    def generate_random_cdr(self):
        """Generate a random CDR based on weighted probabilities"""
        # Select a random CDR type based on weights
        cdr_type = random.choices(self.cdr_types, weights=self.cdr_type_weights, k=1)[0]
        
        # Get a random user
        user = self.user_generator.get_random_user()
        
        # Generate the appropriate CDR
        if cdr_type == "voice":
            return self.generate_voice_cdr(user)
        elif cdr_type == "data_usage":
            return self.generate_data_cdr(user)
        elif cdr_type == "sms":
            return self.generate_sms_cdr(user)
        elif cdr_type == "voip":
            return self.generate_voip_cdr(user)
        elif cdr_type == "ims":
            return self.generate_ims_cdr(user)
    
    def generate_cdrs(self, count=100):
        """Generate multiple CDRs"""
        cdrs = []
        for _ in range(count):
            cdrs.append(self.generate_random_cdr())
        return cdrs
    
    def save_cdrs_to_file(self, count=100, filename="cdrs.json"):
        """Save generated CDRs to a JSON file"""
        cdrs = self.generate_cdrs(count)
        with open(filename, 'w') as f:
            json.dump(cdrs, f, indent=2)
        print(f"Saved {len(cdrs)} CDRs to {filename}")
        return cdrs

if __name__ == "__main__":
    # Example usage
    user_gen = UserGenerator(num_users=1000)
    cdr_gen = CDRGenerator(user_gen)
    
    # Generate and save 100 CDRs
    cdrs = cdr_gen.save_cdrs_to_file(count=100)
    
    # Print distribution of CDR types
    cdr_types = [cdr["cdr_type"] for cdr in cdrs]
    type_counts = {cdr_type: cdr_types.count(cdr_type) for cdr_type in set(cdr_types)}
    print("CDR Type Distribution:")
    for cdr_type, count in type_counts.items():
        print(f"{cdr_type}: {count} ({count/len(cdrs)*100:.1f}%)")
