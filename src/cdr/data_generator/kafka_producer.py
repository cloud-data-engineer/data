import json
import time
import os
import argparse
from confluent_kafka import Producer
from user_generator import UserGenerator
from cdr_generator import CDRGenerator

class KafkaProducer:
    def __init__(self, bootstrap_servers, sasl_username=None, sasl_password=None):
        """Initialize Kafka producer with optional SASL authentication"""
        config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'telco-cdr-producer'
        }
        
        # Add SASL authentication if provided
        if sasl_username and sasl_password:
            config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': sasl_username,
                'sasl.password': sasl_password
            })
            
        self.producer = Producer(config)
        self.topics = {
            "voice": "telco-voice-cdrs",
            "data_usage": "telco-data-cdrs",
            "sms": "telco-sms-cdrs",
            "voip": "telco-voip-cdrs",
            "ims": "telco-ims-cdrs"
        }
        
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def send_cdr(self, cdr):
        """Send a single CDR to the appropriate Kafka topic"""
        cdr_type = cdr["cdr_type"]
        topic = self.topics.get(cdr_type)
        
        if not topic:
            print(f"Unknown CDR type: {cdr_type}")
            return
        
        # Convert CDR to JSON string
        cdr_json = json.dumps(cdr)
        
        # Send to Kafka
        self.producer.produce(
            topic=topic,
            key=cdr.get("user_id", "unknown"),
            value=cdr_json,
            callback=self.delivery_report
        )
        
        # Trigger any available delivery reports
        self.producer.poll(0)
    
    def send_cdrs_continuously(self, cdr_generator, interval=1.0, max_count=None):
        """Continuously send CDRs at the specified interval"""
        count = 0
        try:
            while max_count is None or count < max_count:
                cdr = cdr_generator.generate_random_cdr()
                self.send_cdr(cdr)
                count += 1
                
                if count % 10 == 0:
                    print(f"Sent {count} CDRs")
                
                # Wait for the specified interval
                time.sleep(interval)
                
                # Flush producer queue every 100 messages
                if count % 100 == 0:
                    self.producer.flush()
                    
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            # Make sure all messages are sent
            remaining = self.producer.flush(10)
            if remaining > 0:
                print(f"Failed to flush {remaining} messages")
            print(f"Total CDRs sent: {count}")

def main():
    parser = argparse.ArgumentParser(description='Telco CDR Kafka Producer')
    parser.add_argument('--bootstrap-servers', required=True, help='Kafka bootstrap servers')
    parser.add_argument('--sasl-username', help='SASL username for authentication')
    parser.add_argument('--sasl-password', help='SASL password for authentication')
    parser.add_argument('--interval', type=float, default=1.0, help='Interval between messages in seconds')
    parser.add_argument('--max-count', type=int, help='Maximum number of CDRs to send (default: unlimited)')
    parser.add_argument('--num-users', type=int, default=1000, help='Number of users to generate if users file not found')
    parser.add_argument('--users-file', default='users.json', help='File to load users from')
    
    args = parser.parse_args()
    
    # Create user and CDR generators
    if os.path.exists(args.users_file):
        print(f"Loading users from {args.users_file}...")
        user_gen = UserGenerator.load_from_file(args.users_file)
    else:
        print(f"Users file {args.users_file} not found. Generating {args.num_users} new users...")
        user_gen = UserGenerator(num_users=args.num_users)
        user_gen.save_users_to_file(args.users_file)
    
    cdr_gen = CDRGenerator(user_gen)
    
    # Create and run Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        sasl_username=args.sasl_username,
        sasl_password=args.sasl_password
    )
    
    print(f"Starting to send CDRs to Kafka at {args.bootstrap_servers}")
    producer.send_cdrs_continuously(cdr_gen, interval=args.interval, max_count=args.max_count)

if __name__ == "__main__":
    main()
