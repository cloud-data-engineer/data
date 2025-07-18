import json
import time
import argparse
import os
from confluent_kafka import Producer
from user_generator import UserGenerator

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def main():
    parser = argparse.ArgumentParser(description='Export Telco Users to Kafka')
    parser.add_argument('--bootstrap-servers', required=True, help='Kafka bootstrap servers')
    parser.add_argument('--sasl-username', help='SASL username for authentication')
    parser.add_argument('--sasl-password', help='SASL password for authentication')
    parser.add_argument('--topic', default='telco-users', help='Kafka topic for users')
    parser.add_argument('--num-users', type=int, default=1000, help='Number of users to generate')
    parser.add_argument('--users-file', default='users.json', help='File to save/load users from')
    parser.add_argument('--force-new-users', action='store_true', help='Force generation of new users even if file exists')
    
    args = parser.parse_args()
    
    # Configure Kafka producer
    config = {
        'bootstrap.servers': args.bootstrap_servers,
        'client.id': 'telco-user-producer'
    }
    
    # Add SASL authentication if provided
    if args.sasl_username and args.sasl_password:
        config.update({
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': args.sasl_username,
            'sasl.password': args.sasl_password
        })
    
    producer = Producer(config)
    
    # Generate or load users
    if args.force_new_users or not os.path.exists(args.users_file):
        print(f"Generating {args.num_users} users...")
        user_gen = UserGenerator(num_users=args.num_users)
        # Save users for future use
        user_gen.save_users_to_file(args.users_file)
    else:
        print(f"Loading users from {args.users_file}...")
        user_gen = UserGenerator.load_from_file(args.users_file)
    
    users = user_gen.get_users()
    
    # Send users to Kafka
    print(f"Sending {len(users)} users to Kafka topic: {args.topic}")
    for user in users:
        user_json = json.dumps(user)
        producer.produce(
            topic=args.topic,
            key=user["user_id"],
            value=user_json,
            callback=delivery_report
        )
        producer.poll(0)
    
    # Make sure all messages are sent
    remaining = producer.flush(10)
    if remaining > 0:
        print(f"Failed to flush {remaining} messages")
    else:
        print(f"Successfully sent {len(users)} users to Kafka")

if __name__ == "__main__":
    main()
