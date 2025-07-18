import json
import random
import uuid
import os
from datetime import datetime, timedelta

class UserGenerator:
    def __init__(self, num_users=1000):
        self.num_users = num_users
        self.users = []
        self.generate_users()
        
    def generate_users(self):
        """Generate a list of user profiles"""
        
        # Define possible plans
        plans = [
            {"name": "Basic", "data_limit_gb": 5, "voice_minutes": 500, "sms_count": 1000},
            {"name": "Standard", "data_limit_gb": 15, "voice_minutes": 1000, "sms_count": 2000},
            {"name": "Premium", "data_limit_gb": 50, "voice_minutes": 5000, "sms_count": 5000},
            {"name": "Unlimited", "data_limit_gb": 100, "voice_minutes": 10000, "sms_count": 10000}
        ]
        
        # Define valid city-state combinations
        city_state_map = [
            {"city": "New York", "state": "NY"},
            {"city": "Los Angeles", "state": "CA"},
            {"city": "Chicago", "state": "IL"},
            {"city": "Houston", "state": "TX"},
            {"city": "Phoenix", "state": "AZ"},
            {"city": "Philadelphia", "state": "PA"},
            {"city": "San Antonio", "state": "TX"},
            {"city": "San Diego", "state": "CA"},
            {"city": "Dallas", "state": "TX"},
            {"city": "San Jose", "state": "CA"},
            {"city": "Austin", "state": "TX"},
            {"city": "Jacksonville", "state": "FL"},
            {"city": "Fort Worth", "state": "TX"},
            {"city": "Columbus", "state": "OH"},
            {"city": "Indianapolis", "state": "IN"},
            {"city": "Charlotte", "state": "NC"},
            {"city": "Seattle", "state": "WA"},
            {"city": "Denver", "state": "CO"},
            {"city": "Boston", "state": "MA"},
            {"city": "Portland", "state": "OR"}
        ]
        
        # Generate users
        for i in range(self.num_users):
            user_id = str(uuid.uuid4())
            msisdn = f"1{random.randint(2000000000, 9999999999)}"  # 11-digit phone number
            imsi = f"310150{random.randint(10000000, 99999999)}"
            imei = f"35824{random.randint(1000000000, 9999999999)}"
            
            # Select a random plan
            plan = random.choice(plans)
            
            # Select a random city-state combination
            location = random.choice(city_state_map)
            
            # Generate a random registration date within the last 2 years
            reg_date = datetime.now() - timedelta(days=random.randint(1, 730))
            
            user = {
                "user_id": user_id,
                "msisdn": msisdn,
                "imsi": imsi,
                "imei": imei,
                "plan_name": plan["name"],
                "data_limit_gb": plan["data_limit_gb"],
                "voice_minutes": plan["voice_minutes"],
                "sms_count": plan["sms_count"],
                "registration_date": reg_date.strftime("%Y-%m-%d"),
                "active": random.random() > 0.05,  # 95% of users are active
                "location": {
                    "city": location["city"],
                    "state": location["state"]
                }
            }
            
            self.users.append(user)
    
    def get_users(self):
        """Return the list of users"""
        return self.users
    
    def get_user_by_index(self, index):
        """Return a specific user by index"""
        if index < len(self.users):
            return self.users[index]
        return None
    
    def get_random_user(self):
        """Return a random user"""
        return random.choice(self.users)
    
    def save_users_to_file(self, filename="users.json"):
        """Save users to a JSON file"""
        with open(filename, 'w') as f:
            json.dump(self.users, f, indent=2)
        print(f"Saved {len(self.users)} users to {filename}")
    
    @classmethod
    def load_from_file(cls, filename="users.json"):
        """Load users from a JSON file"""
        if not os.path.exists(filename):
            print(f"File {filename} not found. Creating new users.")
            return cls()
        
        instance = cls(num_users=0)  # Create empty instance
        
        with open(filename, 'r') as f:
            instance.users = json.load(f)
            instance.num_users = len(instance.users)
        
        print(f"Loaded {instance.num_users} users from {filename}")
        return instance

if __name__ == "__main__":
    # Example usage
    generator = UserGenerator(num_users=1000)
    generator.save_users_to_file()
    
    # Print a sample user
    print("Sample user:")
    print(json.dumps(generator.get_random_user(), indent=2))
    
    # Test loading users from file
    loaded_generator = UserGenerator.load_from_file()
    print(f"Loaded {len(loaded_generator.get_users())} users from file")
