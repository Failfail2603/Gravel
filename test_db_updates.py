#!/usr/bin/env python3
"""
MongoDB User Population Script for Gravel Database
Connects to local MongoDB and ensures 1000 user entries in the gravel_db database.
"""

import pymongo
from datetime import datetime, timedelta
import random
from faker import Faker
from faker.providers import address
import sys
import time

# Initialize Faker for generating realistic data
fake = Faker()
fake.add_provider(address)


def connect_to_mongodb():
    """Connect to local MongoDB instance"""
    try:
        client = pymongo.MongoClient(
            "mongodb://localhost:27017/",
            directConnection=True
        )
        # Test the connection
        client.admin.command('ping')
        print("✓ Successfully connected to MongoDB")
        return client
    except Exception as e:
        print(f"✗ Failed to connect to MongoDB: {e}")
        sys.exit(1)


def generate_user_data():
    """Generate a single user entry with email, birthday, and debitor fields"""
    # Generate a random birthday between 18 and 80 years ago
    start_date = datetime.now() - timedelta(days=80*365)
    end_date = datetime.now() - timedelta(days=18*365)
    random_days = random.randint(0, (end_date - start_date).days)
    birthday = start_date + timedelta(days=random_days)

    return {
        "email": fake.email(),
        "birthday": birthday,
        "debitor": random.randint(1000, 999999),  # Random debitor number
        "role": random.choice(["user", "admin", "moderator"]),
        "address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state(),
            "zip": fake.postcode(),
            "country": fake.country(),
        },
    }


def ensure_users_collection(db, target_count):
    """Ensure the users collection has exactly the target number of entries"""
    collection = db.users

    # Check current count
    current_count = collection.count_documents({})
    print(f"Current user count: {current_count}")

    if current_count >= target_count:
        print(
            f"✓ Collection already has {current_count} users (target: {target_count})")
        return

    # Calculate how many users to add
    users_to_add = target_count - current_count
    print(f"Adding {users_to_add} users to reach target of {target_count}...")

    # Generate and insert users in batches for better performance
    batch_size = 100
    for i in range(0, users_to_add, batch_size):
        batch_count = min(batch_size, users_to_add - i)
        users_batch = [generate_user_data() for _ in range(batch_count)]

        try:
            result = collection.insert_many(users_batch)
            print(
                f"✓ Inserted batch of {len(result.inserted_ids)} users (Progress: {i + batch_count}/{users_to_add})")
        except Exception as e:
            print(f"✗ Error inserting batch: {e}")
            return

    # Verify final count
    final_count = collection.count_documents({})
    print(f"✓ Final user count: {final_count}")


def update_random_users_periodically(db, interval_seconds):
    """Periodically update a random number of users every interval_seconds"""
    collection = db.users

    print(
        f"\nStarting periodic user updates every {interval_seconds} seconds...")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            # Get total user count
            total_users = collection.count_documents({})
            if total_users == 0:
                print("No users found to update")
                time.sleep(interval_seconds)
                continue

            # Determine random number of users to update (1-10% of total, min 1, max 50)
            max_updates = min(50, max(1, total_users // 10))
            num_updates = random.randint(1, max_updates)

            print(f"Updating {num_updates} random users...")

            # Get random users to update
            random_users = list(collection.aggregate([
                {"$sample": {"size": num_updates}}
            ]))

            updates_made = 0
            for user in random_users:
                try:
                    # Randomly choose what to update
                    update_type = random.choice(
                        ["email", "birthday", "debitor"])

                    if update_type == "email":
                        new_data = {"email": fake.email()}
                    elif update_type == "birthday":
                        # Generate new random birthday
                        start_date = datetime.now() - timedelta(days=80*365)
                        end_date = datetime.now() - timedelta(days=18*365)
                        random_days = random.randint(
                            0, (end_date - start_date).days)
                        new_birthday = start_date + timedelta(days=random_days)
                        new_data = {"birthday": new_birthday}
                    else:  # debitor
                        new_data = {"debitor": random.randint(1000, 999999)}

                    # Add timestamp for tracking
                    new_data["last_updated"] = datetime.now()

                    # Update the user
                    result = collection.update_one(
                        {"_id": user["_id"]},
                        {"$set": new_data}
                    )

                    if result.modified_count > 0:
                        updates_made += 1
                        print(
                            f"  ✓ Updated user {user['email']} - changed {update_type}")

                except Exception as e:
                    print(
                        f"  ✗ Failed to update user {user.get('email', 'unknown')}: {e}")

            print(f"Successfully updated {updates_made}/{num_updates} users\n")

            # Wait for next interval
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\n✓ Periodic updates stopped by user")
    except Exception as e:
        print(f"\n✗ Error during periodic updates: {e}")


def main():
    """Main function to execute the script"""
    print("MongoDB User Population Script")
    print("=" * 40)

    # Connect to MongoDB
    client = connect_to_mongodb()

    # Get the gravel_db database
    db = client.gravel_db
    print(f"✓ Using database: gravel_db")

    # Ensure users collection has entries
    ensure_users_collection(db, 500000)

    # Create an index on email for better query performance
    try:
        db.users.create_index("email", unique=True, background=True)
        print("✓ Created unique index on email field")
    except pymongo.errors.DuplicateKeyError:
        print("✓ Index on email already exists")
    except Exception as e:
        print(f"✗ Could not create index: {e}")

    # Display some sample data
    print("\nSample users:")
    print("-" * 20)
    sample_users = db.users.find().limit(3)
    for i, user in enumerate(sample_users, 1):
        print(f"{i}. Email: {user['email']}")
        print(f"   Birthday: {user['birthday'].strftime('%Y-%m-%d')}")
        print(f"   Debitor: {user['debitor']}")
        print(f"   Role: {user['role']}")
        print()

    # Ask if user wants to start periodic updates
    print("Options:")
    print("1. Exit script")
    print("2. Start periodic user updates")

    try:
        choice = input("\nEnter your choice (1-2): ").strip()

        if choice == "2":
            update_interval = input(
                "\nEnter update interval in milliseconds: ").strip()
            try:
                update_interval = int(update_interval)
                if update_interval <= 0:
                    print("Error: Interval must be a positive number")
                    print("Exiting script...")
                    return
            except ValueError:
                print("Error: Please enter a valid number for the interval")
                print("Exiting script...")
                return

            # Start periodic updates - this will run indefinitely until Ctrl+C
            update_random_users_periodically(db, update_interval / 1000)
        else:
            print("Exiting script...")

    except KeyboardInterrupt:
        print("\n✓ Script interrupted by user")
    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        client.close()
        print("✓ Database connection closed")
        print("Script completed successfully!")


if __name__ == "__main__":
    main()
