import redis
from redis.exceptions import AuthenticationError, ConnectionError

# Add the password parameter to connect successfully
r = redis.Redis(
      host='localhost', 
      port=6379, 
      db=0, 
      password='tahir@2024',  # Your Redis password
      decode_responses=True
)

# Test the connection
try:
      if r.ping():
            print("Successfully connected to Redis!")
except AuthenticationError:
      print("Authentication failed. Please check your password.")
except ConnectionError:
      print("Could not connect to Redis. Ensure the Docker container is running.")

# 1. Simple Strings
r.set('user:100', 'Alice')
print(r.get('user:100'))  # Output: Alice

# 2. Working with Hashes (Ideal for objects/dictionaries)
r.hset('session:456', mapping={'username': 'bob', 'role': 'admin'})
print(r.hgetall('session:456'))  # Output: {'username': 'bob', 'role': 'admin'}

# 3. Lists (Ordered collections)
r.rpush('queue:tasks', 'task1', 'task2')
print(r.lpop('queue:tasks'))  # Output: task1
