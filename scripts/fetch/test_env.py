import os
from dotenv import load_dotenv

#loading the .env file  from the root of the project
load_dotenv()

#getting the api key
api_key = os.getenv("ENTSOE_API_KEY")

print("my API key is:", api_key)
