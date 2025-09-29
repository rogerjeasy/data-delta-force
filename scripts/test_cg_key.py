import os
from dotenv import load_dotenv
import requests

load_dotenv()

api_key = os.getenv('COINGECKO_API_KEY')
print(f"API Key found: {bool(api_key)}")

print("\n1. Testing CoinGecko Demo Key (Header Method)...")
cg_key = os.getenv('COINGECKO_API_KEY')
if cg_key:
    url = "https://api.coingecko.com/api/v3/ping"
    headers = {'x-cg-demo-api-key': cg_key}
    response = requests.get(url, headers=headers)
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        print(f"   ✓ CoinGecko Demo key works!")
        print(f"   Response: {response.json()}")
    else:
        print(f"   ✗ Error: {response.text}")
else:
    print("   ✗ No CoinGecko key found")