import requests
import os

# Updated credentials
CLIENT_ID = "ee2cba79d332182f"
CLIENT_SECRET = "3f15bd8f2e1dc3b2a1475bb458ef9961"
URI = "http://192.168.1.47:8181/api/catalog/v1/oauth/tokens"

print(f"Testing Auth to: {URI}")
print(f"Client ID: {CLIENT_ID}")

payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "PRINCIPAL_ROLE:ALL"
}

try:
    resp = requests.post(URI, data=payload, timeout=5)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text}")
except Exception as e:
    print(f"Failed: {e}")