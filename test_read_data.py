import requests

data = requests.get("https://data-api.max-entropy.com/")
print(data.json())