import requests
import json


entities = ["current", "voltage"] 

for entity in entities:
    r = requests.get(f'http://localhost:1026/v2/entities/{entity}')
    print(r.content)
