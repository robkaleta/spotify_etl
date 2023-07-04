from requests import post
import os
import json
from dotenv import load_dotenv
import base64

load_dotenv()

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

print(client_id + client_secret)

def get_token():
    url = 'https://accounts.spotify.com/api/token'
    
    auth_string = client_id +  ':' + client_secret
    auth_bytes = auth_string.encode('utf-8')
    auth64 = str(base64.b64encode(auth_bytes), 'utf-8')

    headers = {
        'Authorization': 'Basic ' + auth64,
        'Content_Type': 'application/x-www-form-urlencoded'
    }

    data = {'grant_type' : 'client_credentials'}
    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result['access_token']
    return(token)

def get_auth_header(token):
    return{'Authorization': 'Bearer ' + token}

token = get_token()

print(get_auth_header(token))