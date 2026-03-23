import requests

# Remplace par ta vraie clé API Alldebrid
API_KEY = "L5FVDBiJTMKdyFLXttLj"

url = "https://api.alldebrid.com/v4.1/magnet/status"
headers = {
    "Authorization": f"Bearer {API_KEY}"
}

# L'API v4/v4.1 d'Alldebrid utilise principalement des requêtes POST
response = requests.post(url, headers=headers)

# Affichage de la réponse au format JSON
print(response.json())


# {'status': 'success', 'data': {'magnets': []}}, Réponse quand il y a rien dans la librairie
# {'status': 'success', 'data': {'magnets': [{'id': 487603165, 'filename': 'Les Tontons Flingueurs 1963 VFF 2160p UHD BluRay REMUX HEVC DTS-HD MA 2.0.mkv', 'size': 59496074792, 'hash': '61f4d1348b2d21740d966b0894bedabc6c35d339', 'status': 'Ready', 'statusCode': 4, 'uploadDate': 1774283495, 'completionDate': 1774283495, 'type': 'b', 'notified': False, 'version': 2, 'nbLinks': 1}]}} Réponse avec 1 element