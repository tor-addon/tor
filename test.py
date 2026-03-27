import requests



headers = {

    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
}

params = {
    'title': 'Sinners',
}

response = requests.get('https://darkiworld2026.com/api/v1/search/Le%20Sifflet', headers=headers)


print(response.json())