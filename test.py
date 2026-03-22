import requests

cookies = {

    'uid': 'TsrFO2Mfrd5RWyDbLlvqFJXy',


}

headers = {

    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
}

files = [
    ('magnets[]', (None, '920435cbcef2b527c212219a462a3c415d87ac67')),
    ('magnets[]', (None, '920435cbcef2b527c212219a462a3c415d87ac67')),
]

response = requests.post('https://alldebrid.com/internalapi/v4/magnet/upload', cookies=cookies, headers=headers, files=files)

print(response.content)