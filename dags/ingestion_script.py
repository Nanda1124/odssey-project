import requests

response = requests.get('https://transitfeeds.com/p/zet-zagreba-ki-elektri-ni-tramvaj/1007/latest/download')

print(response.status_code)
print(response.content)