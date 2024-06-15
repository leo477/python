import requests
import json
def NBURequest(y):
    print(y)
    response = requests.get(
        "https://bank.gov.ua/NBU_Exchange/exchange_site?start=20240615&end=20240615&valcode=usd&sort"
        "=exchangedate&order=desc&json")
    todos = json.loads(response.text)
    for todo in todos:
        fin = todo["rate"]
    return fin