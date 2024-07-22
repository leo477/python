import requests
import csv

begin_date = input("Input begin date (yyyyMMdd): ")
end_date = input("Input end date (yyyyMMdd): ")
valcode = input("Input currency name: ")

url = f"https://bank.gov.ua/NBU_Exchange/exchange_site?start={begin_date}&end=%20{end_date}&valcode={valcode.lower()}&sort=exchangedate&order=desc&json"

body_items = []
header=["Date", "Currency", "Rate"]
response = requests.get(url)
if response.status_code == 200:
    try:
        data = response.json()
        for items in data:
            body_item = []
            for item in items:
                if item in ("exchangedate", "cc", "rate"):
                    body_item.append(items.get(item))
            body_items.append(body_item)
    except requests.exceptions.JSONDecodeError:
        print("exception")
else:
    print(f"Error: {response.status_code}")

table=[header, *body_items]
with open("example.csv", mode="w") as file:
    writer = csv.writer(file)
    writer.writerows(table)
