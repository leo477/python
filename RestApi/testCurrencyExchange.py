from Functions.Function import NBURequest
from datetime import date
print(NBURequest(32))


today = date.today()
print("Today's date:", today)


formatted_date = today.strftime("%Y%m%d")
print(formatted_date+"sdfsdf")
