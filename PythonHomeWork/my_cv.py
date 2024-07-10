from datetime import date

born=date(1993,8,3)
today=date.today()
print (f"Hi i was born {(today-born).days/365} years ago")