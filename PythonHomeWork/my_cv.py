from datetime import date

born=date(1993,8,3)
today=date.today()
name="Dimka"

surname="Zakharchenko"
name="Dmytro"
languages=["Ukranian","English","Russian with translator"]
position="BigData Analytic"
food={"tomato","oranges","milk", "fish","fish"}

print("Hi, I'm " + name + " " + surname)
print(f"I'm {(today-born).days//365} years old")
print("I speak:")
for l in languages:
    if l!=languages[2]:
        print(l)
print("What food i like to have for dinner?")
for f in food:
    match f:
        case "oranges" : print(f.upper())
        case _:    print(f)
flag="programmer" in position
if not flag:
    print("I work as " + position)
else:
    print("Yeah! I'm cool cause I work as programmer")