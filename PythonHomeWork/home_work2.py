from datetime import datetime


class People:
    def __init__(self):
        self.people = []

    def add_person(self, name, surname, birthdate, position, salary):
        person = {"name": name, "surname": surname, "birthdate": birthdate, "position": position, "salary": salary}
        self.people.append(person)

    def remove_person(self, name, surname):
        self.people = [person for person in self.people if person["name"] != name or person["surname"] != surname]

    def person_search(self, criteria, value):
        return [person for person in self.people if person[criteria] == value]

    def show_people(self):
        for person in self.people:
            try:
                age = self.person_age(person["birthdate"])
            except ValueError:
                age = 0
            print(
                f"Name: {person["name"]}, surname: {person["surname"]}, birthdate: {person["birthdate"]}, age: {age},position: {person["position"]}, salary: {person["salary"]}")

    def person_age(self, birthdate):
        birthdate = datetime.strptime(birthdate, "%Y-%m-%d")
        today = datetime.today()
        age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
        return age

    def update_info(self, name, surname, newname=None, newsurname=None, newbithdate=None, newposition=None,
                    newsalary=None):
        for person in self.people:
            if person["name"] == name and person["surname"] == surname:
                if newname: person["name"] = newname
                if newsurname: person["surname"] = newsurname
                if newbithdate: person["birthdate"] = newbithdate
                if newposition: person["position"] = newposition
                if newsalary: person["salary"] = newsalary


def menu():
    p = People()
    while True:
        print("\tMain menu")
        print("1. Додати людину")
        print("2. Видалити людину")
        print("3. Оновити дані про людину")
        print("4. Пошук людини за критерієм")
        print("5. Відображення списку всіх людей")
        print("6. Вихід")
        choice = input("Виберіть опцію: ")
        match choice:
            case "1":
                name = input("Input name: ")
                surname = input("Input surname: ")
                birthdate = input("Input birthdate (yyyy-mm-dd): ")
                position = input("Input position: ")
                salary = input("Input salary: ")
                p.add_person(name, surname, birthdate, position, salary)
            case "2":
                name = input("Input name: ")
                surname = input("Input surname: ")
                p.remove_person(name, surname)
            case "3":
                name = input("input name: ")
                surname = input("input surname: ")
                new_name = input("Input new name if you don't want to change it, just press Enter: ")
                new_surname = input("Input new surname if you don't want to change it, just press Enter: ")
                new_birthdate = input(
                    "Input new birthdate (yyyy-mm-dd) if you don't want to change it, just press Enter: ")
                new_position = input("Input new position if you don't want to change it, just press Enter: ")
                new_salary = input("Input new salary if you don't want to change it, just press Enter: ")
                p.update_info(name, surname, new_name, new_surname, new_birthdate, new_position, new_salary)
            case "4":
                criteria = input("Input searching criteria(name,surname,birthdate,position,salary): ")
                value = input(f"Input value for {criteria}: ")
                print(p.person_search(criteria, value))
            case "5":
                p.show_people()
            case "6":
                break
            case _:
                print("Wrong change")


menu()
