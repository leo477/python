import psycopg2.errors
from psycopg2 import connect
import csv

with connect(
        dbname="test",
        user="postgres",
        password="Pass",
        host="localhost",
        port="5432"
) as connection:
    cursor = connection.cursor()
    try:
        cursor.execute("create table users ("
                       "id serial primary key,"
                       "login varchar(50) NOT NULL,"
                       "first_name varchar(50) not null,"
                       "last_name varchar(100) not null,"
                       "email varchar(100) not null,"
                       "position varchar(50),"
                       "phone varchar(20),"
                       "salary decimal(10,2) default 0)")
        connection.commit()
    except psycopg2.errors.DuplicateTable:
        print("Table didn't create cause it's existed!")
        connection.rollback()
    cursor.execute("delete from users")
    cursor.execute("insert into users (login, first_name, last_name, email, position, phone, salary)  "
                   "values ('LOG1', 'Dima', 'Karambych','sweetpie@gmail.com','Programmer','2626345626',4.23),"
                   "('LOG1234', 'Vanya', 'Fatpig','tastiefood@gmail.com','plumber','262343626',5.23),"
                   "('LOG1235', 'John', 'Week','assassinpiupiu@gmail.com','Killer','26345626',454566.23),"
                   "('LOG1236', 'Mike', 'Ivanov','vanichka@gmail.com','homless','26266656',0.23),"
                   "('LOG1237', 'Vitya', 'Handsomeman','apolon@gmail.com','Sailor','26676',8.23)")
    connection.commit()
    cursor.execute("select * from users")
    rows = cursor.fetchall()

    with open("res.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        writer.writerow([desc[0] for desc in cursor.description])
        writer.writerows(rows)
    cursor.close()
