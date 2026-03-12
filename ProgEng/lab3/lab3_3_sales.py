import os
import random
from datetime import datetime, timedelta

from dotenv import load_dotenv
from faker import Faker
from neo4j import GraphDatabase
from prettytable import PrettyTable


load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DBNAME = os.getenv("NEO4J_DBNAME")


class Neo4jConnection:
    """
    Класс для управления подключением и сессией базы данных Neo4j.
    """
    def __init__(self, uri, username, password):
        self._uri = uri
        self._username = username
        self._password = password
        self._driver = None

    def __enter__(self):
        self._driver = GraphDatabase.driver(
            self._uri, auth=(self._username, self._password)
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        if self._driver:
            self._driver.close()

    def execute_query(self, query, parameters=None):
        with self._driver.session(database=NEO4J_DBNAME) as session:
            result = session.run(query, parameters or {})
            results_list = [record for record in result]
            # print(results_list)
            return results_list
        
class User:
    """
    Класс, представляющий сущность User в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH (n:Users) DETACH DELETE n"
        self.connection.execute_query(query)
    
    def create(self, user_id, user_name, user_surname, birth_date):
        query = """
        CREATE (:Users {
        userId: $userId,
        userName: $userName,
        userSurname: $userSurname,
        birthDate: date($birthDate)
        })
        """
        self.connection.execute_query(
        query,
        {
        "userId": user_id,
        "userName": user_name,
        "userSurname": user_surname,
        "birthDate": birth_date,
        },
        )
    
    def get_all(self):
        query = "MATCH (u:Users) RETURN u.userId AS userId, u.userName AS userName, u.userSurname AS userSurname, u.birthDate AS birthDate"
        return self.connection.execute_query(query)
    
    def get_last_id(self):
        query = "MATCH (u:Users) RETURN COALESCE(MAX(u.userId), 0) as last_id"
        return self.connection.execute_query(query)[0]["last_id"]
    
    def show_all(self):
        users = self.get_all()
        users_table = PrettyTable()
        users_table.field_names = [
        "ID клиента",
        "Имя клиента",
        "Фамилия клиента",
        "Дата рождения",
        ]
        for user in users:
            users_table.add_row(
                [
                    user["userId"],
                    user["userName"],
                    user["userSurname"],
                    user["birthDate"],
                ]
            )
    
        print("\nВсе клиенты:")
        print(users_table)

class Product:
    """
    Класс, представляющий сущность Product в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection

    def clear(self):
        query = "MATCH (n:Products) DETACH DELETE n"
        self.connection.execute_query(query)

    def create(self, pr_id, pr_name, pr_description=""):
        query = """
        CREATE (:Products {
        prId: $prId,
        prName: $prName,
        prDescription: $prDescription
        })
        """
        self.connection.execute_query(
            query, {"prId": pr_id, "prName": pr_name, "prDescription": pr_description}
        )

    def get_all(self):
        query = "MATCH (p:Products) RETURN p.prId AS prId, p.prName AS prName, p.prDescription AS prDescription"
        return self.connection.execute_query(query)
    
    def get_last_id(self):
        query = "MATCH (p:Products) RETURN COALESCE(MAX(p.prId), 0) as last_id"
        result = self.connection.execute_query(query)[0]["last_id"]
        return result
    
    def show_all(self):
        products = self.get_all()
        products_table = PrettyTable()
        products_table.field_names = ["ID товара", "Название товара", "Описание товара"]
        for product in products:
            products_table.add_row(
                [product["prId"], product["prName"], product["prDescription"]]
            )
        print("\nВсе товары:")
        print(products_table)


class Order:
    """
    Класс, представляющий связь Order в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH ()-[o:ORDER]->() DETACH DELETE o"
        self.connection.execute_query(query)

    def create(self, order_id, user_id, pr_id, order_date, price):
        query = """
        MATCH (u:Users {userId: $userId}), (p:Products {prId: $prId})
        MERGE (u)-[:ORDER {
        orderId: $orderId,
        orderDate: date($orderDate),
        price: $price
        }]->(p)
        """
        self.connection.execute_query(
            query,
            {
            "orderId": order_id,
            "userId": user_id,
            "prId": pr_id,
            "orderDate": order_date,
            "price": price,
            },
        )

    def get_all(self):
        query = """
        MATCH (u:Users)-[o:ORDER]->(p:Products)
        RETURN o.orderId AS orderId, o.orderDate AS orderDate, o.price AS price,
        u.userId AS userId, u.userName AS userName, u.userSurname AS
        userSurname,
        p.prId AS prId, p.prName AS prName
        ORDER BY o.orderId
        """
        return self.connection.execute_query(query)
        
    def get_last_id(self):
        query = "MATCH ()-[o:ORDER]->() RETURN COALESCE(MAX(o.orderId), 0) as last_id"
        return self.connection.execute_query(query)[0]["last_id"]
    
    def show_all(self):
        orders = self.get_all()
        orders_table = PrettyTable()
        orders_table.field_names = [
            "ID заказа",
            "Дата заказа",
            "Цена",
            "ID клиента",
            "Имя клиента",
            "Фамилия клиента",
            "ID товара",
            "Название товара",
        ]
        for order in orders:
            orders_table.add_row(
            [
                order["orderId"],
                order["orderDate"],
                order["price"],
                order["userId"],
                order["userName"],
                order["userSurname"],
                order["prId"],
                order["prName"],
            ]
        )
    
        print("\nВсе заказы:")
        print(orders_table)


class DatabaseService:
    def __init__(self, connection):
        self.connection = connection
        self.user_model = User(connection)
        self.product_model = Product(connection)
        self.order_model = Order(connection)

    def clear_database(self):
        self.user_model.clear()
        self.product_model.clear()
        self.order_model.clear()
        print("Database cleared")

    def create_sample_data(self):
        # Создание пользователей
        self.user_model.create(1, "Петр", "Антошин", "1991-07-10")
        self.user_model.create(2, "Сергей", "Пастухов", "2002-03-11")
        self.user_model.create(3, "Анна", "Рокотова", "1999-11-17")
        print("Users created")
        # Создание продуктов
        self.product_model.create(1, "Смартфон", "средство связи")
        self.product_model.create(2, "Ноутбук", "рабочая станция")
        self.product_model.create(3, "Телевизор", "каналы о природе")
        self.product_model.create(4, "Наушники", "слушаем подкасты")
        self.product_model.create(5, "Кондиционер", "")
        self.product_model.create(6, "Кофемашина", "для души")
        print("Products created")
        # Создание заказов
        self.order_model.create(1, 1, 1, "2024-06-03", 100)
        self.order_model.create(2, 2, 2, "2024-06-11", 200)
        self.order_model.create(3, 2, 1, "2024-06-11", 100)
        self.order_model.create(4, 1, 3, "2024-06-18", 300)
        self.order_model.create(5, 1, 4, "2024-06-19", 50)
        self.order_model.create(6, 3, 5, "2024-07-10", 350)
        self.order_model.create(7, 3, 1, "2024-07-10", 1000)
        print("Orders created")

    def show_all_data(self):
        self.user_model.show_all()
        self.product_model.show_all()
        self.order_model.show_all()
        print("All data showed")


def random_date(start_year=None, end_year=None, start_date=None, end_date=None):
    """
    Функция создания случайной даты в формате YYYY-MM-DD из указанного диапазона
    На вход поступают start_year и end_year или start_date и end_date
    Они используются в качестве границ дипазонов случаных дат
    """

    if start_year and end_year:
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
    elif start_date and end_date:
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("start_date and end_date must be in format YYYY-MM-DD")
    else:
        raise ValueError(
        "Either provide start_year and end_year or start_date and end_date"
        )
    
    random_date = start_date + timedelta(
        days=random.randint(0, (end_date - start_date).days)
    )
    return random_date.strftime("%Y-%m-%d")


class UserGenerator:
    """
    Класс для генерации случайных пользователей с ипользованием Faker
    """
    def __init__(self, connection):
        self.connection = connection
        self.user_model = User(connection)
        self.fake = Faker("ru_RU")
    def generate_users(self, user_count: int):
        for i in range(user_count):
            user_name = (
                self.fake.first_name_male()
                if i % 2 == 0
                else self.fake.first_name_female()
            )
        user_surname = (
            self.fake.last_name_male()
            if i % 2 == 0
            else self.fake.last_name_female()
        )
        birth_date = random_date(1900, 2009)
        last_id = self.user_model.get_last_id() + 1
        self.user_model.create(last_id, user_name, user_surname, birth_date)


class OrderGenerator:
    """
    Класс для генерации случайных заказов
    """
    def __init__(self, connection):
        self.connection = connection
        self.order_model = Order(connection)
    def generate_orders(self, order_count: int):
        users_query = "MATCH (u:Users) RETURN u.userId"
        users = [
            user["u.userId"] for user in self.connection.execute_query(users_query)
        ]
        products_query = "MATCH (p:Products) RETURN p.prId"
        products = [
            product["p.prId"]
            for product in self.connection.execute_query(products_query)
        ]
        for _ in range(order_count):
            user_id = random.choice(users)
            pr_id = random.choice(products)
            order_date = random_date(start_date="2024-07-10", end_date="2024-08-10")
            price = random.randint(50, 500)
            last_id = self.order_model.get_last_id() + 1
            self.order_model.create(last_id, user_id, pr_id, order_date, price)


def main():
    try:
        with Neo4jConnection(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD) as connection:
            db_service = DatabaseService(connection)
            user_generator = UserGenerator(connection)
            order_generator = OrderGenerator(connection)
            db_service.clear_database()
            db_service.create_sample_data()

            user_generator.generate_users(10)
            order_generator.generate_orders(20)
            db_service.show_all_data()

    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()