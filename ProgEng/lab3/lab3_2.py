import os
from neo4j import GraphDatabase
from dotenv import load_dotenv
from prettytable import PrettyTable

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DBNAME = os.getenv("NEO4J_DBNAME")

def clear_db(session):
    session.run("MATCH (n) DETACH DELETE n")

def create_db_by_query(session):
    query = """
 CREATE
 (user1:Users {userId:1, userName: 'Петр', userSurname:'Антошин',
birthDate: date('1991-07-10')}),
 (user2:Users {userId:2, userName: 'Сергей',
userSurname:'Пастухов', birthDate: date('2002-03-11')}),
 (user3:Users {userId:3, userName: 'Анна', userSurname:'Рокотова',
birthDate: date('1999-11-17')})
 CREATE
 (p1:Products {prId:1, prName: 'Смартфон', prDescription:'средство
связи'}),
 (p2:Products {prId:2, prName: 'Ноутбук', prDescription:'рабочая
станция'}),
 (p3:Products {prId:3, prName: 'Телевизор', prDescription:'каналы
о природе'}),
 (p4:Products {prId:4, prName: 'Наушники', prDescription:'слушаем
подкасты'}),
 (p5:Products {prId:5, prName: 'Кондиционер', prDescription:''}),
 (p6:Products {prId:6, prName: 'Кофемашина', prDescription:'для
души'})
 CREATE
 (user1)-[:ORDER {orderId:1, orderDate:date('2024-06-03'),
price:100}]->(p1),
 (user2)-[:ORDER {orderId:2, orderDate:date('2024-06-11'),
price:200}]->(p2),
 (user2)-[:ORDER {orderId:3, orderDate:date('2024-06-11'),
price:100}]->(p1),
 (user1)-[:ORDER {orderId:4, orderDate:date('2024-06-18'),
price:300}]->(p3),
 (user1)-[:ORDER {orderId:5, orderDate:date('2024-06-19'),
price:50}]->(p4),
 (user3)-[:ORDER {orderId:6, orderDate:date('2024-07-10'),
price:350}]->(p5),
 (user3)-[:ORDER {orderId:7, orderDate:date('2024-07-10'),
price:100}]->(p1)
 """
    session.run(query)

def show_users_info(session):
    query = """
    MATCH (u:Users)
    RETURN u.userId, u.userName, u.userSurname, u.birthDate
    """
    users = session.run(query)
    print("\nВсе клиенты:")
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
                user["u.userId"],
                user["u.userName"],
                user["u.userSurname"],
                user["u.birthDate"],
            ]
            )
    print(users_table)

def show_orders_info(session):
    query = """
    MATCH (u:Users)-[o:ORDER]->(p:Products)
    RETURN o.orderId, o.orderDate, o.price, u.userId, u.userName,
    u.userSurname, p.prId, p.prName
    ORDER BY o.orderId
    """
    orders = session.run(query)
    print("\nВсе заказы:")
    orders_table = PrettyTable()
    orders_table.field_names = [
        "ID заказа",
        "Дата заказа",
        "Цена",
        "ID клиента",
        "Имя клиента",
        "Фамилия клиента",
        "ID тоавара",
        "Название товара",
        ]
    for order in orders:
        orders_table.add_row(
            [
                order["o.orderId"],
                order["o.orderDate"],
                order["o.price"],
                order["u.userId"],
                order["u.userName"],
                order["u.userSurname"],
                order["p.prId"],
                order["p.prName"],
            ]
        )
    print(orders_table)

def show_products_info(session):
    query = """
    MATCH (p:Products)
    RETURN p.prId, p.prName, p.prDescription
    """
    products = session.run(query)
    print("\nВсе товары:")
    products_table = PrettyTable()
    products_table.field_names = [
        "ID товара",
        "Название товара",
        "Описание товара",
    ]
    for product in products:
        products_table.add_row(
            [
                product["p.prId"],
                product["p.prName"],
                product["p.prDescription"],
            ]
        )
    print(products_table)

def create_user(session, userName, userSurname, birthDate, userId=None):
    if userId is None:
        last_id = session.run(
            "MATCH (u:Users) RETURN COALESCE(MAX(u.userId), 0) as last_id"
        ).single()["last_id"]
        userId = last_id + 1
    session.run(
        """
        CREATE (:Users {
        userId: $userId,
        userName: $userName,
        userSurname: $userSurname,
        birthDate: date($birthDate)
        })
        """,
        userId=userId,
        userName=userName,
        userSurname=userSurname,
        birthDate=birthDate,
    )

def create_product(session, prName, prDescription="", prId=None):
    if prId is None:
        last_id = session.run(
            "MATCH (p:Products) RETURN COALESCE(MAX(p.prId), 0) as last_id"
        ).single()["last_id"]
        
        prId = last_id + 1
    session.run(
        """
        CREATE (:Products {
        prId: $prId,
        prName: $prName,
        prDescription: $prDescription
        })
        """,
        prId=prId,
        prName=prName,
        prDescription=prDescription,
    )

def create_order(session, userId, prId, orderDate, price, orderId=None):
    if orderId is None:
        last_id = session.run(
            "MATCH ()-[o:ORDER]->() RETURN COALESCE(MAX(o.orderId), 0) as last_id"
        ).single()["last_id"]

        orderId = last_id + 1

    session.run(
        """
        MATCH (u:Users {userId: $userId}), (p:Products {prId: $prId})
        MERGE (u)-[:ORDER {
        orderId: $orderId,
        orderDate: date($orderDate),
        price: $price
        }]->(p)
        """,
        userId=userId,
        prId=prId,
        orderId=orderId,
        orderDate=orderDate,
        price=price,
    )

def create_db_by_def(session):
    # Создание (узлов) пользователей
    create_user(session, "Петр", "Антошин", "1991-07-10")
    create_user(session, "Сергей", "Пастухов", "2002-03-11")
    create_user(session, "Анна", "Рокотова", "1999-11-17")
    # Создание (узлов) товаров
    create_product(session, "Смартфон", "средство связи")
    create_product(session, "Ноутбук", "рабочая станция")
    create_product(session, "Телевизор", "каналы о природе")
    create_product(session, "Наушники", "слушаем подкасты")
    create_product(session, "Кондиционер")
    create_product(session, "Кофемашина", "для души")
    # # Создание [отношений/связей] заказов
    create_order(session, 1, 1, "2024-06-03", 100)
    create_order(session, 2, 2, "2024-06-11", 200)
    create_order(session, 2, 1, "2024-06-11", 100)
    create_order(session, 1, 3, "2024-06-18", 300)
    create_order(session, 1, 4, "2024-06-19", 50)
    create_order(session, 3, 5, "2024-07-10", 350)
    create_order(session, 3, 1, "2024-07-10", 100)

# Основной блок выполнения
def main():
    try:
        # Подключение к базе данных Neo4j
        with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD)) as driver:
            with driver.session(database=NEO4J_DBNAME) as session:
                # Очистка базы данных
                clear_db(session)

                # Заполнение БД
                create_db_by_def(session)

                # create_db_by_query(session)
                # Создание товара
                create_product(session, "Микроволновка", "Для разогрева еды")

                # Просмотр информации о пользователях
                show_users_info(session)

                # Просмотр информации о товарах
                show_products_info(session)

                # Просмотр информации о заказах
                show_orders_info(session)

    except Exception as e:
        print(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    main()