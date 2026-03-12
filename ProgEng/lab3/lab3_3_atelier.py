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
            return results_list


class Client:
    """
    Класс, представляющий сущность Client в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH (n:Client) DETACH DELETE n"
        self.connection.execute_query(query)
    
    def create(self, client_id, full_name, address, phone):
        query = """
        CREATE (:Client {
            id: $clientId,
            full_name: $fullName,
            address: $address,
            phone: $phone
        })
        """
        self.connection.execute_query(
            query,
            {
                "clientId": client_id,
                "fullName": full_name,
                "address": address,
                "phone": phone,
            },
        )
    
    def get_all(self):
        query = """
        MATCH (c:Client) 
        RETURN c.id AS id, c.full_name AS full_name, 
               c.address AS address, c.phone AS phone
        ORDER BY c.id
        """
        return self.connection.execute_query(query)
    
    def get_last_id(self):
        query = "MATCH (c:Client) RETURN COALESCE(MAX(c.id), 0) as last_id"
        return self.connection.execute_query(query)[0]["last_id"]
    
    def show_all(self):
        clients = self.get_all()
        clients_table = PrettyTable()
        clients_table.field_names = [
            "ID клиента",
            "ФИО клиента",
            "Адрес",
            "Телефон",
        ]
        for client in clients:
            clients_table.add_row(
                [
                    client["id"],
                    client["full_name"],
                    client["address"],
                    client["phone"],
                ]
            )
    
        print("\n=== КЛИЕНТЫ ===")
        print(clients_table)


class Master:
    """
    Класс, представляющий сущность Master в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH (n:Master) DETACH DELETE n"
        self.connection.execute_query(query)
    
    def create(self, master_id, full_name, address, phone, passport, birthdate, salary, exp):
        query = """
        CREATE (:Master {
            id: $masterId,
            full_name: $fullName,
            address: $address,
            phone: $phone,
            passport: $passport,
            birthdate: date($birthdate),
            salary: $salary,
            exp: $exp
        })
        """
        self.connection.execute_query(
            query,
            {
                "masterId": master_id,
                "fullName": full_name,
                "address": address,
                "phone": phone,
                "passport": passport,
                "birthdate": birthdate,
                "salary": salary,
                "exp": exp,
            },
        )
    
    def get_all(self):
        query = """
        MATCH (m:Master) 
        RETURN m.id AS id, m.full_name AS full_name, m.address AS address, 
               m.phone AS phone, m.passport AS passport, 
               m.birthdate AS birthdate, m.salary AS salary, m.exp AS exp
        ORDER BY m.id
        """
        return self.connection.execute_query(query)
    
    def get_last_id(self):
        query = "MATCH (m:Master) RETURN COALESCE(MAX(m.id), 0) as last_id"
        return self.connection.execute_query(query)[0]["last_id"]
    
    def show_all(self):
        masters = self.get_all()
        masters_table = PrettyTable()
        masters_table.field_names = [
            "ID",
            "ФИО мастера",
            "Телефон",
            "Зарплата",
            "Стаж",
        ]
        for master in masters:
            masters_table.add_row(
                [
                    master["id"],
                    master["full_name"],
                    master["phone"],
                    master["salary"],
                    master["exp"],
                ]
            )
    
        print("\n=== МАСТЕРА ===")
        print(masters_table)


class Model:
    """
    Класс, представляющий сущность Model в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH (n:Model) DETACH DELETE n"
        self.connection.execute_query(query)
    
    def create(self, model_id, article, model_type, season, price, add_price, date):
        query = """
        CREATE (:Model {
            id: $modelId,
            article: $article,
            type: $type,
            season: $season,
            price: $price,
            add_price: $addPrice,
            date: date($date)
        })
        """
        self.connection.execute_query(
            query,
            {
                "modelId": model_id,
                "article": article,
                "type": model_type,
                "season": season,
                "price": price,
                "addPrice": add_price,
                "date": date,
            },
        )
    
    def get_all(self):
        query = """
        MATCH (m:Model) 
        RETURN m.id AS id, m.article AS article, m.type AS type, 
               m.season AS season, m.price AS price, m.add_price AS add_price,
               m.date AS date
        ORDER BY m.id
        """
        return self.connection.execute_query(query)
    
    def get_last_id(self):
        query = "MATCH (m:Model) RETURN COALESCE(MAX(m.id), 0) as last_id"
        return self.connection.execute_query(query)[0]["last_id"]
    
    def show_all(self):
        models = self.get_all()
        models_table = PrettyTable()
        models_table.field_names = [
            "ID",
            "Артикул",
            "Тип",
            "Сезон",
            "Цена",
            "Доп. цена",
        ]
        for model in models:
            models_table.add_row(
                [
                    model["id"],
                    model["article"],
                    model["type"],
                    model["season"],
                    model["price"],
                    model["add_price"],
                ]
            )
    
        print("\n=== МОДЕЛИ ОДЕЖДЫ ===")
        print(models_table)


class Silk:
    """
    Класс, представляющий сущность Silk (ткань) в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH (n:Silk) DETACH DELETE n"
        self.connection.execute_query(query)
    
    def create(self, silk_id, color, width, price_meter, amount, name):
        query = """
        CREATE (:Silk {
            id: $silkId,
            color: $color,
            width: $width,
            price_meter: $priceMeter,
            amount: $amount,
            name: $name
        })
        """
        self.connection.execute_query(
            query,
            {
                "silkId": silk_id,
                "color": color,
                "width": width,
                "priceMeter": price_meter,
                "amount": amount,
                "name": name,
            },
        )
    
    def get_all(self):
        query = """
        MATCH (s:Silk) 
        RETURN s.id AS id, s.color AS color, s.width AS width, 
               s.price_meter AS price_meter, s.amount AS amount, s.name AS name
        ORDER BY s.id
        """
        return self.connection.execute_query(query)
    
    def get_last_id(self):
        query = "MATCH (s:Silk) RETURN COALESCE(MAX(s.id), 0) as last_id"
        return self.connection.execute_query(query)[0]["last_id"]
    
    def show_all(self):
        silks = self.get_all()
        silks_table = PrettyTable()
        silks_table.field_names = [
            "ID",
            "Название",
            "Цвет",
            "Ширина",
            "Цена/м",
            "Кол-во",
        ]
        for silk in silks:
            silks_table.add_row(
                [
                    silk["id"],
                    silk["name"],
                    silk["color"],
                    silk["width"],
                    silk["price_meter"],
                    silk["amount"],
                ]
            )
    
        print("\n=== ТКАНИ ===")
        print(silks_table)


class Measure:
    """
    Класс, представляющий сущность Measure (мерки) в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH (n:Measure) DETACH DELETE n"
        self.connection.execute_query(query)
    
    def create(self, measure_id, client_id, measure_date, data):
        query = """
        MATCH (c:Client {id: $clientId})
        CREATE (m:Measure {
            id: $measureId,
            measure_date: date($measureDate),
            data: $data
        })
        CREATE (c)-[:HAS_MEASURE]->(m)
        """
        self.connection.execute_query(
            query,
            {
                "measureId": measure_id,
                "clientId": client_id,
                "measureDate": measure_date,
                "data": data,
            },
        )
    
    def get_all(self):
        query = """
        MATCH (c:Client)-[:HAS_MEASURE]->(m:Measure)
        RETURN m.id AS id, m.measure_date AS measure_date, m.data AS data,
               c.id AS client_id, c.full_name AS client_name
        ORDER BY m.id
        """
        return self.connection.execute_query(query)
    
    def get_last_id(self):
        query = "MATCH (m:Measure) RETURN COALESCE(MAX(m.id), 0) as last_id"
        return self.connection.execute_query(query)[0]["last_id"]


class Order:
    """
    Класс, представляющий сущность Order в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH (n:Order) DETACH DELETE n"
        self.connection.execute_query(query)
    
    def create(self, order_id, client_id, master_id, model_id, status, start_date, end_date=None):
        query = """
        MATCH (c:Client {id: $clientId})
        MATCH (m:Master {id: $masterId})
        MATCH (mod:Model {id: $modelId})
        CREATE (o:Order {
            id: $orderId,
            status: $status,
            start_date: date($startDate),
            end_date: 
                CASE WHEN $endDate IS NOT NULL 
                THEN date($endDate) 
                ELSE NULL END
        })
        CREATE (c)-[:PLACED]->(o)
        CREATE (o)-[:ASSIGNED_TO]->(m)
        CREATE (o)-[:FOR_MODEL]->(mod)
        """
        self.connection.execute_query(
            query,
            {
                "orderId": order_id,
                "clientId": client_id,
                "masterId": master_id,
                "modelId": model_id,
                "status": status,
                "startDate": start_date,
                "endDate": end_date,
            },
        )
    
    def get_all(self):
        query = """
        MATCH (c:Client)-[:PLACED]->(o:Order)
        MATCH (o)-[:ASSIGNED_TO]->(m:Master)
        MATCH (o)-[:FOR_MODEL]->(mod:Model)
        OPTIONAL MATCH (o)-[:HAS_COST]->(cost:Cost)
        RETURN o.id AS id, o.status AS status, 
               o.start_date AS start_date, o.end_date AS end_date,
               c.id AS client_id, c.full_name AS client_name,
               m.id AS master_id, m.full_name AS master_name,
               mod.id AS model_id, mod.type AS model_type,
               cost.total_cost AS total_cost, cost.paid_amount AS paid_amount
        ORDER BY o.id
        """
        return self.connection.execute_query(query)
    
    def get_last_id(self):
        query = "MATCH (o:Order) RETURN COALESCE(MAX(o.id), 0) as last_id"
        return self.connection.execute_query(query)[0]["last_id"]
    
    def show_all(self):
        orders = self.get_all()
        orders_table = PrettyTable()
        orders_table.field_names = [
            "ID заказа",
            "Статус",
            "Дата начала",
            "Дата окончания",
            "Клиент",
            "Мастер",
            "Модель",
            "Сумма",
            "Оплачено",
        ]
        for order in orders:
            orders_table.add_row(
                [
                    order["id"],
                    order["status"],
                    order["start_date"],
                    order["end_date"] or "в работе",
                    order["client_name"],
                    order["master_name"],
                    order["model_type"],
                    order["total_cost"] or 0,
                    order["paid_amount"] or 0,
                ]
            )
    
        print("\n=== ЗАКАЗЫ ===")
        print(orders_table)


class Cost:
    """
    Класс, представляющий сущность Cost (стоимость заказа) в базе данных.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH (n:Cost) DETACH DELETE n"
        self.connection.execute_query(query)
    
    def create(self, cost_id, order_id, tailor_cost, material_cost, total_cost, paid_amount, status):
        query = """
        MATCH (o:Order {id: $orderId})
        CREATE (c:Cost {
            id: $costId,
            tailor_cost: $tailorCost,
            material_cost: $materialCost,
            total_cost: $totalCost,
            paid_amount: $paidAmount,
            status: $status
        })
        CREATE (o)-[:HAS_COST]->(c)
        """
        self.connection.execute_query(
            query,
            {
                "costId": cost_id,
                "orderId": order_id,
                "tailorCost": tailor_cost,
                "materialCost": material_cost,
                "totalCost": total_cost,
                "paidAmount": paid_amount,
                "status": status,
            },
        )
    
    def get_last_id(self):
        query = "MATCH (c:Cost) RETURN COALESCE(MAX(c.id), 0) as last_id"
        return self.connection.execute_query(query)[0]["last_id"]


class OrderMaterial:
    """
    Класс, представляющий связь USES_MATERIAL между заказом и тканью.
    """
    def __init__(self, connection):
        self.connection = connection
    
    def clear(self):
        query = "MATCH ()-[r:USES_MATERIAL]->() DELETE r"
        self.connection.execute_query(query)
    
    def create(self, order_id, silk_id, silk_cost, silk_consume, cliend_silk_data=None):
        query = """
        MATCH (o:Order {id: $orderId})
        MATCH (s:Silk {id: $silkId})
        CREATE (o)-[:USES_MATERIAL {
            cliend_silk_data: $cliendSilkData,
            silk_cost: $silkCost,
            silk_consume: $silkConsume
        }]->(s)
        """
        self.connection.execute_query(
            query,
            {
                "orderId": order_id,
                "silkId": silk_id,
                "cliendSilkData": cliend_silk_data,
                "silkCost": silk_cost,
                "silkConsume": silk_consume,
            },
        )


class DatabaseService:
    def __init__(self, connection):
        self.connection = connection
        self.client_model = Client(connection)
        self.master_model = Master(connection)
        self.model_model = Model(connection)
        self.silk_model = Silk(connection)
        self.measure_model = Measure(connection)
        self.order_model = Order(connection)
        self.cost_model = Cost(connection)
        self.order_material_model = OrderMaterial(connection)

    def clear_database(self):
        self.order_material_model.clear()
        self.cost_model.clear()
        self.order_model.clear()
        self.measure_model.clear()
        self.silk_model.clear()
        self.model_model.clear()
        self.master_model.clear()
        self.client_model.clear()
        print("База данных очищена")

    def create_sample_data(self):
        # Создание клиентов
        self.client_model.create(1, "Иванов Петр Сергеевич", "ул. Ленина, 10-5", "+7(495)123-45-67")
        self.client_model.create(2, "Петрова Анна Ивановна", "ул. Гагарина, 25-12", "+7(495)234-56-78")
        self.client_model.create(3, "Сидоров Алексей Владимирович", "пр. Мира, 5-34", "+7(495)345-67-89")
        self.client_model.create(4, "Козлова Елена Дмитриевна", "ул. Пушкина, 15-7", "+7(495)456-78-90")
        print("Клиенты созданы")

        # Создание мастеров
        self.master_model.create(1, "Оруэлл Джордж", "ул. Центральная, 3-8", "+7(495)111-22-33", "4501 123456", "1985-03-15", 62000, 8)
        self.master_model.create(2, "Кейдж Николас", "ул. Садовая, 17-4", "+7(495)222-33-44", "4502 234567", "1990-07-22", 55000, 7)
        self.master_model.create(3, "Цепеш Алукард Интегров", "ул. Новая, 6-15", "+7(495)333-44-55", "4503 345678", "1982-11-05", 65000, 10)
        self.master_model.create(4, "Николсон Джек", "ул. Зеленая, 9-23", "+7(495)444-55-66", "4504 456789", "1988-09-18", 60000, 5)
        self.master_model.create(5, "Замятин Евгений Иванович", "ул. Парковая, 4-11", "+7(495)555-66-77", "4505 567890", "1979-12-30", 53000, 12)
        self.master_model.create(6, "Куприянов Михаил", "ул. Речная, 22-6", "+7(495)666-77-88", "4506 678901", "1992-05-07", 58000, 4)
        self.master_model.create(7, "Крылов Порфирий", "ул. Школьная, 11-31", "+7(495)777-88-99", "4507 789012", "1983-08-25", 59000, 9)
        print("Мастера созданы")

        # Создание моделей одежды
        self.model_model.create(1, "DR-001", "Платье вечернее", "wn", 8500, 1200, "2023-01-15")
        self.model_model.create(2, "ST-002", "Костюм мужской", "sp", 12000, 2000, "2023-02-10")
        self.model_model.create(3, "SK-003", "Юбка карандаш", "sm", 3500, 500, "2023-03-05")
        self.model_model.create(4, "SH-004", "Рубашка классическая", "au", 2800, 400, "2023-04-20")
        self.model_model.create(5, "TR-005", "Брюки женские", "wn", 4200, 600, "2023-05-12")
        self.model_model.create(6, "CT-006", "Пальто зимнее", "wn", 18500, 3500, "2023-06-08")
        self.model_model.create(7, "BL-007", "Блуза", "sm", 2900, 450, "2023-07-25")
        print("Модели созданы")

        # Создание тканей
        self.silk_model.create(1, "Красный", 1.5, 850, 50, "Шелк натуральный")
        self.silk_model.create(2, "Синий", 1.4, 720, 35, "Шелк армани")
        self.silk_model.create(3, "Черный", 1.5, 950, 42, "Шелк крепдешин")
        self.silk_model.create(4, "Белый", 1.4, 680, 28, "Шелк шифон")
        self.silk_model.create(5, "Зеленый", 1.5, 790, 33, "Шелк атлас")
        self.silk_model.create(6, "Фиолетовый", 1.4, 880, 22, "Шелк бархат")
        self.silk_model.create(7, "Бежевый", 1.5, 750, 45, "Шелк жаккард")
        print("Ткани созданы")

        # Создание заказов
        self.order_model.create(1001, 1, 1, 1, "completed", "2023-09-15", "2023-09-30")
        self.order_model.create(1002, 2, 2, 2, "in_progress", "2023-12-05", None)
        self.order_model.create(1003, 3, 3, 3, "completed", "2023-10-10", "2023-10-25")
        self.order_model.create(1004, 4, 4, 5, "pending", "2023-12-10", None)
        self.order_model.create(1005, 5, 5, 6, "completed", "2023-11-01", "2023-11-28")
        self.order_model.create(1006, 6, 6, 4, "in_progress", "2023-12-01", None)
        self.order_model.create(1007, 7, 7, 7, "completed", "2023-09-05", "2023-09-20")
        print("Заказы созданы")

        # Создание стоимостей заказов
        self.cost_model.create(201, 1001, 2500, 4250, 6750, 6750, "paid")
        self.cost_model.create(202, 1002, 3200, 5700, 8900, 5000, "partial")
        self.cost_model.create(203, 1003, 1800, 2520, 4320, 4320, "paid")
        self.cost_model.create(204, 1004, 2200, 0, 2200, 1000, "partial")
        self.cost_model.create(205, 1005, 4500, 6160, 10660, 10660, "paid")
        self.cost_model.create(206, 1006, 1500, 2040, 3540, 0, "unpaid")
        self.cost_model.create(207, 1007, 2100, 3750, 5850, 5850, "paid")
        print("Стоимости заказов созданы")

        # Создание связей с тканями
        self.order_material_model.create(1001, 1, 4250, 2.5)
        self.order_material_model.create(1002, 3, 5700, 3.0)
        self.order_material_model.create(1003, 2, 2520, 1.8)
        self.order_material_model.create(1004, 5, 0, 2.2, "Ткань клиента")
        self.order_material_model.create(1005, 6, 6160, 3.5)
        self.order_material_model.create(1006, 4, 2040, 1.5)
        self.order_material_model.create(1007, 7, 3750, 2.0)
        print("Материалы заказов созданы")

        # Создание мерок клиентов
        self.measure_model.create(101, 1, "2023-09-10", "ОГ=92, ОТ=70, ОБ=96")
        self.measure_model.create(102, 1, "2023-12-15", "ОГ=93, ОТ=71, ОБ=97")
        self.measure_model.create(103, 2, "2023-08-05", "ОГ=88, ОТ=65, ОБ=92")
        self.measure_model.create(104, 2, "2023-11-20", "ОГ=88, ОТ=66, ОБ=93")
        self.measure_model.create(105, 3, "2023-10-12", "ОГ=98, ОТ=82, ОБ=102")
        self.measure_model.create(106, 4, "2023-09-28", "ОГ=90, ОТ=68, ОБ=94")
        self.measure_model.create(107, 5, "2023-11-05", "ОГ=95, ОТ=78, ОБ=100")
        print("Мерки созданы")

    def show_all_data(self):
        self.client_model.show_all()
        self.master_model.show_all()
        self.model_model.show_all()
        self.silk_model.show_all()
        self.order_model.show_all()
        print("\nВсе данные отображены")

    def run_queries(self):
        """Выполнение аналитических запросов"""
        print("\n" + "="*80)
        print("АНАЛИТИЧЕСКИЕ ЗАПРОСЫ")
        print("="*80)
        
        # Запрос 1: Эффективность мастеров
        query1 = """
        MATCH (m:Master)<-[:ASSIGNED_TO]-(o:Order)-[:HAS_COST]->(cost:Cost)
        WITH m, SUM(cost.paid_amount) AS TotalOrdersRevenue
        RETURN m.full_name AS Master, 
               m.salary AS MonthlySalary,
               TotalOrdersRevenue AS RevenueFromOrders,
               ROUND(100.0 * TotalOrdersRevenue / m.salary, 2) AS EfficiencyPercent
        ORDER BY EfficiencyPercent DESC
        """
        result1 = self.connection.execute_query(query1)
        table1 = PrettyTable()
        table1.field_names = ["Мастер", "Зарплата", "Выручка", "Эффективность %"]
        for record in result1:
            table1.add_row([record["Master"], record["MonthlySalary"], 
                           record["RevenueFromOrders"], record["EfficiencyPercent"]])
        print("\n1. Эффективность мастеров:")
        print(table1)

        # Запрос 2: Популярность моделей по сезонам
        query2 = """
        MATCH (mod:Model)<-[:FOR_MODEL]-(o:Order)
        RETURN mod.season AS Season,
               mod.type AS ModelType,
               COUNT(o) AS OrderCount,
               AVG(mod.price) AS AvgBasePrice
        ORDER BY Season, OrderCount DESC
        """
        result2 = self.connection.execute_query(query2)
        table2 = PrettyTable()
        table2.field_names = ["Сезон", "Тип модели", "Кол-во заказов", "Ср. цена"]
        for record in result2:
            table2.add_row([record["Season"], record["ModelType"], 
                           record["OrderCount"], round(record["AvgBasePrice"], 2)])
        print("\n2. Популярность моделей по сезонам:")
        print(table2)

        # Запрос 3: Клиенты с наибольшими суммами заказов
        query3 = """
        MATCH (c:Client)-[:PLACED]->(o:Order)-[:HAS_COST]->(cost:Cost)
        RETURN c.full_name AS Client,
               COUNT(o) AS OrdersCount,
               SUM(cost.total_cost) AS TotalSpent,
               AVG(cost.total_cost) AS AvgOrderCost
        ORDER BY TotalSpent DESC
        LIMIT 5
        """
        result3 = self.connection.execute_query(query3)
        table3 = PrettyTable()
        table3.field_names = ["Клиент", "Кол-во заказов", "Всего потрачено", "Ср. чек"]
        for record in result3:
            table3.add_row([record["Client"], record["OrdersCount"], 
                           record["TotalSpent"], round(record["AvgOrderCost"], 2)])
        print("\n3. Топ-5 клиентов по сумме заказов:")
        print(table3)


def random_date(start_year=None, end_year=None, start_date=None, end_date=None):
    """
    Функция создания случайной даты в формате YYYY-MM-DD из указанного диапазона
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


class ClientGenerator:
    """
    Класс для генерации случайных клиентов с использованием Faker
    """
    def __init__(self, connection):
        self.connection = connection
        self.client_model = Client(connection)
        self.fake = Faker("ru_RU")
    
    def generate_clients(self, client_count: int):
        for i in range(client_count):
            full_name = self.fake.name()
            address = f"ул. {self.fake.street_name()}, {random.randint(1,50)}-{random.randint(1,50)}"
            phone = f"+7(9{random.randint(10,99)}){random.randint(100,999)}-{random.randint(10,99)}-{random.randint(10,99)}"
            last_id = self.client_model.get_last_id() + 1
            self.client_model.create(last_id, full_name, address, phone)
        print(f"Сгенерировано {client_count} клиентов")


class OrderGenerator:
    """
    Класс для генерации случайных заказов
    """
    def __init__(self, connection):
        self.connection = connection
        self.order_model = Order(connection)
        self.cost_model = Cost(connection)
        self.order_material_model = OrderMaterial(connection)
    
    def generate_orders(self, order_count: int):
        clients_query = "MATCH (c:Client) RETURN c.id AS id"
        clients = [record["id"] for record in self.connection.execute_query(clients_query)]
        
        masters_query = "MATCH (m:Master) RETURN m.id AS id"
        masters = [record["id"] for record in self.connection.execute_query(masters_query)]
        
        models_query = "MATCH (mod:Model) RETURN mod.id AS id, mod.price AS price"
        models = self.connection.execute_query(models_query)
        
        silks_query = "MATCH (s:Silk) RETURN s.id AS id, s.price_meter AS price_meter"
        silks = self.connection.execute_query(silks_query)
        
        statuses = ["completed", "in_progress", "pending"]
        
        for _ in range(order_count):
            client_id = random.choice(clients)
            master_id = random.choice(masters)
            model = random.choice(models)
            model_id = model["id"]
            silk = random.choice(silks)
            silk_id = silk["id"]
            
            status = random.choice(statuses)
            start_date = random_date(start_date="2024-01-01", end_date="2024-12-31")
            end_date = random_date(start_date=start_date, end_date="2024-12-31") if status == "completed" else None
            
            last_order_id = self.order_model.get_last_id() + 1
            self.order_model.create(last_order_id, client_id, master_id, model_id, 
                                   status, start_date, end_date)
            
            silk_consume = round(random.uniform(1.0, 4.0), 1)
            silk_cost = float(silk_consume * silk["price_meter"]) if random.choice([True, False]) else 0.0
            tailor_cost = float(random.randint(1500, 5000))
            total_cost = silk_cost + tailor_cost
            
            total_cost_int = int(total_cost)
            paid_amount = total_cost_int if status == "completed" else float(random.randint(0, total_cost_int))
            
            payment_status = "paid" if paid_amount == total_cost else "partial" if paid_amount > 0 else "unpaid"
            
            last_cost_id = self.cost_model.get_last_id() + 1
            self.cost_model.create(
                int(last_cost_id), 
                int(last_order_id), 
                float(tailor_cost), 
                float(silk_cost), 
                float(total_cost), 
                float(paid_amount), 
                payment_status
            )
            
            cliend_data = "Ткань клиента" if silk_cost == 0 else None
            self.order_material_model.create(
                int(last_order_id), 
                int(silk_id), 
                float(silk_cost), 
                float(silk_consume), 
                cliend_data
            )
        
        print(f"Сгенерировано {order_count} заказов")


def main():
    try:
        with Neo4jConnection(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD) as connection:
            db_service = DatabaseService(connection)
            client_generator = ClientGenerator(connection)
            order_generator = OrderGenerator(connection)
            
            db_service.clear_database()
            db_service.create_sample_data()
            
            client_generator.generate_clients(10)
            order_generator.generate_orders(20)
            
            db_service.show_all_data()
            db_service.run_queries()

    except Exception as e:
        print(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    main()