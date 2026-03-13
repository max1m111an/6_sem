import os
import pandas as pd
from dotenv import load_dotenv
import clickhouse_connect

load_dotenv()

class DatabaseConnection:
    def __init__(self, host, port, username, password, database='atelier'):
        self.client = clickhouse_connect.get_client(
           host=host,
           port=port,
           username=username,
           password=password
        )
        self.database = database
        # Создаем базу данных, если не существует
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
        self.client.command(f"USE {database}")

    def close(self):
        self.client.close()

    def clear_database(self):
        try:
            tables = self.client.query(f"SHOW TABLES FROM {self.database}").result_rows
            for table in tables:
                table_name = table[0]
                self.client.command(f"DROP TABLE IF EXISTS {self.database}.{table_name}")
            print("База данных успешно очищена")
            return True
        except Exception as e:
            print(f"Ошибка при очистке базы данных: {e}")
            return False


class Client:
    def __init__(self, connection):
        self.connection = connection
        self.table = f"{connection.database}.clients"
        self._create_table()

    def _create_table(self):
        self.connection.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.table}
        (
            id UInt32,
            full_name String,
            address String,
            phone String
        )
        ENGINE = MergeTree()
        ORDER BY id
        """)

    def create(self, client_id, full_name, address, phone):
        query = f"""
        INSERT INTO {self.table} (id, full_name, address, phone)
        VALUES ({client_id}, '{full_name}', '{address}', '{phone}')
        """
        self.connection.client.command(query)
        return client_id

    def get(self, client_id):
        query = f"SELECT * FROM {self.table} WHERE id = {client_id}"
        result = self.connection.client.query(query).result_rows
        return result[0] if result else None

    def update(self, client_id, update_fields):
        set_clause = ", ".join([f"{k} = '{v}'" for k, v in update_fields.items()])
        query = f"ALTER TABLE {self.table} UPDATE {set_clause} WHERE id = {client_id}"
        self.connection.client.command(query)
        return 1

    def delete(self, client_id):
        query = f"ALTER TABLE {self.table} DELETE WHERE id = {client_id}"
        self.connection.client.command(query)
        return 1

    def get_all(self):
        query = f"SELECT * FROM {self.table} ORDER BY id"
        return self.connection.client.query(query).result_rows

    def get_last_id(self):
        query = f"SELECT max(id) FROM {self.table}"
        result = self.connection.client.query(query).result_rows
        return result[0][0] if result[0][0] else 0


class Master:
    def __init__(self, connection):
        self.connection = connection
        self.table = f"{connection.database}.masters"
        self._create_table()

    def _create_table(self):
        self.connection.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.table}
        (
            id UInt32,
            full_name String,
            address String,
            phone String,
            passport String,
            birthdate Date,
            salary Float32,
            exp UInt8
        )
        ENGINE = MergeTree()
        ORDER BY id
        """)

    def create(self, master_id, full_name, address, phone, passport, birthdate, salary, exp):
        query = f"""
        INSERT INTO {self.table} 
        (id, full_name, address, phone, passport, birthdate, salary, exp)
        VALUES ({master_id}, '{full_name}', '{address}', '{phone}', 
                '{passport}', '{birthdate}', {salary}, {exp})
        """
        self.connection.client.command(query)
        return master_id

    def get_all(self):
        query = f"SELECT * FROM {self.table} ORDER BY id"
        return self.connection.client.query(query).result_rows

    def get_last_id(self):
        query = f"SELECT max(id) FROM {self.table}"
        result = self.connection.client.query(query).result_rows
        return result[0][0] if result[0][0] else 0


class Model:
    def __init__(self, connection):
        self.connection = connection
        self.table = f"{connection.database}.models"
        self._create_table()

    def _create_table(self):
        self.connection.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.table}
        (
            id UInt32,
            article String,
            type String,
            season Enum8('wn' = 1, 'sp' = 2, 'sm' = 3, 'au' = 4),
            price Float32,
            add_price Float32,
            date Date
        )
        ENGINE = MergeTree()
        ORDER BY id
        """)

    def create(self, model_id, article, model_type, season, price, add_price, date):
        query = f"""
        INSERT INTO {self.table} 
        (id, article, type, season, price, add_price, date)
        VALUES ({model_id}, '{article}', '{model_type}', '{season}', 
                {price}, {add_price}, '{date}')
        """
        self.connection.client.command(query)
        return model_id

    def get_all(self):
        query = f"SELECT * FROM {self.table} ORDER BY id"
        return self.connection.client.query(query).result_rows

    def get_last_id(self):
        query = f"SELECT max(id) FROM {self.table}"
        result = self.connection.client.query(query).result_rows
        return result[0][0] if result[0][0] else 0


class Silk:
    def __init__(self, connection):
        self.connection = connection
        self.table = f"{connection.database}.silk"
        self._create_table()

    def _create_table(self):
        self.connection.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.table}
        (
            id UInt32,
            color String,
            width Float32,
            price_meter Float32,
            amount UInt32,
            name String
        )
        ENGINE = MergeTree()
        ORDER BY id
        """)

    def create(self, silk_id, color, width, price_meter, amount, name):
        query = f"""
        INSERT INTO {self.table} 
        (id, color, width, price_meter, amount, name)
        VALUES ({silk_id}, '{color}', {width}, {price_meter}, {amount}, '{name}')
        """
        self.connection.client.command(query)
        return silk_id

    def get_all(self):
        query = f"SELECT * FROM {self.table} ORDER BY id"
        return self.connection.client.query(query).result_rows

    def get_last_id(self):
        query = f"SELECT max(id) FROM {self.table}"
        result = self.connection.client.query(query).result_rows
        return result[0][0] if result[0][0] else 0


class Order:
    def __init__(self, connection):
        self.connection = connection
        self.table = f"{connection.database}.orders"
        self._create_table()

    def _create_table(self):
        self.connection.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.table}
        (
            id UInt32,
            status String,
            start_date Date,
            end_date Nullable(Date),
            client_id UInt32,
            master_id UInt32,
            model_id UInt32
        )
        ENGINE = MergeTree()
        ORDER BY id
        """)

    def create(self, order_id, status, start_date, end_date, client_id, master_id, model_id):
        end_date_str = f"'{end_date}'" if end_date else "NULL"
        query = f"""
        INSERT INTO {self.table} 
        (id, status, start_date, end_date, client_id, master_id, model_id)
        VALUES ({order_id}, '{status}', '{start_date}', {end_date_str}, 
                {client_id}, {master_id}, {model_id})
        """
        self.connection.client.command(query)
        return order_id

    def get_all(self):
        query = f"SELECT * FROM {self.table} ORDER BY id"
        return self.connection.client.query(query).result_rows

    def get_last_id(self):
        query = f"SELECT max(id) FROM {self.table}"
        result = self.connection.client.query(query).result_rows
        return result[0][0] if result[0][0] else 0


class OrderCost:
    def __init__(self, connection):
        self.connection = connection
        self.table = f"{connection.database}.order_costs"
        self._create_table()

    def _create_table(self):
        self.connection.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.table}
        (
            id UInt32,
            tailor_cost Float32,
            material_cost Float32,
            total_cost Float32,
            paid_amount Float32,
            status String,
            order_id UInt32
        )
        ENGINE = MergeTree()
        ORDER BY id
        """)

    def create(self, cost_id, tailor_cost, material_cost, total_cost, paid_amount, status, order_id):
        query = f"""
        INSERT INTO {self.table} 
        (id, tailor_cost, material_cost, total_cost, paid_amount, status, order_id)
        VALUES ({cost_id}, {tailor_cost}, {material_cost}, {total_cost}, 
                {paid_amount}, '{status}', {order_id})
        """
        self.connection.client.command(query)
        return cost_id

    def get_all(self):
        query = f"SELECT * FROM {self.table} ORDER BY id"
        return self.connection.client.query(query).result_rows

    def get_last_id(self):
        query = f"SELECT max(id) FROM {self.table}"
        result = self.connection.client.query(query).result_rows
        return result[0][0] if result[0][0] else 0


class ClientMeasure:
    def __init__(self, connection):
        self.connection = connection
        self.table = f"{connection.database}.client_measures"
        self._create_table()

    def _create_table(self):
        self.connection.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.table}
        (
            id UInt32,
            measure_date Date,
            data String,
            client_id UInt32
        )
        ENGINE = MergeTree()
        ORDER BY id
        """)

    def create(self, measure_id, measure_date, data, client_id):
        query = f"""
        INSERT INTO {self.table} 
        (id, measure_date, data, client_id)
        VALUES ({measure_id}, '{measure_date}', '{data}', {client_id})
        """
        self.connection.client.command(query)
        return measure_id

    def get_all(self):
        query = f"SELECT * FROM {self.table} ORDER BY id"
        return self.connection.client.query(query).result_rows

    def get_last_id(self):
        query = f"SELECT max(id) FROM {self.table}"
        result = self.connection.client.query(query).result_rows
        return result[0][0] if result[0][0] else 0


class OrderMaterial:
    def __init__(self, connection):
        self.connection = connection
        self.table = f"{connection.database}.order_materials"
        self._create_table()

    def _create_table(self):
        self.connection.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.table}
        (
            id UInt32,
            cliend_silk_data Nullable(String),
            silk_cost Nullable(Float32),
            silk_consume Nullable(Float32),
            order_id UInt32,
            silk_id Nullable(UInt32)
        )
        ENGINE = MergeTree()
        ORDER BY id
        """)

    def create(self, material_id, cliend_silk_data, silk_cost, silk_consume, order_id, silk_id):
        cliend_data_str = f"'{cliend_silk_data}'" if cliend_silk_data else "NULL"
        silk_cost_str = str(silk_cost) if silk_cost is not None else "NULL"
        silk_consume_str = str(silk_consume) if silk_consume is not None else "NULL"
        silk_id_str = str(silk_id) if silk_id is not None else "NULL"
        
        query = f"""
        INSERT INTO {self.table} 
        (id, cliend_silk_data, silk_cost, silk_consume, order_id, silk_id)
        VALUES ({material_id}, {cliend_data_str}, {silk_cost_str}, 
                {silk_consume_str}, {order_id}, {silk_id_str})
        """
        self.connection.client.command(query)
        return material_id

    def get_all(self):
        query = f"SELECT * FROM {self.table} ORDER BY id"
        return self.connection.client.query(query).result_rows

    def get_last_id(self):
        query = f"SELECT max(id) FROM {self.table}"
        result = self.connection.client.query(query).result_rows
        return result[0][0] if result[0][0] else 0


class DatabaseService:
    def __init__(self, connection, database='atelier'):
        self.connection = connection
        self.client_model = Client(connection)
        self.master_model = Master(connection)
        self.model_model = Model(connection)
        self.silk_model = Silk(connection)
        self.order_model = Order(connection)
        self.order_cost_model = OrderCost(connection)
        self.client_measure_model = ClientMeasure(connection)
        self.order_material_model = OrderMaterial(connection)
        self.database = database

    def create_sample_data(self):
        print("Создание таблиц...")
        self.client_model._create_table()
        self.master_model._create_table()
        self.model_model._create_table()
        self.silk_model._create_table()
        self.order_model._create_table()
        self.order_cost_model._create_table()
        self.client_measure_model._create_table()
        self.order_material_model._create_table()
        print("Таблицы созданы")

        # Создание клиентов
        clients_data = [
            (1, 'Иванов Петр Сергеевич', 'ул. Ленина, 10-5', '+7(495)123-45-67'),
            (2, 'Петрова Анна Ивановна', 'ул. Гагарина, 25-12', '+7(495)234-56-78'),
            (3, 'Сидоров Алексей Владимирович', 'пр. Мира, 5-34', '+7(495)345-67-89'),
            (4, 'Козлова Елена Дмитриевна', 'ул. Пушкина, 15-7', '+7(495)456-78-90'),
            (5, 'Морозов Денис Андреевич', 'ул. Советская, 8-21', '+7(495)567-89-01'),
            (6, 'Волкова Татьяна Сергеевна', 'ул. Кирова, 12-3', '+7(495)678-90-12'),
            (7, 'Соколов Игорь Павлович', 'пр. Победы, 30-15', '+7(495)789-01-23')
        ]
        for client in clients_data:
            self.client_model.create(*client)
        print("Клиенты созданы")

        # Создание мастеров
        masters_data = [
            (1, 'Оруэлл Джордж', 'ул. Центральная, 3-8', '+7(495)111-22-33', '4501 123456', '1985-03-15', 62000, 8),
            (2, 'Кейдж Николас', 'ул. Садовая, 17-4', '+7(495)222-33-44', '4502 234567', '1990-07-22', 55000, 7),
            (3, 'Цепеш Алукард Интегров', 'ул. Новая, 6-15', '+7(495)333-44-55', '4503 345678', '1982-11-05', 65000, 10),
            (4, 'Николсон Джек', 'ул. Зеленая, 9-23', '+7(495)444-55-66', '4504 456789', '1988-09-18', 60000, 5),
            (5, 'Замятин Евгений Иванович', 'ул. Парковая, 4-11', '+7(495)555-66-77', '4505 567890', '1979-12-30', 53000, 12),
            (6, 'Куприянов Михаил', 'ул. Речная, 22-6', '+7(495)666-77-88', '4506 678901', '1992-05-07', 58000, 4),
            (7, 'Крылов Порфирий', 'ул. Школьная, 11-31', '+7(495)777-88-99', '4507 789012', '1983-08-25', 59000, 9)
        ]
        for master in masters_data:
            self.master_model.create(*master)
        print("Мастера созданы")

        # Создание моделей
        models_data = [
            (1, 'DR-001', 'Платье вечернее', 'wn', 8500, 1200, '2023-01-15'),
            (2, 'ST-002', 'Костюм мужской', 'sp', 12000, 2000, '2023-02-10'),
            (3, 'SK-003', 'Юбка карандаш', 'sm', 3500, 500, '2023-03-05'),
            (4, 'SH-004', 'Рубашка классическая', 'au', 2800, 400, '2023-04-20'),
            (5, 'TR-005', 'Брюки женские', 'wn', 4200, 600, '2023-05-12'),
            (6, 'CT-006', 'Пальто зимнее', 'wn', 18500, 3500, '2023-06-08'),
            (7, 'BL-007', 'Блуза', 'sm', 2900, 450, '2023-07-25')
        ]
        for model in models_data:
            self.model_model.create(*model)
        print("Модели созданы")

        # Создание тканей
        silks_data = [
            (1, 'Красный', 1.5, 850, 50, 'Шелк натуральный'),
            (2, 'Синий', 1.4, 720, 35, 'Шелк армани'),
            (3, 'Черный', 1.5, 950, 42, 'Шелк крепдешин'),
            (4, 'Белый', 1.4, 680, 28, 'Шелк шифон'),
            (5, 'Зеленый', 1.5, 790, 33, 'Шелк атлас'),
            (6, 'Фиолетовый', 1.4, 880, 22, 'Шелк бархат'),
            (7, 'Бежевый', 1.5, 750, 45, 'Шелк жаккард')
        ]
        for silk in silks_data:
            self.silk_model.create(*silk)
        print("Ткани созданы")

        # Создание заказов
        orders_data = [
            (1001, 'completed', '2023-09-15', '2023-09-30', 1, 3, 1),
            (1002, 'in_progress', '2023-12-05', None, 2, 1, 2),
            (1003, 'completed', '2023-10-10', '2023-10-25', 3, 5, 3),
            (1004, 'pending', '2023-12-10', None, 4, 2, 5),
            (1005, 'completed', '2023-11-01', '2023-11-28', 5, 4, 6),
            (1006, 'in_progress', '2023-12-01', None, 6, 6, 4),
            (1007, 'completed', '2023-09-05', '2023-09-20', 7, 7, 7)
        ]
        for order in orders_data:
            self.order_model.create(*order)
        print("Заказы созданы")

        # Создание стоимостей заказов
        costs_data = [
            (201, 2500, 4250, 6750, 6750, 'paid', 1001),
            (202, 3200, 5700, 8900, 5000, 'partial', 1002),
            (203, 1800, 2520, 4320, 4320, 'paid', 1003),
            (204, 2200, 0, 2200, 1000, 'partial', 1004),
            (205, 4500, 6160, 10660, 10660, 'paid', 1005),
            (206, 1500, 2040, 3540, 0, 'unpaid', 1006),
            (207, 2100, 3750, 5850, 5850, 'paid', 1007)
        ]
        for cost in costs_data:
            self.order_cost_model.create(*cost)
        print("Стоимости заказов созданы")

        # Создание мерок клиентов
        measures_data = [
            (101, '2023-09-10', 'ОГ=92, ОТ=70, ОБ=96', 1),
            (102, '2023-12-15', 'ОГ=93, ОТ=71, ОБ=97', 1),
            (103, '2023-08-05', 'ОГ=88, ОТ=65, ОБ=92', 2),
            (104, '2023-11-20', 'ОГ=88, ОТ=66, ОБ=93', 2),
            (105, '2023-10-12', 'ОГ=98, ОТ=82, ОБ=102', 3),
            (106, '2023-09-28', 'ОГ=90, ОТ=68, ОБ=94', 4),
            (107, '2023-11-05', 'ОГ=95, ОТ=78, ОБ=100', 5)
        ]
        for measure in measures_data:
            self.client_measure_model.create(*measure)
        print("Мерки созданы")

        # Создание материалов заказов
        materials_data = [
            (1, None, 4250, 2.5, 1001, 1),
            (2, None, 5700, 3.0, 1002, 3),
            (3, None, 2520, 1.8, 1003, 2),
            (4, 'Ткань клиента', 0, 2.2, 1004, 5),
            (5, None, 6160, 3.5, 1005, 6),
            (6, None, 2040, 1.5, 1006, 4),
            (7, None, 3750, 2.0, 1007, 7)
        ]
        for material in materials_data:
            self.order_material_model.create(*material)
        print("Материалы заказов созданы")

    def show_all_data(self):
        print("\n=== КЛИЕНТЫ ===")
        print(pd.DataFrame(self.client_model.get_all(),
                          columns=['id', 'full_name', 'address', 'phone']))
        
        print("\n=== МАСТЕРА ===")
        print(pd.DataFrame(self.master_model.get_all(),
                          columns=['id', 'full_name', 'address', 'phone', 'passport', 'birthdate', 'salary', 'exp']))
        
        print("\n=== МОДЕЛИ ===")
        print(pd.DataFrame(self.model_model.get_all(),
                          columns=['id', 'article', 'type', 'season', 'price', 'add_price', 'date']))
        
        print("\n=== ТКАНИ ===")
        print(pd.DataFrame(self.silk_model.get_all(),
                          columns=['id', 'color', 'width', 'price_meter', 'amount', 'name']))
        
        print("\n=== ЗАКАЗЫ ===")
        print(pd.DataFrame(self.order_model.get_all(),
                          columns=['id', 'status', 'start_date', 'end_date', 'client_id', 'master_id', 'model_id']))
    
    def execute_queries(self):
        """Выполнение всех аналитических запросов"""
        print("\n" + "="*80)
        print("ВЫПОЛНЕНИЕ АНАЛИТИЧЕСКИХ ЗАПРОСОВ")
        print("="*80)
        
        self.query1_all_clients()
        self.query2_masters_exp_gt_8()
        self.query3_winter_models()
        self.query4_orders_with_clients_masters()
        self.query5_client_payment_summary()
        self.query6_orders_by_silk_color()
        self.query7_master_order_count()
        self.query8_expensive_clients_expensive_silks()
    
    # ----------------------------------------------------------------------
    # ПРОСТЫЕ ЗАПРОСЫ
    # ----------------------------------------------------------------------
    
    def query1_all_clients(self):
        """Запрос 1: Найти всех клиентов с указанием адреса и телефона"""
        query = """
        SELECT 
            full_name AS ClientName, 
            address AS Address, 
            phone AS Phone
        FROM clients
        ORDER BY full_name
        """
        result = self.connection.client.query(query).result_rows
        df = pd.DataFrame(result, columns=['ClientName', 'Address', 'Phone'])
        print("\n1. Все клиенты:")
        print(df.to_string(index=False))
        return df
    
    def query2_masters_exp_gt_8(self):
        """Запрос 2: Найти мастеров со стажем более 8 лет"""
        query = """
        SELECT 
            full_name AS MasterName, 
            exp AS Experience, 
            salary AS Salary
        FROM masters
        WHERE exp > 8
        """
        result = self.connection.client.query(query).result_rows
        df = pd.DataFrame(result, columns=['MasterName', 'Experience', 'Salary'])
        print("\n2. Мастера со стажем более 8 лет:")
        print(df.to_string(index=False))
        return df
    
    def query3_winter_models(self):
        """Запрос 3: Показать все модели зимнего сезона ('wn')"""
        query = """
        SELECT 
            article AS Article, 
            type AS Type, 
            price AS Price
        FROM models
        WHERE season = 'wn'
        """
        result = self.connection.client.query(query).result_rows
        df = pd.DataFrame(result, columns=['Article', 'Type', 'Price'])
        print("\n3. Модели зимнего сезона:")
        print(df.to_string(index=False))
        return df
    
    # ----------------------------------------------------------------------
    # ЗАПРОСЫ СРЕДНЕЙ СЛОЖНОСТИ
    # ----------------------------------------------------------------------
    
    def query4_orders_with_clients_masters(self):
        """Запрос 4: Получить все заказы с информацией о клиентах и мастерах"""
        query = """
        SELECT 
            o.id AS OrderID,
            o.status AS Status,
            c.full_name AS Client,
            m.full_name AS Master,
            o.start_date AS StartDate,
            o.end_date AS EndDate
        FROM orders o
        JOIN clients c ON o.client_id = c.id
        JOIN masters m ON o.master_id = m.id
        ORDER BY o.start_date DESC
        """
        result = self.connection.client.query(query).result_rows
        df = pd.DataFrame(result, columns=['OrderID', 'Status', 'Client', 'Master', 'StartDate', 'EndDate'])
        print("\n4. Все заказы с информацией о клиентах и мастерах:")
        print(df.to_string(index=False))
        return df
    
    def query5_client_payment_summary(self):
        """Запрос 5: Найти общую сумму оплат по каждому клиенту"""
        query = """
        SELECT 
            c.full_name AS Client,
            COUNT(o.id) AS OrdersCount,
            SUM(oc.paid_amount) AS TotalPaid,
            AVG(oc.total_cost) AS AvgOrderCost
        FROM clients c
        JOIN orders o ON c.id = o.client_id
        JOIN order_costs oc ON o.id = oc.order_id
        GROUP BY c.id, c.full_name
        ORDER BY TotalPaid DESC
        """
        result = self.connection.client.query(query).result_rows
        df = pd.DataFrame(result, columns=['Client', 'OrdersCount', 'TotalPaid', 'AvgOrderCost'])
        print("\n5. Сумма оплат по каждому клиенту:")
        print(df.to_string(index=False))
        return df
    
    def query6_orders_by_silk_color(self):
        """Запрос 6: Показать заказы, использующие ткань определенного цвета"""
        query = """
        SELECT 
            o.id AS OrderID,
            s.name AS SilkName,
            s.color AS Color,
            s.price_meter AS PricePerMeter,
            s.amount AS AvailableAmount
        FROM orders o
        JOIN order_materials om ON o.id = om.order_id
        JOIN silk s ON om.silk_id = s.id
        WHERE s.color IN ('Красный', 'Синий')
        """
        result = self.connection.client.query(query).result_rows
        df = pd.DataFrame(result, columns=['OrderID', 'SilkName', 'Color', 'PricePerMeter', 'AvailableAmount'])
        print("\n6. Заказы с тканью красного или синего цвета:")
        print(df.to_string(index=False))
        return df
    
    def query7_master_order_count(self):
        """Запрос 7: Найти мастеров и количество выполненных ими заказов"""
        query = """
        SELECT 
            m.full_name AS Master,
            COUNT(o.id) AS OrdersCompleted
        FROM masters m
        LEFT JOIN orders o ON m.id = o.master_id
        GROUP BY m.id, m.full_name
        ORDER BY OrdersCompleted DESC
        """
        result = self.connection.client.query(query).result_rows
        df = pd.DataFrame(result, columns=['Master', 'OrdersCompleted'])
        print("\n7. Количество заказов по мастерам:")
        print(df.to_string(index=False))
        return df
    
    # ----------------------------------------------------------------------
    # СЛОЖНЫЕ ЗАПРОСЫ
    # ----------------------------------------------------------------------
    
    def query8_expensive_clients_expensive_silks(self):
        """Запрос 8: Найти клиентов с заказами >8000 руб и тканью >800 руб/м"""
        query = """
        SELECT DISTINCT
            c.full_name AS Client,
            c.phone AS Phone,
            groupArray(DISTINCT o.id) AS ExpensiveOrders,
            groupArray(DISTINCT s.name) AS ExpensiveSilks
        FROM clients c
        JOIN orders o ON c.id = o.client_id
        JOIN order_costs oc ON o.id = oc.order_id
        JOIN order_materials om ON o.id = om.order_id
        JOIN silk s ON om.silk_id = s.id
        WHERE oc.total_cost > 8000 AND s.price_meter > 800
        GROUP BY c.id, c.full_name, c.phone
        """
        result = self.connection.client.query(query).result_rows
        df = pd.DataFrame(result, columns=['Client', 'Phone', 'ExpensiveOrders', 'ExpensiveSilks'])
        print("\n8. Клиенты с дорогими заказами и дорогими тканями:")
        print(df.to_string(index=False))
        return df


def main():
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", 8123))
    username = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "0000")

    connection = DatabaseConnection(host, port, username, password, database='atelier')
    db_service = DatabaseService(connection)

    connection.clear_database()
    db_service.create_sample_data()
    
    db_service.show_all_data()
    
    last_client_id = db_service.client_model.get_last_id()
    db_service.client_model.create(
        last_client_id + 1, 
        "Тестовый Клиент", 
        "ул. Тестовая, 1-1", 
        "+7(999)999-99-99"
    )
    print("\nДобавлен новый клиент")
    
    db_service.show_all_data()

    db_service.execute_queries()

    connection.close()

if __name__ == "__main__":
    main()