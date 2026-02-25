import pandas as pd
import psycopg2

def make_connection():
    return psycopg2.connect(
        database="students",
        user="postgres",
        password="191919",
        host="127.0.0.1",
        port="5432"
        )

def test_connection(connection):
    cursor = connection.cursor()
    print(connection.get_dsn_parameters(), "\n")
    cursor.execute("SELECT version();")
    version_ps = cursor.fetchone()
    print("Вы подключены к - ", version_ps, "\n")


def create_func_store_data():
    postgresql_func = """
    CREATE OR REPLACE FUNCTION select_data1(id_dept int) RETURNS
    SETOF departments AS $$
    SELECT * FROM departments WHERE departments.department_id >
    id_dept;
    $$ LANGUAGE SQL;
    """
    cursor.execute(postgresql_func)
    connection.commit()

def add_locations_table():
    cursor.execute(
    """ 
    CREATE TABLE if not exists locations
    (location_id int PRIMARY KEY,
    city varchar(30),
    postal_code varchar(12)
    """
    )
    connection.commit()

def fill_locations_by_raws():
    cursor.execute(
    """ 
    INSERT INTO locations VALUES 
    (1,'Roma', '00989'),
    (2,'Venice','10934'),
    (3,'Tokyo', '1689'),
    (4,'Hiroshima','6823'),
    (5,'Southlake', '26192'),
    (6,'South San Francisco', '99236'),
    (7,'South Brunswick','50090'),
    (8,'Seattle','98199'),
    (9,'Toronto','M5V 2L7'),
    (10,'Whitehorse','YSW 9T2');
    """
    )
    connection.commit()

def add_location_id_col():
    cursor.execute(
    """ 
    ALTER TABLE employees
    ADD COLUMN location_id INTEGER;
    """
    )
    connection.commit()

def add_fk_location_id():
    cursor.execute(
    """ 
    ALTER TABLE employees
    ADD CONSTRAINT fk_employee_location 
    FOREIGN KEY (location_id) 
    REFERENCES locations (location_id)
    """
    )
    connection.commit()

def update_location_id_emp():
    cursor.execute(
    """ 
    UPDATE employees 
    SET location_id = department_id / 10;
    """
    )
    connection.commit()

def var_query_1(connection):
    query = pd.read_sql_query("""select d.department_name, round(avg(e.salary), 1) as avg_salary
    from employees e join departments d on e.department_id = d.department_id
    group by d.department_name;""",
    connection)
    print(query)

def var_query_2(connection):
    query = pd.read_sql_query("""select j.job_title, count(e.employee_id) as count_e
    from jobs j join employees e on e.job_id = j.job_id
    group by j.job_title
    order by count_e desc;""",
    connection)
    print(query)

def var_query_3(connection):
    query = pd.read_sql_query("""select l.city, sum(e.salary)
    from locations l join employees e on e.location_id = l.location_id
    group by l.city;""",
    connection)
    print(query)


connection = make_connection()
cursor = connection.cursor()

#test_connection(connection)
#add_locations_table()
#fill_locations_by_raws()
#create_func_store_data()

#add_location_id_col()
#add_fk_location_id()
#update_location_id_emp()

#var_query_1(connection)
var_query_2(connection)
#var_query_3(connection)
