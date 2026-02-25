import pandas as pd
import psycopg2
import matplotlib.pyplot as plt

connection = psycopg2.connect(
    database="students",
    user="postgres",
    password="191919",
    host="127.0.0.1",
    port="5432"
    )

def build_hist_1(limiter: list[int] = [3_000, 12_000]):
    limiter_cond: str = ""
    if len(limiter) == 2:
        limiter_cond = f"having avg(e.salary) >= {limiter[0]} and avg(e.salary) <= {limiter[1]}"

    sql = f"""select d.department_name, avg(e.salary) as avg_salary
    from departments d join employees e on e.manager_id = d.manager_id
    group by d.department_name
    {limiter_cond};"""
    df = pd.read_sql_query(sql, connection)
    plt.barh(df['department_name'], df['avg_salary'], color='red')
    plt.title('Средняя зарплата по отделам', fontsize=14, fontweight='bold')
    plt.xlabel('Название отдела')
    plt.ylabel('Средняя зарплата')
    plt.tight_layout()
    plt.show()

def build_hist_2(limiter: list[int] = [7_000, 20_000]):
    limiter_cond: str = ""
    if len(limiter) == 2:
        limiter_cond = f"having sum(e.salary) >= {limiter[0]} and sum(e.salary) <= {limiter[1]}"

    sql = f"""select j.job_title, MAX(e.salary) as max_salary
    from employees e join jobs j on e.job_id = j.job_id 
    group by j.job_title
    {limiter_cond};"""
    df = pd.read_sql_query(sql, connection)
    plt.barh(df['job_title'], df['max_salary'], color='green')
    plt.title('Максимальная зарплата по должностям', fontsize=14, fontweight='bold')
    plt.xlabel('Должность сотрудника')
    plt.ylabel('Максимальная зарплата')
    plt.tight_layout()
    plt.show()

def build_hist_3():
    sql = f"""select l.city, sum(e.salary) as sum_salary
    from employees e join locations l on e.location_id = l.location_id
    group by l.city;"""
    df = pd.read_sql_query(sql, connection)
    plt.barh(df['city'], df['sum_salary'])
    plt.title('Суммарная зарплата по регионам', fontsize=14, fontweight='bold')
    plt.xlabel('Суммарная зарплата')
    plt.ylabel('Регион')
    plt.tight_layout()
    plt.show()

build_hist_3()
