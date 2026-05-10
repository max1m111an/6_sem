import sys
import os
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QDialog, QTabWidget, QWidget,
    QVBoxLayout, QHBoxLayout, QFormLayout, QPushButton, QLabel,
    QLineEdit, QListWidget, QTableView, QComboBox, QMessageBox,
    QDialogButtonBox, QSpinBox, QDoubleSpinBox,
    QDateEdit, QTimeEdit
)
from PyQt5.QtCore import Qt, QDate, QTime
from PyQt5.QtGui import QStandardItemModel, QStandardItem
import psycopg2
from psycopg2 import sql, Error
from dotenv import load_dotenv
import matplotlib
matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import traceback
from collections import defaultdict

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.current_host = None
        self.current_port = None
        self.current_db = None
        self.current_user = None

    def connect(self, host, port, database, user, password):
        try:
            self.conn = psycopg2.connect(
                host=host, port=port, database=database,
                user=user, password=password
            )
            self.cursor = self.conn.cursor()
            self.cursor.execute("SET search_path TO post")
            self.current_host = host
            self.current_port = port
            self.current_db = database
            self.current_user = user
            return True, f"Подключение установлено как '{user}'"
        except Error as e:
            return False, str(e)

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query, params=None, fetch=True):
        try:
            self.cursor.execute(query, params)
            if fetch and self.cursor.description:
                columns = [desc[0] for desc in self.cursor.description]
                rows = self.cursor.fetchall()
                return columns, rows
            else:
                self.conn.commit()
                return None, None
        except Error as e:
            self.conn.rollback()
            raise e

    def get_available_roles(self):
        """Получить список ролей с правом LOGIN"""
        try:
            query = """
                SELECT rolname 
                FROM pg_roles 
                WHERE rolcanlogin = true 
                  AND rolname NOT LIKE 'pg_%'
                ORDER BY rolname
            """
            cols, rows = self.execute_query(query)
            return [row[0] for row in rows]
        except Exception:
            return []

    def get_tables(self):
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
        """
        cols, rows = self.execute_query(query)
        return rows

    def get_columns(self, schema, table):
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        return self.execute_query(query, (schema, table))

    def get_primary_keys(self, schema, table):
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s AND tc.table_name = %s
        """
        cols, rows = self.execute_query(query, (schema, table))
        return [row[0] for row in rows]

    def get_table_data(self, schema, table, limit=1000):
        query = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
            sql.Identifier(schema), sql.Identifier(table))
        return self.execute_query(query, (limit,))

    def insert_row(self, schema, table, column_values):
        columns = list(column_values.keys())
        values = [column_values[c] for c in columns]
        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING *").format(
            sql.Identifier(schema), sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(values))
        )
        self.execute_query(query, values, fetch=True)
        return

    def update_row(self, schema, table, pk_column, pk_value, column_values):
        set_clauses = []
        params = []
        for col, val in column_values.items():
            set_clauses.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
            params.append(val)
        params.append(pk_value)
        query = sql.SQL("UPDATE {}.{} SET {} WHERE {} = %s").format(
            sql.Identifier(schema), sql.Identifier(table),
            sql.SQL(', ').join(set_clauses),
            sql.Identifier(pk_column)
        )
        self.execute_query(query, params, fetch=False)

    def delete_row(self, schema, table, pk_column, pk_value):
        query = sql.SQL("DELETE FROM {}.{} WHERE {} = %s").format(
            sql.Identifier(schema), sql.Identifier(table),
            sql.Identifier(pk_column)
        )
        self.execute_query(query, (pk_value,), fetch=False)

    def call_function(self, func_name, params):
        try:
            if not params:
                query = f"SELECT * FROM {func_name}()"
                self.cursor.execute(query)
            else:
                placeholders = ', '.join(['%s'] * len(params))
                query = f"SELECT * FROM {func_name}({placeholders})"
                self.cursor.execute(query, params)
            
            if self.cursor.description:
                columns = [desc[0] for desc in self.cursor.description]
                rows = self.cursor.fetchall()
                return columns, rows
            else:
                return None, None
        except Error as e:
            self.conn.rollback()
            raise e


class PostgresAuthDialog(QDialog):
    """Окно обязательной авторизации пользователя PostgreSQL"""

    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db = db_manager
        self.authenticated_user = None
        self.setWindowTitle("Авторизация пользователя PostgreSQL")
        self.setFixedSize(450, 300)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        # Заголовок
        title = QLabel("Вход в ИС почты")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 14px; font-weight: bold; margin: 10px;")
        title.setWordWrap(True)
        layout.addWidget(title)

        # Информация о подключении
        info_label = QLabel(
            f"База данных: {self.db.current_db}\n"
            f"Сервер: {self.db.current_host}:{self.db.current_port}"
        )
        info_label.setStyleSheet("color: gray; font-size: 11px;")
        info_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(info_label)

        # Форма ввода
        form = QFormLayout()

        # Выпадающий список ролей
        self.role_combo = QComboBox()
        self.role_combo.setEditable(False)
        self.load_roles()
        form.addRow("Роль:", self.role_combo)

        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.password_edit.setPlaceholderText("Введите пароль")
        form.addRow("Пароль:", self.password_edit)

        layout.addLayout(form)

        # Кнопка обновления списка ролей
        refresh_layout = QHBoxLayout()
        self.refresh_roles_btn = QPushButton("Обновить список ролей")
        self.refresh_roles_btn.clicked.connect(self.load_roles)
        refresh_layout.addStretch()
        refresh_layout.addWidget(self.refresh_roles_btn)
        refresh_layout.addStretch()
        layout.addLayout(refresh_layout)

        # Кнопки
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.button(QDialogButtonBox.Ok).setText("Войти")
        btn_box.button(QDialogButtonBox.Cancel).setText("Закрыть")
        btn_box.accepted.connect(self.authenticate)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

        self.setLayout(layout)
        self.password_edit.setFocus()

    def load_roles(self):
        """Загрузка списка доступных ролей из PostgreSQL"""
        self.role_combo.clear()
        try:
            roles = self.db.get_available_roles()
            if roles:
                self.role_combo.addItems(roles)
                # Автовыбор administrator, если есть
                if 'administrator' in roles:
                    self.role_combo.setCurrentText('administrator')
            else:
                self.role_combo.addItem("Нет доступных ролей")
        except Exception as e:
            self.role_combo.addItem(f"Ошибка загрузки: {str(e)[:50]}")

    def authenticate(self):
        login = self.role_combo.currentText().strip()
        password = self.password_edit.text().strip()

        if not login or not password:
            QMessageBox.warning(self, "Предупреждение", "Выберите роль и введите пароль!")
            return

        if login in ("Нет доступных ролей", "") or login.startswith("Ошибка"):
            QMessageBox.critical(self, "Ошибка", "Список ролей не загружен. Обновите список.")
            return

        try:
            host = self.db.current_host
            port = self.db.current_port
            db = self.db.current_db
            test_conn = psycopg2.connect(
                host=host,
                port=port,
                database=db,
                user=login,
                password=password
            )
            test_conn.close()

            self.db.disconnect()
            success, msg = self.db.connect(host, port, db, login, password)

            if success:
                self.authenticated_user = login
                QMessageBox.information(
                    self, "Успех",
                    f"Авторизация выполнена!\nТекущий пользователь: {login}\n\n"
                    f"Права доступа определяются ролью '{login}'."
                )
                self.accept()
            else:
                QMessageBox.critical(self, "Ошибка", f"Не удалось подключиться:\n{msg}")

        except Error as e:
            QMessageBox.critical(
                self, "Ошибка",
                f"Неверный пароль для роли '{login}'.\n\n{str(e)}"
            )
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка авторизации:\n{str(e)}")

    def get_authenticated_user(self):
        return self.authenticated_user


class TablesTab(QWidget):
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db = db_manager
        self.current_schema = None
        self.current_table = None
        self.pk_columns = []
        self.init_ui()

    def init_ui(self):
        main_layout = QHBoxLayout()

        left_layout = QVBoxLayout()
        left_layout.addWidget(QLabel("Таблицы:"))
        self.table_list = QListWidget()
        self.table_list.itemClicked.connect(self.on_table_selected)
        left_layout.addWidget(self.table_list)
        self.refresh_btn = QPushButton("Обновить список")
        self.refresh_btn.clicked.connect(self.refresh_tables)
        left_layout.addWidget(self.refresh_btn)

        right_layout = QVBoxLayout()
        self.info_label = QLabel("Выберите таблицу")
        right_layout.addWidget(self.info_label)

        self.data_view = QTableView()
        self.data_view.setSelectionBehavior(QTableView.SelectRows)
        self.data_view.setSelectionMode(QTableView.SingleSelection)
        right_layout.addWidget(self.data_view)

        btn_layout = QHBoxLayout()
        self.add_btn = QPushButton("Добавить")
        self.edit_btn = QPushButton("Редактировать")
        self.delete_btn = QPushButton("Удалить")
        self.add_btn.clicked.connect(self.add_record)
        self.edit_btn.clicked.connect(self.edit_record)
        self.delete_btn.clicked.connect(self.delete_record)
        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.edit_btn)
        btn_layout.addWidget(self.delete_btn)
        right_layout.addLayout(btn_layout)

        main_layout.addLayout(left_layout, 1)
        main_layout.addLayout(right_layout, 4)
        self.setLayout(main_layout)

        self.model = QStandardItemModel()
        self.data_view.setModel(self.model)
        self.refresh_tables()

    def refresh_tables(self):
        self.table_list.clear()
        try:
            tables = self.db.get_tables()
            for schema, table in tables:
                self.table_list.addItem(f"{schema}.{table}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def on_table_selected(self, item):
        table_full = item.text()
        parts = table_full.split('.')
        if len(parts) == 2:
            self.current_schema, self.current_table = parts
            self.load_table_data()

    def load_table_data(self):
        if not self.current_table:
            return
        try:
            cols, _ = self.db.get_columns(self.current_schema, self.current_table)
            self.pk_columns = self.db.get_primary_keys(self.current_schema, self.current_table)
            data_cols, rows = self.db.get_table_data(self.current_schema, self.current_table)
            self.model.clear()
            self.model.setHorizontalHeaderLabels(data_cols)
            for row in rows:
                items = [QStandardItem(str(val) if val is not None else 'NULL') for val in row]
                self.model.appendRow(items)
            self.info_label.setText(f"Таблица: {self.current_schema}.{self.current_table} | Строк: {len(rows)}")
            self.data_view.resizeColumnsToContents()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить данные:\n{str(e)}")

    def get_record_dialog(self, record_data=None):
        dialog = QDialog(self)
        dialog.setWindowTitle("Редактирование записи" if record_data else "Добавление записи")
        layout = QVBoxLayout()
        form = QFormLayout()

        cols, _ = self.db.get_columns(self.current_schema, self.current_table)
        entries = {}
        for col_name, data_type, is_nullable in cols:
            if col_name in self.pk_columns and record_data:
                label = QLabel(str(record_data[col_name]) if record_data else 'auto')
                form.addRow(f"{col_name} (PK):", label)
                entries[col_name] = label
                continue

            if data_type in ('integer', 'smallint', 'bigint', 'serial'):
                widget = QSpinBox()
                widget.setRange(-2147483648, 2147483647)
                if record_data and record_data[col_name] is not None:
                    widget.setValue(int(record_data[col_name]))
            elif data_type in ('numeric', 'decimal', 'real', 'double precision'):
                widget = QDoubleSpinBox()
                widget.setRange(-1e9, 1e9)
                widget.setDecimals(3)
                if record_data and record_data[col_name] is not None:
                    widget.setValue(float(record_data[col_name]))
            elif data_type in ('date',):
                widget = QDateEdit()
                widget.setCalendarPopup(True)
                widget.setDisplayFormat("yyyy-MM-dd")
                if record_data and record_data[col_name] is not None:
                    widget.setDate(QDate.fromString(str(record_data[col_name]), "yyyy-MM-dd"))
            elif data_type in ('timestamp without time zone', 'timestamp'):
                widget = QLineEdit()
                if record_data and record_data[col_name] is not None:
                    widget.setText(str(record_data[col_name]))
                else:
                    widget.setPlaceholderText("YYYY-MM-DD HH:MM:SS")
            elif data_type == 'time without time zone':
                widget = QTimeEdit()
                widget.setDisplayFormat("HH:mm:ss")
                if record_data and record_data[col_name] is not None:
                    widget.setTime(QTime.fromString(str(record_data[col_name]), "HH:mm:ss"))
            else:
                widget = QLineEdit()
                if record_data and record_data[col_name] is not None:
                    widget.setText(str(record_data[col_name]))

            entries[col_name] = widget
            nullable_text = " (NOT NULL)" if is_nullable == 'NO' else ""
            form.addRow(f"{col_name}{nullable_text}:", widget)

        layout.addLayout(form)
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(dialog.accept)
        btn_box.rejected.connect(dialog.reject)
        layout.addWidget(btn_box)
        dialog.setLayout(layout)
        return dialog, entries

    def add_record(self):
        if not self.current_table:
            return
        dialog, entries = self.get_record_dialog()
        if dialog.exec_() == QDialog.Accepted:
            try:
                values = {}
                for col, widget in entries.items():
                    if isinstance(widget, QLabel):
                        continue
                    if isinstance(widget, QSpinBox) or isinstance(widget, QDoubleSpinBox):
                        values[col] = widget.value()
                    elif isinstance(widget, QDateEdit):
                        values[col] = widget.date().toString("yyyy-MM-dd")
                    elif isinstance(widget, QTimeEdit):
                        values[col] = widget.time().toString("HH:mm:ss")
                    elif isinstance(widget, QLineEdit):
                        text = widget.text().strip()
                        values[col] = text if text else None

                self.db.insert_row(self.current_schema, self.current_table, values)
                QMessageBox.information(self, "Успех", "Запись добавлена")
                self.load_table_data()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось добавить запись:\n{str(e)}")

    def edit_record(self):
        if not self.current_table or not self.pk_columns:
            QMessageBox.warning(self, "Предупреждение", "Невозможно редактировать без первичного ключа")
            return
        selection = self.data_view.selectionModel().selectedRows()
        if not selection:
            QMessageBox.warning(self, "Предупреждение", "Выберите строку для редактирования")
            return
        row = selection[0].row()
        record = {}
        for col_idx in range(self.model.columnCount()):
            col_name = self.model.horizontalHeaderItem(col_idx).text()
            val = self.model.item(row, col_idx).text()
            if val == 'NULL':
                val = None
            record[col_name] = val

        dialog, entries = self.get_record_dialog(record)
        if dialog.exec_() == QDialog.Accepted:
            try:
                values = {}
                for col, widget in entries.items():
                    if isinstance(widget, QLabel):
                        continue
                    if isinstance(widget, QSpinBox) or isinstance(widget, QDoubleSpinBox):
                        values[col] = widget.value()
                    elif isinstance(widget, QDateEdit):
                        values[col] = widget.date().toString("yyyy-MM-dd")
                    elif isinstance(widget, QTimeEdit):
                        values[col] = widget.time().toString("HH:mm:ss")
                    elif isinstance(widget, QLineEdit):
                        text = widget.text().strip()
                        values[col] = text if text else None

                pk_col = self.pk_columns[0]
                pk_val = record[pk_col]
                self.db.update_row(self.current_schema, self.current_table, pk_col, pk_val, values)
                QMessageBox.information(self, "Успех", "Запись обновлена")
                self.load_table_data()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось обновить запись:\n{str(e)}")

    def delete_record(self):
        if not self.current_table or not self.pk_columns:
            QMessageBox.warning(self, "Предупреждение", "Требуется первичный ключ")
            return
        selection = self.data_view.selectionModel().selectedRows()
        if not selection:
            return
        row = selection[0].row()
        pk_col = self.pk_columns[0]
        col_index = None
        for i in range(self.model.columnCount()):
            if self.model.horizontalHeaderItem(i).text() == pk_col:
                col_index = i
                break
        if col_index is not None:
            pk_val = self.model.item(row, col_index).text()
        else:
            return

        reply = QMessageBox.question(self, "Подтверждение",
                                     f"Удалить запись с {pk_col}={pk_val}?",
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            try:
                self.db.delete_row(self.current_schema, self.current_table, pk_col, pk_val)
                self.load_table_data()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Удаление не удалось:\n{str(e)}")


class ProceduresTab(QWidget):
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db = db_manager
        self.functions = [
            {"name": "count_by_type_per_day", "params": [("p_office_id", "integer"), ("p_date", "date")],
             "description": "Кол-во отправлений по типам за день в отделении"},

            {"name": "employee_sendings_by_type", "params": [],
             "description": "Список сотрудников с подсчётом по типам"},

            {"name": "first_dispatch_date", "params": [("p_sender_id", "integer")],
             "description": "Дата первого отправления клиента"},

            {"name": "employee_schedule", "params": [("p_employee_id", "integer")],
             "description": "Расписание работы сотрудника"},

            {"name": "total_cost_by_employee", "params": [("p_employee_id", "integer")],
             "description": "Суммарная стоимость отправлений сотрудника"},

            {"name": "top_employee_by_mail_type", "params": [],
             "description": "Лучшие сотрудники по типам отправлений"},

            {"name": "avg_daily_dispatch_by_region", "params": [("p_start_date", "date"), ("p_end_date", "date")],
             "description": "Среднее кол-во отправлений в день по регионам за период"},

            {"name": "employees_above_average", "params": [],
             "description": "Сотрудники с кол-вом отправлений выше среднего по отделению"},

            {"name": "anomalous_clients", "params": [("p_min_sendings", "integer"), ("p_min_cost", "numeric")],
             "description": "Клиенты с аномальной активностью"},

            {"name": "top_clients_by_month", "params": [("p_top_n", "integer")],
             "description": "Топ клиентов по стоимости за месяц"},
        ]
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        self.func_combo = QComboBox()
        self.func_combo.addItem("-- Выберите функцию --")
        for func in self.functions:
            self.func_combo.addItem(f"{func['name']} - {func['description']}")
        self.func_combo.currentIndexChanged.connect(self.on_func_selected)
        layout.addWidget(self.func_combo)

        self.params_widget = QWidget()
        self.params_layout = QFormLayout()
        self.params_widget.setLayout(self.params_layout)
        layout.addWidget(self.params_widget)

        self.run_btn = QPushButton("Выполнить")
        self.run_btn.clicked.connect(self.execute_function)
        layout.addWidget(self.run_btn)

        self.result_view = QTableView()
        self.result_model = QStandardItemModel()
        self.result_view.setModel(self.result_model)
        layout.addWidget(self.result_view)

        self.setLayout(layout)
        self.current_func = None

    def on_func_selected(self, index):
        if index == 0:
            self.current_func = None
            self.clear_params()
            return
        self.current_func = self.functions[index - 1]
        self.build_params_inputs()

    def build_params_inputs(self):
        for i in reversed(range(self.params_layout.count())):
            widget = self.params_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        self.param_widgets = []
        for name, ptype in self.current_func['params']:
            if ptype == 'integer':
                widget = QSpinBox()
                widget.setRange(-1000000, 1000000)
            elif ptype == 'numeric':
                widget = QDoubleSpinBox()
                widget.setDecimals(2)
                widget.setRange(0, 1e9)
            elif ptype == 'date':
                widget = QDateEdit()
                widget.setCalendarPopup(True)
                widget.setDisplayFormat("yyyy-MM-dd")
                widget.setDate(QDate.currentDate())
            else:
                widget = QLineEdit()
            label = QLabel(name.replace('p_', '').replace('_', ' ').title() + ':')
            self.params_layout.addRow(label, widget)
            self.param_widgets.append((name, ptype, widget))

    def clear_params(self):
        for i in reversed(range(self.params_layout.count())):
            widget = self.params_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        self.param_widgets = []

    def execute_function(self):
        if not self.current_func:
            QMessageBox.warning(self, "Предупреждение", "Выберите функцию")
            return
        params = []
        for name, ptype, widget in self.param_widgets:
            if ptype == 'integer' or ptype == 'numeric':
                params.append(widget.value())
            elif ptype == 'date':
                params.append(widget.date().toString("yyyy-MM-dd"))
            else:
                params.append(widget.text().strip())
        try:
            columns, rows = self.db.call_function(self.current_func['name'], params)
            self.result_model.clear()
            if columns:
                self.result_model.setHorizontalHeaderLabels(columns)
                for row in rows:
                    items = [QStandardItem(str(v) if v is not None else 'NULL') for v in row]
                    self.result_model.appendRow(items)
                self.result_view.resizeColumnsToContents()
            else:
                QMessageBox.information(self, "Результат", "Функция выполнена без результата")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при вызове функции:\n{str(e)}\n\nПолная ошибка:\n{traceback.format_exc()}")


class VisualizationTab(QWidget):
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db = db_manager
        self.figure = Figure()
        self.canvas = FigureCanvas(self.figure)
        self.ax = self.figure.add_subplot(111)

        layout = QVBoxLayout()
        control_layout = QHBoxLayout()
        control_layout.addWidget(QLabel("Тип графика:"))
        self.plot_combo = QComboBox()
        self.plot_combo.addItem("Круговая: типы отправлений")
        self.plot_combo.addItem("Гистограмма: отправления по регионам за квартал")
        self.plot_combo.addItem("Линейный: стоимость по месяцам")
        self.plot_combo.addItem("Столбчатая: топ клиентов по стоимости")
        control_layout.addWidget(self.plot_combo)
        self.build_btn = QPushButton("Построить")
        self.build_btn.clicked.connect(self.build_plot)
        control_layout.addWidget(self.build_btn)
        layout.addLayout(control_layout)
        layout.addWidget(self.canvas)
        self.setLayout(layout)

    def build_plot(self):
        plot_type = self.plot_combo.currentText()
        self.ax.clear()
        try:
            if "Круговая" in plot_type:
                self.pie_types()
            elif "Гистограмма" in plot_type:
                self.bar_regions()
            elif "Линейный" in plot_type:
                self.line_monthly_cost()
            elif "Столбчатая" in plot_type:
                self.bar_top_clients()
            self.canvas.draw()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось построить график:\n{str(e)}")

    def pie_types(self):
        _, rows = self.db.execute_query(
            "SELECT mt.type_name, COUNT(*) FROM transfers t JOIN mail_types mt ON t.type_id=mt.type_id GROUP BY mt.type_name ORDER BY 2 DESC")
        labels = [r[0] for r in rows]
        sizes = [r[1] for r in rows]
        
        self.ax.clear()
        
        wedges, texts, autotexts = self.ax.pie(
            sizes, 
            labels=None,
            autopct='%1.1f%%', 
            startangle=90,
            pctdistance=0.85,
            explode=[0.02] * len(sizes)
        )
        
        for autotext in autotexts:
            autotext.set_fontsize(8)
            autotext.set_fontweight('bold')
        
        self.ax.legend(
            wedges, 
            [f'{l} ({s})' for l, s in zip(labels, sizes)],
            title="Типы отправлений",
            loc='center left',
            bbox_to_anchor=(1, 0, 0.5, 1),
            fontsize=8,
            title_fontsize=9
        )
        
        self.ax.set_title("Распределение отправлений по типам", fontsize=12, fontweight='bold')
        self.figure.tight_layout()

    def bar_regions(self):
        _, rows = self.db.call_function("avg_daily_dispatch_by_region", ['2024-01-01', '2025-01-01'])
        regions = [r[0] for r in rows]
        values = [float(r[1]) for r in rows]
        
        self.ax.clear()
        bars = self.ax.bar(range(len(regions)), values, color='steelblue')
        
        self.ax.set_xticks(range(len(regions)))
        self.ax.set_xticklabels(regions, rotation=45, ha='right', fontsize=9)
        
        for bar, val in zip(bars, values):
            if val > 0:
                self.ax.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.02,
                            f'{val:.1f}', ha='center', va='bottom', fontsize=8)
        
        self.ax.set_title("Среднее количество отправлений в день по регионам")
        self.ax.set_ylabel("Отправлений в день")
        self.ax.grid(axis='y', alpha=0.3)
        
        self.ax.margins(x=0.05)
        if values:
            self.ax.set_ylim(0, max(values) * 1.15)
        
        self.figure.tight_layout()

    def line_monthly_cost(self):
        cols, rows = self.db.execute_query(
            "SELECT DATE_TRUNC('month', dispatch_date) AS month, SUM(cost) FROM transfers GROUP BY month ORDER BY month")
        months = [r[0].strftime('%Y-%m') for r in rows]
        costs = [float(r[1]) for r in rows]
        self.ax.plot(months, costs, marker='o', color='green', linewidth=2)
        self.ax.set_title("Суммарная стоимость отправлений по месяцам")
        self.ax.tick_params(axis='x', rotation=45)
        self.ax.grid(True)

    def bar_top_clients(self):
        _, rows = self.db.call_function("top_clients_by_month", [5])
        d = defaultdict(float)
        for r in rows: 
            d[f"{r[1]} {r[2]}"] += float(r[3])
        
        items = sorted(d.items(), key=lambda x: x[1], reverse=True)[:10]
        clients = [i[0] for i in items]
        totals = [i[1] for i in items]
        
        self.ax.clear()
        
        bars = self.ax.barh(range(len(clients)), totals, color='coral', height=0.6)
        
        self.ax.set_yticks(range(len(clients)))
        self.ax.set_yticklabels(clients, fontsize=9)
        
        for bar, val in zip(bars, totals):
            if val > 0:
                self.ax.text(
                    bar.get_width() + max(totals) * 0.01,
                    bar.get_y() + bar.get_height() / 2,
                    f'{val:,.0f} ₽',
                    va='center',
                    fontsize=9
                )
        
        self.ax.set_title("Топ клиентов по общей стоимости отправлений", fontsize=12, fontweight='bold')
        self.ax.set_xlabel("Суммарная стоимость (руб.)", fontsize=10)
        self.ax.grid(axis='x', alpha=0.3)
        
        if totals:
            self.ax.set_xlim(0, max(totals) * 1.15)
        
        self.ax.invert_yaxis()
        
        self.figure.tight_layout()


class MainWindow(QMainWindow):
    def __init__(self, db_manager):
        super().__init__()
        self.db = db_manager
        self.current_user = None
        self.setWindowTitle("Post Office")
        self.resize(1200, 800)

        # Верхняя панель
        toolbar = QHBoxLayout()
        self.user_label = QLabel("Пользователь: авторизация требуется")
        self.user_label.setStyleSheet("font-weight: bold; color: red;")
        toolbar.addWidget(self.user_label)
        toolbar.addStretch()

        self.auth_btn = QPushButton("Авторизация PostgreSQL")
        self.auth_btn.clicked.connect(self.show_postgres_auth)
        toolbar.addWidget(self.auth_btn)

        self.change_user_btn = QPushButton("Сменить пользователя")
        self.change_user_btn.clicked.connect(self.show_postgres_auth)
        self.change_user_btn.setEnabled(False)
        toolbar.addWidget(self.change_user_btn)

        # Вкладки (изначально скрыты)
        self.tabs = QTabWidget()
        self.tables_tab = TablesTab(self.db)
        self.procedures_tab = ProceduresTab(self.db)
        self.visualization_tab = VisualizationTab(self.db)

        self.tabs.addTab(self.tables_tab, "Таблицы")
        self.tabs.addTab(self.procedures_tab, "Процедуры")
        self.tabs.addTab(self.visualization_tab, "Визуализация")
        self.tabs.setEnabled(False)  # Отключаем до авторизации

        central_widget = QWidget()
        main_layout = QVBoxLayout()
        main_layout.addLayout(toolbar)
        main_layout.addWidget(self.tabs)
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

    def show_postgres_auth(self):
        if self.current_user:
            reply = QMessageBox.question(
                self, "Смена пользователя",
                f"Вы авторизованы как '{self.current_user}'.\nХотите сменить пользователя?",
                QMessageBox.Yes | QMessageBox.No
            )
            if reply == QMessageBox.No:
                return

        auth_dialog = PostgresAuthDialog(self.db, self)
        if auth_dialog.exec_() == QDialog.Accepted:
            self.current_user = auth_dialog.get_authenticated_user()
            if self.current_user:
                self.user_label.setText(f"Пользователь: {self.current_user}")
                self.user_label.setStyleSheet("font-weight: bold; color: green;")
                self.auth_btn.setText("Повторная авторизация")
                self.change_user_btn.setEnabled(True)
                self.tabs.setEnabled(True)

    def closeEvent(self, event):
        self.db.disconnect()
        event.accept()


def main():
    app = QApplication(sys.argv)

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME")
    admin_user = os.getenv("DB_ADMIN_USER", "postgres")
    admin_password = os.getenv("DB_ADMIN_PASSWORD")

    db_manager = DatabaseManager()
    success, msg = db_manager.connect(host, port, database, admin_user, admin_password)

    if not success:
        QMessageBox.critical(None, "Ошибка подключения",
                             f"Не удалось подключиться к базе данных.\n\n{msg}\n\n"
                             "Проверьте файл .env и доступность сервера.")
        sys.exit(1)

    window = MainWindow(db_manager)
    window.show()

    window.show_postgres_auth()

    if not window.current_user:
        db_manager.disconnect()
        sys.exit(0)

    app.exec_()

    db_manager.disconnect()


if __name__ == "__main__":
    main()