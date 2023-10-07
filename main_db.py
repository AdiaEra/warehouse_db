from psycopg2.errors import UniqueViolation

from DB import conn


def add_order(order_id):
    """
    Функция записи № заказа в таблицу order_number
    :param order_id: номер заказа
    :return: "Новый заказ добавлен" или "Заказ с таким id уже есть в базе"(при дублировании номера заказа)
    """
    with conn.cursor() as cur:
        select_query = """SELECT order_id FROM order_number WHERE order_id = %s"""
        cur.execute(select_query, (order_id,))
        cur.fetchone()
        if cur.fetchone() is None:
            try:
                insert_query = """INSERT INTO order_number(order_id) 
                                  VALUES (%s)"""
                cur.execute(insert_query, (order_id,))
            except UniqueViolation:
                return 'Заказ с таким id уже есть в базе'
        return 'Новый заказ добавлен'


order = '56E-456'
# print(add_order(order))
conn.commit()


def data_order(rack: int, shelf: int, category: str, quantity: int, order_id: str):
    """
    Функция заполнения таблицы order_information с информацией по заказу
    :param rack: номер стеллажа
    :param shelf: номер полки
    :param category: наименование материала
    :param quantity: количество материала
    :param order_id: номер заказа
    :return: Информация по заказу №.. записана
    """
    with conn.cursor() as cur:
        insert_query = """INSERT INTO order_information(rack, shelf, category, quantity, order_id) VALUES (%s, %s, %s, %s, %s)"""
        cur.execute(insert_query, (rack, shelf, category, quantity, order_id))
        return f'Информация по заказу {order_id} записана'


data_rack = 4
data_shelf = 3
data_category = 'мяч'
data_quantity = 12
order = 'YW8-99'
# print(data_order(data_rack, data_shelf, data_category, data_quantity, order))
conn.commit()


def delete_order(order_id):
    """
    Функция удаления заказа со всеми данными по нему
    :param order_id: номер заказа
    :return: Заказ №.. удалён из БД
    """
    with conn.cursor() as cur:
        delete_query = """DELETE FROM order_number WHERE order_id = %s"""
        cur.execute(delete_query, (order_id,))
        return f'Заказ {order_id} удалён из БД'


order = '34Y-ui'
# print(delete_order(order))
conn.commit()


def search_data(order_id):
    """
    Функция выводит информацию по наименованию и количеству заказа по номеру заказа
    :param order_id: номер заказа
    :return: список материалов и их количества
    """
    with conn.cursor() as cur:
        select_query = """SELECT category, quantity FROM order_information WHERE order_id = %s"""
        cur.execute(select_query, (order_id,))
        return cur.fetchall()


order = 'YW8-99'


# print(search_data(order))

def data_racks_shelf(rack: int, shelf: int):
    """
    Функция выводит информацию по наименованию и количеству заказа по номеру стеллажа и полки, на которой лежит материал
    :param rack: номер стеллажа
    :param shelf: номер полки
    :return: список материалов и их количества на конкретном стеллаже и полке
    """
    with conn.cursor() as cur:
        select_query = """SELECT category, quantity FROM order_information WHERE rack = %s AND shelf = %s"""
        cur.execute(select_query, (rack, shelf))
        return cur.fetchall()


data_rack = 4
data_shelf = 3


# print(data_racks_shelf(data_rack, data_shelf))


def data_racks(rack: int):
    """
    Функция выводит информацию по наименованию и количеству заказа по номеру стеллажа, на котором лежит материал
    :param rack: номер стеллажа
    :return: список материалов и их количества на конкретном стеллаже
    """
    with conn.cursor() as cur:
        select_query = """SELECT category, quantity FROM order_information WHERE rack = %s"""
        cur.execute(select_query, (rack,))
        return cur.fetchall()


data_rack = 4


# print(data_racks(data_rack))

#
def add_quantity(count: int, order_id: str, rack: int, shelf: int, category: str):
    """
    Функция добавления определённого количества материала в заказе
    :param count:
    :param order_id:
    :param rack:
    :param shelf:
    :param category:
    :return:
    """
    with conn.cursor() as cur:
        select_query = """UPDATE order_information SET quantity = quantity + %s WHERE order_id = %s AND rack = %s 
        AND shelf = %s AND category = %s"""
        cur.execute(select_query, (count, order_id, rack, shelf, category))
        return f'Количество материала "{category}" в заказе № {order_id}, хранящегося на {shelf} полке {rack} стеллажа, увеличино на {count}'


number_order = 'YW8-99'
order_category = 'гвозди'
order_rack = 1
order_shelf = 1
order_count = 333
# print(add_quantity(order_count, number_order, order_rack, order_shelf, order_category))
conn.commit()


def subtract_the_amount(count: int, order_id: str, rack: int, shelf: int, category: str):
    """
    Функция уменьшения определённого количества материала в заказе
    :param count:
    :param order_id:
    :param rack:
    :param shelf:
    :param category:
    :return:
    """
    with conn.cursor() as cur:
        select_query = """UPDATE order_information SET quantity = quantity - %s WHERE order_id = %s AND rack = %s 
        AND shelf = %s AND category = %s"""
        cur.execute(select_query, (count, order_id, rack, shelf, category))
        return f'Количество материала "{category}" в заказе № {order_id}, хранящегося на {shelf} полке {rack} стеллажа, уменьшено на {count}'


number_order = 'YW8-99'
order_category = 'гвозди'
order_rack = 1
order_shelf = 1
order_count = 333
# print(subtract_the_amount(order_count, number_order, order_rack, order_shelf, order_category))
conn.commit()

