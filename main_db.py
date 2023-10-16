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


order = 'YY-88'
# print(add_order(order))
conn.commit()


def data_order(rack: str, shelf: int, category: str, quantity: int, order_id: str):
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


data_rack = 'А-1'
data_shelf = 1
data_category = 'свечи автомобильные'
data_quantity = 430
order = 'YQ-1'
# print(data_order(data_rack, data_shelf, data_category, data_quantity, order))
conn.commit()


def data_without_order(rack: str, shelf: int, category: str, quantity: int):
    """
    Функция заполнения таблицы order_information без номера заказа
    :param rack: номер стеллажа
    :param shelf: номер полки
    :param category: наименование материала
    :param quantity: количество материала
    :return: Информация записана
    """
    with conn.cursor() as cur:
        insert_query = """INSERT INTO order_information(rack, shelf, category, quantity) VALUES (%s, %s, %s, %s)"""
        cur.execute(insert_query, (rack, shelf, category, quantity))
        return f'Информация записана'


data_rack = 'C-2'
data_shelf = 5
data_category = 'автошины'
data_quantity = 44

# print(data_without_order(data_rack, data_shelf, data_category, data_quantity))
conn.commit()


def search_id(rack: str, shelf: int,
              category: str):  # поиск id (функция ниже(data_order_number) - для записи номера заказа по найденному id)
    """
    Функция вывода id по заданным параметрам
    :param rack: стеллаж
    :param shelf: полка
    :param category: наименование материала
    :return: id
    """
    with conn.cursor() as cur:
        insert_query = """SELECT id FROM order_information WHERE rack = %s AND shelf = %s AND category = %s"""
        cur.execute(insert_query, (rack, shelf, category))
        return cur.fetchone()


data_rack = 'А-1'
data_shelf = 1
data_category = 'свечи автомобильные'
data_quantity = 430


# print(search_id(data_rack, data_shelf, data_category))


def data_order_number(order_id: str, id: int):
    """
    Функция записи номера заказа в таблицу order_information по id
    :param order_id: номер заказа
    :param id: id
    :return: Номер заказа добавлен
    """
    with conn.cursor() as cur:
        insert_query = """UPDATE order_information SET order_id = %s WHERE id = %s"""
        cur.execute(insert_query, (order_id, id))
        return 'Номер заказа добавлен'


data_id = 9
order = 'TT-77'
# print(data_order_number(order, data_id))
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


def delete_without_order_number(rack: str, shelf: int, category: str):
    """
    Функция удаления данных, по хранящимся материалам (без номера заказа)
    :return: Данные удалёны из БД
    """
    with conn.cursor() as cur:
        delete_query = """DELETE FROM order_information WHERE rack = %s
        AND shelf = %s AND category = %s"""
        cur.execute(delete_query, (rack, shelf, category))
        return f'Данные удалёны из БД'


data_rack = 'А-1'
data_shelf = 1
data_category = 'свечи автомобильные'
# print(delete_without_order_number(data_rack, data_shelf, data_category))
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


order = 'Yu-1'


# print(search_data(order))

def data_racks_shelf(rack: str, shelf: int):
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


def data_racks(rack: str):
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
def add_quantity(count: int, order_id: str, rack: str, shelf: int, category: str):
    """
    Функция добавления определённого количества материала в заказе
    :param count: количество материала, которое добавляется
    :param order_id: номер заказа
    :param rack: номер стеллажа
    :param shelf: номер полки
    :param category: наименование материала
    :return:
    """
    with conn.cursor() as cur:
        select_query = """UPDATE order_information SET quantity = quantity + %s WHERE order_id = %s AND rack = %s
        AND shelf = %s AND category = %s"""
        cur.execute(select_query, (count, order_id, rack, shelf, category))
        return f'Количество материала "{category}" в заказе № {order_id}, хранящегося на {shelf} полке {rack} стеллажа, увеличино на {count}'


number_order = 'Yu-1'
order_category = 'каска'
order_rack = 'E-2'
order_shelf = 3
order_count = 1333
# print(add_quantity(order_count, number_order, order_rack, order_shelf, order_category))
conn.commit()


def subtract_the_amount(count: int, order_id: str, rack: str, shelf: int, category: str):
    """
    Функция уменьшения определённого количества материала в заказе
    :param count: количество, на которое уменьшается материал
    :param order_id: номер заказа
    :param rack: номер стеллажа
    :param shelf: номер полки
    :param category: наименование материала
    :return: Количество материала "{category}" в заказе № {order_id}, хранящегося на {shelf} полке {rack} стеллажа, уменьшено на {count}
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


def data_category():
    """
    Функция выдаёт список материалов, хранящихся на складе
    :return: список материалов
    """
    with conn.cursor() as cur:
        select_query = """SELECT category FROM order_information"""
        cur.execute(select_query)
        list_1 = []
        for i in cur.fetchall():
            for item in i:
                list_1.append(item)
        return list_1


# print(data_category())

def data_from_shelf(rack: str, shelf: int):
    """
    Функция выдаёт список материалов, хранящихся на полке определённого стеллажа
    :param rack: стеллаж
    :param shelf: полка
    :return: список материалов
    """
    with conn.cursor() as cur:
        select_query = """SELECT category FROM order_information WHERE rack = %s AND shelf = %s"""
        cur.execute(select_query, (rack, shelf))
        list_1 = []
        for i in cur.fetchall():
            for item in i:
                list_1.append(item)
        return list_1


data_rack = 'E-2'
data_shelf = 2

print(data_from_shelf(data_rack, data_shelf))


def search_rack_shelf(category):
    """
    Функция поиска номера стеллажа и полки, на которой находится определённый материал
    :param category: наименование материала
    :return: номер стеллажа и полки
    """
    with conn.cursor() as cur:
        select_query = """SELECT rack, shelf FROM order_information WHERE category = %s"""
        cur.execute(select_query, (category,))
        return cur.fetchone()  # в результате выдаёт картеж
        # list_1 = []
        # for i in cur.fetchall():
        #     for item in i:
        #         list_1.append(item)
        # return list_1     # в результате выдаёт список


data_category = 'каска'


# print(search_rack_shelf(data_category))


def search_rack_shelf_2(order_id):
    """
    Функция поиска номера стеллажа и полки, на которой находится определённый материал
    :param order_id: наименование материала
    :return: номер стеллажа и полки
    """
    with conn.cursor() as cur:
        select_query = """SELECT rack, shelf FROM order_information WHERE order_id = %s"""
        cur.execute(select_query, (order_id,))
        return cur.fetchall()  # в результате выдаёт картеж


order = 'Yu-1'


# print(search_rack_shelf_2(order))


def list_rack():
    """
    Функция для получения списка стеллажей
    :return: список стеллажей
    """
    with conn.cursor() as cur:
        select_query = """SELECT rack FROM order_information"""
        cur.execute(select_query)
        list_1 = []
        for i in cur.fetchall():
            for item in i:
                list_1.append(item)
        return list_1


# print(list_rack())


def list_shelf(rack: str):
    """
    Функция показывает полки на определённом стеллаже
    :param rack: стеллаж
    :return: список полок
    """
    with conn.cursor() as cur:
        select_query = """SELECT shelf FROM order_information WHERE rack = %s"""
        cur.execute(select_query, (rack,))
        list_1 = []
        for i in cur.fetchall():
            for item in i:
                list_1.append(item)
        return list_1


data_rack = 'E-2'


# print(list_shelf(data_rack))

def subtract_the_amount_without_order_number(count: int, rack: str, shelf: int, category: str):
    """
    Функция уменьшения определённого количества материала
    :param count: количество, на которое уменьшается материал
    :param rack: номер стеллажа
    :param shelf: номер полки
    :param category: наименование материала
    :return: Количество материала "{category}" в заказе № {order_id}, хранящегося на {shelf} полке {rack} стеллажа, уменьшено на {count}
    """
    with conn.cursor() as cur:
        select_query = """UPDATE order_information SET quantity = quantity - %s WHERE rack = %s
        AND shelf = %s AND category = %s"""
        cur.execute(select_query, (count, rack, shelf, category))
        return f'Количество материала "{category}", хранящегося на {shelf} полке {rack} стеллажа, уменьшено на {count}'


data_rack = 'C-2'
data_shelf = 5
data_category = 'автошины'
order_count = 44
# print(subtract_the_amount_without_order_number(order_count, data_rack, data_shelf, data_category))
conn.commit()


def add_quantity_without_order_number(count: int, rack: str, shelf: int, category: str):
    """
    Функция добавления определённого количества материала в заказе
    :param count: количество материала, которое добавляется
    :param rack: номер стеллажа
    :param shelf: номер полки
    :param category: наименование материала
    :return:
    """
    with conn.cursor() as cur:
        select_query = """UPDATE order_information SET quantity = quantity + %s WHERE rack = %s
        AND shelf = %s AND category = %s"""
        cur.execute(select_query, (count, rack, shelf, category))
        return f'Количество материала "{category}", хранящегося на {shelf} полке {rack} стеллажа, увеличино на {count}'


data_rack = 'C-2'
data_shelf = 5
data_category = 'автошины'
order_count = 44
# print(add_quantity_without_order_number(order_count, data_rack, data_shelf, data_category))
conn.commit()

# def search_data(order_id):
#     with conn.cursor() as cur:
#         select_query = """SELECT category, quantity FROM order_information WHERE order_id = %s"""
#         cur.execute(select_query, (order_id,))
#         res = cur.fetchall()
#         for row in res:
#             print("category =", row[0], )
#             print("quantity =", row[1], "\n")
#
#
# order = 'YW8-99'
# print(search_data(order))


# def search_id_1(rack: str, shelf: int, category: str, order_id: str):
#     with conn.cursor() as cur:
#         insert_query = """SELECT id FROM order_information WHERE rack = %s AND shelf = %s AND category = %s"""
#         cur.execute(insert_query, (rack, shelf, category))
#         if cur.fetchone():
#             insert_query = """UPDATE order_information SET order_id = %s WHERE id = id"""
#             cur.execute(insert_query, (order_id,))
#             return 'ok'
#
#
# data_rack = 'А-1'
# data_shelf = 1
# data_category = 'свечи автомобильные'
# data_quantity = 430
# order = 'Yu-1'
# print(search_id_1(data_rack, data_shelf, data_category, order))
# conn.commit()
