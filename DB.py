import psycopg2

with psycopg2.connect(user="postgres",
                      password="Hun$917&305TpS",  # host="127.0.0.1",
                      port="5432",
                      database="warehouse_db") as conn:
    def create_db():
        """
        Функция, создающая структуру БД (таблицы)
        :return: База данных создана
        """
        with conn.cursor() as cur:
            create_query = """ CREATE TABLE IF NOT EXISTS order_number(
                                order_id VARCHAR(20) PRIMARY KEY
                                );
                                CREATE TABLE IF NOT EXISTS order_information(
                                id SERIAL PRIMARY KEY,
                                rack INTEGER NOT NULL,                 
                                shelf INTEGER NOT NULL,
                                category VARCHAR(200) NOT NULL,
                                quantity INTEGER NOT NULL,
                                order_id VARCHAR(20) NOT NULL REFERENCES order_number(order_id)
                                ON DELETE CASCADE)"""
            cur.execute(create_query)
            return 'База данных создана'


    # print(create_db())
    conn.commit()


    def delete_db():
        """
        Функция, удаляющая таблицы базы данных
        :return: База данных удалена
        """
        with conn.cursor() as cur:
            delete_query = """DROP TABLE order_information;
                DROP TABLE order_number
                CASCADE"""
            cur.execute(delete_query)
            return 'База данных удалена'

    # print(delete_db())
    conn.commit()