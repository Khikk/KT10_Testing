import psycopg2
import timeit

# Первый запрос
def get_customer_order_totals():
    conn = psycopg2.connect(database="your_database_name", user="your_username", password="your_password", host="your_host", port="your_port")
    cur = conn.cursor()
    cur.execute("""
        SELECT c.name, SUM(o.total_amount) as total
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        WHERE o.date >= DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY c.name
        ORDER BY total DESC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

print("Время выполнения первого запроса: ", timeit.timeit(get_customer_order_totals, number=100))

# Второй запрос
def get_products_above_category_avg():
    conn = psycopg2.connect(database="your_database_name", user="your_username", password="your_password", host="your_host", port="your_port")
    cur = conn.cursor()
    cur.execute("""
        SELECT p.name, p.price, AVG(p.price) OVER (PARTITION BY p.category_id) as avg_price
        FROM products p
        WHERE p.price > AVG(p.price) OVER (PARTITION BY p.category_id)
        ORDER BY p.price ASC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

print("Время выполнения второго запроса: ", timeit.timeit(get_products_above_category_avg, number=100))

# Третий запрос
def get_orders_by_electronics_category():
    conn = psycopg2.connect(database="your_database_name", user="your_username", password="your_password", host="your_host", port="your_port")
    cur = conn.cursor()
    cur.execute("""
        SELECT o.id, o.date
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p ON p.id = oi.product_id
        WHERE p.category = 'Электроника'
        ORDER BY o.date;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

print("Время выполнения третьего запроса: ", timeit.timeit(get_orders_by_electronics_category, number=100))

# Четвертый запрос
def get_customers_without_orders():
    conn = psycopg2.connect(database="your_database_name", user="your_username", password="your_password", host="your_host", port="your_port")
    cur = conn.cursor()
    cur.execute("""
        SELECT c.name
        FROM customers c
        LEFT JOIN orders o ON o.customer_id = c.id AND o.date >= DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 year')
        WHERE o.id IS NULL
        ORDER BY c.name;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

print("Время выполнения четвертого запроса: ", timeit.timeit(get_customers_without_orders, number=100))

# Пятый запрос
def get_products_without_orders():
    conn = psycopg2.connect(database="your_database_name", user="your_username", password="your_password", host="your_host", port="your_port")
    cur = conn.cursor()
    cur.execute("""
        SELECT p.name
        FROM products p
        LEFT JOIN order_items oi ON oi.product_id = p.id
        WHERE oi.id IS NULL
        ORDER BY p.name;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

print("Время выполнения пятого запроса: ", timeit.timeit(get_products_without_orders, number=100))
