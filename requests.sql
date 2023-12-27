SELECT c.name, SUM(o.total_amount) as total
FROM customers c
JOIN orders o ON o.customer_id = c.id
WHERE o.date >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY c.name
ORDER BY total DESC;


SELECT p.name, p.price, AVG(p.price) OVER (PARTITION BY p.category_id) as avg_price
FROM products p
WHERE p.price > AVG(p.price) OVER (PARTITION BY p.category_id)
ORDER BY p.price ASC;



SELECT o.id, o.date
FROM orders o
JOIN order_items oi ON oi.order_id = o.id
JOIN products p ON p.id = oi.product_id
WHERE p.category = 'Электроника'
ORDER BY o.date;



SELECT c.name
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id AND o.date >= DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 year')
WHERE o.id IS NULL
ORDER BY c.name;


SELECT p.name
FROM products p
LEFT JOIN order_items oi ON oi.product_id = p.id
WHERE oi.id IS NULL
ORDER BY p.name;

