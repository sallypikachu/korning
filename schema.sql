-- DEFINE YOUR DATABASE SCHEMA HERE
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS frequencies;

CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(100)
);

CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  account VARCHAR(50)
);

CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100)
);

CREATE TABLE frequencies (
  id SERIAL PRIMARY KEY,
  frequency VARCHAR(50)
);

CREATE TABLE sales (
  id SERIAL PRIMARY KEY,
  sale_date DATE,
  sale_amount DECIMAL,
  units_sold INT,
  invoice_no INT,
  employee_id INT REFERENCES employees(id),
  customer_id INT REFERENCES customers(id),
  product_id INT REFERENCES products(id),
  frequency_id INT REFERENCES frequencies(id)
);
