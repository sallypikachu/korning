# Use this file to import the sales information into the
# the database.

require "pg"
require "csv"
require "pry"

system 'psql korning < schema.sql'

def db_connection
  begin
    connection = PG.connect(dbname: "korning")
    yield(connection)
  ensure
    connection.close
  end
end

def csv_to_arr_of_hash(file)
  arr_of_hash = []
  CSV.foreach("#{file}", headers: true, header_converters: :symbol) do |row|
    array = row.to_hash
    arr_of_hash << array
  end
  arr_of_hash
end

def insert_employees(arr_of_arr)
  arr_of_arr.each do |arr|
    db_connection do |conn|
      conn.exec("INSERT INTO employees (name, email) VALUES ('#{arr[0][0..-2]}', '#{arr[1][0..-2]}');")
    end
  end
end

def insert_customers(arr_of_arr)
  arr_of_arr.each do |arr|
    db_connection do |conn|
      conn.exec("INSERT INTO customers (name, account) VALUES ('#{arr[0][0..-2]}', '#{arr[1][0..-2]}');")
    end
  end
end

def insert_products(arr_of_arr)
  arr_of_arr.each do |arr|
    db_connection do |conn|
      conn.exec("INSERT INTO products (name) VALUES ('#{arr}');")
    end
  end
end

def insert_frequencies(arr_of_arr)
  arr_of_arr.each do |arr|
    db_connection do |conn|
      conn.exec("INSERT INTO frequencies (frequency) VALUES ('#{arr}');")
    end
  end
end

def unique(arr_of_hash, data)
  unique = []
  arr_of_hash.each do |hash|
    if !unique.include?(hash[data])
      unique << hash[data]
    end
  end
  unique
end

arr_of_hash = csv_to_arr_of_hash('sales.csv')

unique_employees = unique(arr_of_hash, :employee)
unique_customers = unique(arr_of_hash, :customer_and_account_no)
unique_products = unique(arr_of_hash, :product_name)
unique_invoice_frequencies = unique(arr_of_hash, :invoice_frequency)

parsed_unique_employees = unique_employees.map {|x| x.split("(")}
parsed_unique_customers = unique_customers.map {|x| x.split("(")}

insert_employees(parsed_unique_employees)
insert_customers(parsed_unique_customers)
insert_products(unique_products)
insert_frequencies(unique_invoice_frequencies)

def insert_sales(arr_of_hash)
  arr_of_hash.each do |hash|
    employee_name = hash[:employee].split(" (").first
    company = hash[:customer_and_account_no].split(" (").first
    product = hash[:product_name]
    freq = hash[:invoice_frequency]

    employee = db_connection {|conn|
      conn.exec("SELECT id FROM employees WHERE name = '#{employee_name}'")}
    customer = db_connection {|conn|
      conn.exec("SELECT id FROM customers WHERE name = '#{company}'")}
    product = db_connection {|conn|
      conn.exec("SELECT id FROM products WHERE name = '#{product}'")}
    frequent = db_connection {|conn|
      conn.exec("SELECT id FROM frequencies WHERE frequency = '#{freq}'")}

    db_connection do |conn|
      conn.exec("INSERT INTO sales (sale_date, sale_amount, units_sold, invoice_no, employee_id, customer_id, product_id, frequency_id) VALUES ('#{hash[:sale_date]}', #{hash[:sale_amount][1..-1]}, #{hash[:units_sold]}, #{hash[:invoice_no]}, #{employee[0]["id"]}, #{customer[0]["id"]}, #{product[0]["id"]}, #{frequent[0]["id"]});")
    end
  end
end

insert_sales(arr_of_hash)
