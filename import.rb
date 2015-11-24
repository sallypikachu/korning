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

def insert_person(arr_of_arr, insertion)
  arr_of_arr.each do |arr|
    db_connection do |conn|
      conn.exec(insertion, [arr[0][0..-2], arr[1][0..-2]])
    end
  end
end

def insert_one_variable(arr_of_arr, insertion)
  arr_of_arr.each do |arr|
    db_connection do |conn|
      conn.exec(insertion, [arr])
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

insert_person(parsed_unique_employees, "INSERT INTO employees (name, email) VALUES ($1, $2);")
insert_person(parsed_unique_customers, "INSERT INTO customers (name, account) VALUES ($1, $2);")
insert_one_variable(unique_products, "INSERT INTO products (name) VALUES ($1);")
insert_one_variable(unique_invoice_frequencies, "INSERT INTO frequencies (frequency) VALUES ($1);")

def insert_sales(arr_of_hash)
  arr_of_hash.each do |hash|
    db_connection do|conn|
      employee = conn.exec("SELECT id FROM employees WHERE name = '#{hash[:employee].split(" (").first}';")
      customer = conn.exec("SELECT id FROM customers WHERE name = '#{hash[:customer_and_account_no].split(" (").first}';")
      product = conn.exec("SELECT id FROM products WHERE name = '#{hash[:product_name]}';")
      frequent = conn.exec("SELECT id FROM frequencies WHERE frequency = '#{hash[:invoice_frequency]}';")

      conn.exec("INSERT INTO sales (sale_date, sale_amount, units_sold, invoice_no, employee_id, customer_id, product_id, frequency_id) VALUES ('#{hash[:sale_date]}', #{hash[:sale_amount][1..-1]}, #{hash[:units_sold]}, #{hash[:invoice_no]}, #{employee[0]["id"]}, #{customer[0]["id"]}, #{product[0]["id"]}, #{frequent[0]["id"]});")
    end
  end
end

insert_sales(arr_of_hash)
