-- Tworzenie tabeli kategorii produktów
CREATE TABLE product_categories (
    category_id NUMBER PRIMARY KEY,
    category_name VARCHAR2(100) NOT NULL,
    description VARCHAR2(500),
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Tworzenie tabeli dostawców
CREATE TABLE suppliers (
    supplier_id NUMBER PRIMARY KEY,
    company_name VARCHAR2(100) NOT NULL,
    contact_name VARCHAR2(100),
    phone VARCHAR2(20),
    email VARCHAR2(100),
    address VARCHAR2(200),
    city VARCHAR2(50),
    country VARCHAR2(50),
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Tworzenie tabeli produktów
CREATE TABLE products (
    product_id NUMBER PRIMARY KEY,
    product_name VARCHAR2(100) NOT NULL,
    category_id NUMBER,
    supplier_id NUMBER,
    unit_price NUMBER(10,2) NOT NULL,
    units_in_stock NUMBER DEFAULT 0,
    units_on_order NUMBER DEFAULT 0,
    reorder_level NUMBER DEFAULT 0,
    discontinued NUMBER(1) DEFAULT 0,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES product_categories(category_id),
    CONSTRAINT fk_product_supplier FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- Tworzenie tabeli klientów
CREATE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    company_name VARCHAR2(100) NOT NULL,
    contact_name VARCHAR2(100),
    phone VARCHAR2(20),
    email VARCHAR2(100),
    address VARCHAR2(200),
    city VARCHAR2(50),
    country VARCHAR2(50),
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Tworzenie tabeli pracowników
CREATE TABLE employees (
    employee_id NUMBER PRIMARY KEY,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    email VARCHAR2(100) UNIQUE,
    phone VARCHAR2(20),
    hire_date DATE NOT NULL,
    job_title VARCHAR2(100),
    salary NUMBER(10,2),
    manager_id NUMBER,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT fk_employee_manager FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);

-- Tworzenie tabeli zamówień
CREATE TABLE orders (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    employee_id NUMBER,
    order_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    required_date TIMESTAMP,
    shipped_date TIMESTAMP,
    ship_address VARCHAR2(200),
    ship_city VARCHAR2(50),
    ship_country VARCHAR2(50),
    status VARCHAR2(20) DEFAULT 'NEW',
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_order_employee FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

-- Tworzenie tabeli szczegółów zamówień
CREATE TABLE order_details (
    order_id NUMBER,
    product_id NUMBER,
    unit_price NUMBER(10,2) NOT NULL,
    quantity NUMBER NOT NULL,
    discount NUMBER(3,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_order_details PRIMARY KEY (order_id, product_id),
    CONSTRAINT fk_order_detail_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_order_detail_product FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Tworzenie tabeli magazynów
CREATE TABLE warehouses (
    warehouse_id NUMBER PRIMARY KEY,
    warehouse_name VARCHAR2(100) NOT NULL,
    address VARCHAR2(200),
    city VARCHAR2(50),
    country VARCHAR2(50),
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Tworzenie tabeli stanów magazynowych
CREATE TABLE inventory (
    warehouse_id NUMBER,
    product_id NUMBER,
    quantity NUMBER DEFAULT 0,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_inventory PRIMARY KEY (warehouse_id, product_id),
    CONSTRAINT fk_inventory_warehouse FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id),
    CONSTRAINT fk_inventory_product FOREIGN KEY (product_id) REFERENCES products(product_id)
); 