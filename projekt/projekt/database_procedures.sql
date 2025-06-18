-- Tabela do logowania operacji
CREATE TABLE operation_logs (
    log_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name VARCHAR2(100) NOT NULL,
    operation_type VARCHAR2(20) NOT NULL,
    record_id NUMBER,
    operation_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    user_name VARCHAR2(100),
    additional_info VARCHAR2(4000)
);

-- Tabela do archiwizacji usuniętych danych
CREATE TABLE deleted_records_archive (
    archive_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name VARCHAR2(100) NOT NULL,
    record_id NUMBER,
    record_data CLOB,
    deletion_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    deleted_by VARCHAR2(100)
);

-- Funkcja sprawdzająca poprawność PESEL
CREATE OR REPLACE FUNCTION validate_pesel(p_pesel IN VARCHAR2) 
RETURN BOOLEAN IS
    v_weights CONSTANT NUMBER_ARRAY := NUMBER_ARRAY(1, 3, 7, 9, 1, 3, 7, 9, 1, 3);
    v_sum NUMBER := 0;
    v_control_digit NUMBER;
BEGIN
    -- Sprawdzenie długości
    IF LENGTH(p_pesel) != 11 THEN
        RETURN FALSE;
    END IF;
    
    -- Sprawdzenie czy wszystkie znaki są cyframi
    IF NOT REGEXP_LIKE(p_pesel, '^\d{11}$') THEN
        RETURN FALSE;
    END IF;
    
    -- Obliczenie sumy kontrolnej
    FOR i IN 1..10 LOOP
        v_sum := v_sum + TO_NUMBER(SUBSTR(p_pesel, i, 1)) * v_weights(i);
    END LOOP;
    
    v_control_digit := MOD(10 - MOD(v_sum, 10), 10);
    
    -- Sprawdzenie cyfry kontrolnej
    RETURN v_control_digit = TO_NUMBER(SUBSTR(p_pesel, 11, 1));
END validate_pesel;
/

-- Funkcja do generowania unikalnego kodu produktu
CREATE OR REPLACE FUNCTION generate_product_code(
    p_category_id IN NUMBER,
    p_supplier_id IN NUMBER
) RETURN VARCHAR2 IS
    v_category_code VARCHAR2(3);
    v_supplier_code VARCHAR2(3);
    v_sequence NUMBER;
BEGIN
    -- Pobierz kod kategorii
    SELECT SUBSTR(category_name, 1, 3) INTO v_category_code
    FROM product_categories
    WHERE category_id = p_category_id;
    
    -- Pobierz kod dostawcy
    SELECT SUBSTR(company_name, 1, 3) INTO v_supplier_code
    FROM suppliers
    WHERE supplier_id = p_supplier_id;
    
    -- Pobierz następną wartość sekwencji
    SELECT product_seq.NEXTVAL INTO v_sequence FROM dual;
    
    RETURN UPPER(v_category_code || v_supplier_code || LPAD(v_sequence, 4, '0'));
END generate_product_code;
/

-- Procedura dodawania nowego produktu
CREATE OR REPLACE PROCEDURE add_product(
    p_product_name IN VARCHAR2,
    p_category_id IN NUMBER,
    p_supplier_id IN NUMBER,
    p_unit_price IN NUMBER,
    p_units_in_stock IN NUMBER DEFAULT 0,
    p_reorder_level IN NUMBER DEFAULT 0,
    p_result OUT VARCHAR2
) IS
    v_product_id NUMBER;
    v_product_code VARCHAR2(10);
BEGIN
    -- Walidacja danych
    IF p_unit_price <= 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Cena jednostkowa musi być większa od 0');
    END IF;
    
    -- Generowanie kodu produktu
    v_product_code := generate_product_code(p_category_id, p_supplier_id);
    
    -- Wstawienie produktu
    INSERT INTO products (
        product_name, category_id, supplier_id, unit_price,
        units_in_stock, reorder_level
    ) VALUES (
        p_product_name, p_category_id, p_supplier_id, p_unit_price,
        p_units_in_stock, p_reorder_level
    ) RETURNING product_id INTO v_product_id;
    
    p_result := 'Produkt dodany pomyślnie. ID: ' || v_product_id || ', Kod: ' || v_product_code;
    
    -- Logowanie operacji
    INSERT INTO operation_logs (table_name, operation_type, record_id, user_name)
    VALUES ('products', 'INSERT', v_product_id, USER);
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        p_result := 'Błąd podczas dodawania produktu: ' || SQLERRM;
END add_product;
/

-- Procedura aktualizacji produktu
CREATE OR REPLACE PROCEDURE update_product(
    p_product_id IN NUMBER,
    p_product_name IN VARCHAR2 DEFAULT NULL,
    p_unit_price IN NUMBER DEFAULT NULL,
    p_units_in_stock IN NUMBER DEFAULT NULL,
    p_result OUT VARCHAR2
) IS
    v_old_data CLOB;
BEGIN
    -- Pobierz stare dane do archiwum
    SELECT JSON_OBJECT(
        'product_id' VALUE product_id,
        'product_name' VALUE product_name,
        'unit_price' VALUE unit_price,
        'units_in_stock' VALUE units_in_stock
    ) INTO v_old_data
    FROM products
    WHERE product_id = p_product_id;
    
    -- Aktualizacja produktu
    UPDATE products
    SET product_name = NVL(p_product_name, product_name),
        unit_price = NVL(p_unit_price, unit_price),
        units_in_stock = NVL(p_units_in_stock, units_in_stock),
        updated_at = SYSTIMESTAMP
    WHERE product_id = p_product_id;
    
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Produkt o podanym ID nie istnieje');
    END IF;
    
    p_result := 'Produkt zaktualizowany pomyślnie';
    
    -- Logowanie operacji
    INSERT INTO operation_logs (table_name, operation_type, record_id, user_name)
    VALUES ('products', 'UPDATE', p_product_id, USER);
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        p_result := 'Błąd podczas aktualizacji produktu: ' || SQLERRM;
END update_product;
/

-- Procedura usuwania produktu
CREATE OR REPLACE PROCEDURE delete_product(
    p_product_id IN NUMBER,
    p_result OUT VARCHAR2
) IS
    v_old_data CLOB;
BEGIN
    -- Pobierz dane do archiwum
    SELECT JSON_OBJECT(
        'product_id' VALUE product_id,
        'product_name' VALUE product_name,
        'unit_price' VALUE unit_price,
        'units_in_stock' VALUE units_in_stock
    ) INTO v_old_data
    FROM products
    WHERE product_id = p_product_id;
    
    -- Archiwizacja danych
    INSERT INTO deleted_records_archive (
        table_name, record_id, record_data, deleted_by
    ) VALUES (
        'products', p_product_id, v_old_data, USER
    );
    
    -- Usunięcie produktu
    DELETE FROM products WHERE product_id = p_product_id;
    
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'Produkt o podanym ID nie istnieje');
    END IF;
    
    p_result := 'Produkt usunięty pomyślnie';
    
    -- Logowanie operacji
    INSERT INTO operation_logs (table_name, operation_type, record_id, user_name)
    VALUES ('products', 'DELETE', p_product_id, USER);
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        p_result := 'Błąd podczas usuwania produktu: ' || SQLERRM;
END delete_product;
/

-- Funkcja okienkowa do analizy sprzedaży
CREATE OR REPLACE FUNCTION get_sales_analysis(
    p_start_date IN DATE,
    p_end_date IN DATE
) RETURN SYS_REFCURSOR IS
    v_result SYS_REFCURSOR;
BEGIN
    OPEN v_result FOR
        SELECT 
            p.product_name,
            c.category_name,
            SUM(od.quantity) as total_quantity,
            SUM(od.quantity * od.unit_price) as total_revenue,
            AVG(od.unit_price) as avg_price,
            RANK() OVER (ORDER BY SUM(od.quantity * od.unit_price) DESC) as revenue_rank,
            LAG(SUM(od.quantity * od.unit_price)) OVER (ORDER BY o.order_date) as prev_period_revenue,
            LEAD(SUM(od.quantity * od.unit_price)) OVER (ORDER BY o.order_date) as next_period_revenue
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN products p ON od.product_id = p.product_id
        JOIN product_categories c ON p.category_id = c.category_id
        WHERE o.order_date BETWEEN p_start_date AND p_end_date
        GROUP BY p.product_name, c.category_name, o.order_date
        ORDER BY total_revenue DESC;
    
    RETURN v_result;
END get_sales_analysis;
/

-- Wyzwalacz do automatycznej aktualizacji stanów magazynowych
CREATE OR REPLACE TRIGGER trg_update_inventory
AFTER INSERT OR UPDATE ON order_details
FOR EACH ROW
DECLARE
    v_warehouse_id NUMBER;
BEGIN
    -- Pobierz ID magazynu (przykładowo pierwszy magazyn)
    SELECT warehouse_id INTO v_warehouse_id
    FROM warehouses
    WHERE ROWNUM = 1;
    
    -- Aktualizuj stan magazynowy
    MERGE INTO inventory i
    USING (SELECT :NEW.product_id as product_id, :NEW.quantity as quantity FROM dual) s
    ON (i.warehouse_id = v_warehouse_id AND i.product_id = s.product_id)
    WHEN MATCHED THEN
        UPDATE SET 
            quantity = quantity - s.quantity,
            updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (warehouse_id, product_id, quantity)
        VALUES (v_warehouse_id, s.product_id, -s.quantity);
        
    -- Logowanie operacji
    INSERT INTO operation_logs (
        table_name, operation_type, record_id, user_name,
        additional_info
    ) VALUES (
        'inventory', 'UPDATE', :NEW.product_id, USER,
        'Automatyczna aktualizacja stanu magazynowego po zamówieniu'
    );
EXCEPTION
    WHEN OTHERS THEN
        -- Logowanie błędu
        INSERT INTO operation_logs (
            table_name, operation_type, user_name,
            additional_info
        ) VALUES (
            'inventory', 'ERROR', USER,
            'Błąd podczas aktualizacji stanu magazynowego: ' || SQLERRM
        );
        RAISE;
END trg_update_inventory;
/

-- Wyzwalacz do sprawdzania poziomów magazynowych
CREATE OR REPLACE TRIGGER trg_check_inventory_levels
AFTER UPDATE ON inventory
FOR EACH ROW
WHEN (NEW.quantity <= OLD.reorder_level)
BEGIN
    -- Logowanie informacji o niskim stanie magazynowym
    INSERT INTO operation_logs (
        table_name, operation_type, record_id, user_name,
        additional_info
    ) VALUES (
        'inventory', 'ALERT', :NEW.product_id, USER,
        'Stan magazynowy poniżej poziomu uzupełnienia: ' || :NEW.quantity
    );
END trg_check_inventory_levels;
/

-- Sekwencja dla produktów
CREATE SEQUENCE product_seq
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE; 