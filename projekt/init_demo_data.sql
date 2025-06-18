-- Dodanie przykładowej kategorii, jeśli nie istnieje
MERGE INTO product_categories c
USING (SELECT 1 AS category_id, 'Demo Kategoria' AS category_name FROM dual) src
ON (c.category_id = src.category_id)
WHEN NOT MATCHED THEN
  INSERT (category_id, category_name) VALUES (src.category_id, src.category_name);

-- Dodanie przykładowego dostawcy, jeśli nie istnieje
MERGE INTO suppliers s
USING (SELECT 2 AS supplier_id, 'Demo Dostawca' AS company_name FROM dual) src
ON (s.supplier_id = src.supplier_id)
WHEN NOT MATCHED THEN
  INSERT (supplier_id, company_name) VALUES (src.supplier_id, src.company_name);

-- Utworzenie sekwencji dla produktów, jeśli nie istnieje
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM user_sequences WHERE sequence_name = 'PRODUCT_SEQ';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE product_seq START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;
END;
/

-- Utworzenie triggera do automatycznego nadawania product_id, jeśli nie istnieje
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM user_triggers WHERE trigger_name = 'BI_PRODUCTS';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE q'{
            CREATE OR REPLACE TRIGGER bi_products
            BEFORE INSERT ON products
            FOR EACH ROW
            WHEN (NEW.product_id IS NULL)
            BEGIN
                SELECT product_seq.NEXTVAL INTO :NEW.product_id FROM dual;
            END;
        }';
    END IF;
END;
/

-- Możesz uruchomić ten plik w SQL Developer lub innym narzędziu do obsługi Oracle:
-- @init_demo_data.sql 