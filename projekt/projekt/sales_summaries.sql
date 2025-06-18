-- Tabela do przechowywania podsumowań sprzedaży
CREATE TABLE sales_summaries (
    summary_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    period_type VARCHAR2(20) NOT NULL, -- 'DAILY', 'MONTHLY', 'QUARTERLY', 'YEARLY'
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    category_id NUMBER,
    product_id NUMBER,
    total_quantity NUMBER,
    total_revenue NUMBER(12,2),
    avg_order_value NUMBER(10,2),
    top_selling_product VARCHAR2(100),
    top_selling_category VARCHAR2(100),
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT fk_summary_category FOREIGN KEY (category_id) REFERENCES product_categories(category_id),
    CONSTRAINT fk_summary_product FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Indeksy dla optymalizacji zapytań
CREATE INDEX idx_sales_summaries_period ON sales_summaries(period_type, period_start, period_end);
CREATE INDEX idx_sales_summaries_category ON sales_summaries(category_id);
CREATE INDEX idx_sales_summaries_product ON sales_summaries(product_id);

-- Procedura generująca podsumowanie dzienne
CREATE OR REPLACE PROCEDURE generate_daily_summary(
    p_date IN DATE DEFAULT TRUNC(SYSDATE)
) IS
    v_start_date DATE := TRUNC(p_date);
    v_end_date DATE := TRUNC(p_date) + 1;
BEGIN
    -- Usuń istniejące podsumowanie dla tego dnia
    DELETE FROM sales_summaries 
    WHERE period_type = 'DAILY' 
    AND period_start = v_start_date;
    
    -- Wstaw nowe podsumowanie
    INSERT INTO sales_summaries (
        period_type, period_start, period_end,
        total_quantity, total_revenue, avg_order_value,
        top_selling_product, top_selling_category
    )
    WITH daily_stats AS (
        SELECT 
            SUM(od.quantity) as total_quantity,
            SUM(od.quantity * od.unit_price) as total_revenue,
            AVG(od.quantity * od.unit_price) as avg_order_value
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
    ),
    top_products AS (
        SELECT 
            p.product_name,
            SUM(od.quantity) as total_quantity
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN products p ON od.product_id = p.product_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
        GROUP BY p.product_name
        ORDER BY total_quantity DESC
        FETCH FIRST 1 ROW ONLY
    ),
    top_categories AS (
        SELECT 
            c.category_name,
            SUM(od.quantity) as total_quantity
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN products p ON od.product_id = p.product_id
        JOIN product_categories c ON p.category_id = c.category_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
        GROUP BY c.category_name
        ORDER BY total_quantity DESC
        FETCH FIRST 1 ROW ONLY
    )
    SELECT 
        'DAILY', v_start_date, v_end_date,
        ds.total_quantity, ds.total_revenue, ds.avg_order_value,
        tp.product_name, tc.category_name
    FROM daily_stats ds
    CROSS JOIN top_products tp
    CROSS JOIN top_categories tc;
    
    COMMIT;
END generate_daily_summary;
/

-- Procedura generująca podsumowanie miesięczne
CREATE OR REPLACE PROCEDURE generate_monthly_summary(
    p_year IN NUMBER DEFAULT EXTRACT(YEAR FROM SYSDATE),
    p_month IN NUMBER DEFAULT EXTRACT(MONTH FROM SYSDATE)
) IS
    v_start_date DATE := TO_DATE(p_year || '-' || LPAD(p_month, 2, '0') || '-01', 'YYYY-MM-DD');
    v_end_date DATE := ADD_MONTHS(v_start_date, 1);
BEGIN
    -- Usuń istniejące podsumowanie dla tego miesiąca
    DELETE FROM sales_summaries 
    WHERE period_type = 'MONTHLY' 
    AND period_start = v_start_date;
    
    -- Wstaw nowe podsumowanie
    INSERT INTO sales_summaries (
        period_type, period_start, period_end,
        total_quantity, total_revenue, avg_order_value,
        top_selling_product, top_selling_category
    )
    WITH monthly_stats AS (
        SELECT 
            SUM(od.quantity) as total_quantity,
            SUM(od.quantity * od.unit_price) as total_revenue,
            AVG(od.quantity * od.unit_price) as avg_order_value
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
    ),
    top_products AS (
        SELECT 
            p.product_name,
            SUM(od.quantity) as total_quantity
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN products p ON od.product_id = p.product_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
        GROUP BY p.product_name
        ORDER BY total_quantity DESC
        FETCH FIRST 1 ROW ONLY
    ),
    top_categories AS (
        SELECT 
            c.category_name,
            SUM(od.quantity) as total_quantity
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN products p ON od.product_id = p.product_id
        JOIN product_categories c ON p.category_id = c.category_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
        GROUP BY c.category_name
        ORDER BY total_quantity DESC
        FETCH FIRST 1 ROW ONLY
    )
    SELECT 
        'MONTHLY', v_start_date, v_end_date,
        ms.total_quantity, ms.total_revenue, ms.avg_order_value,
        tp.product_name, tc.category_name
    FROM monthly_stats ms
    CROSS JOIN top_products tp
    CROSS JOIN top_categories tc;
    
    COMMIT;
END generate_monthly_summary;
/

-- Procedura generująca podsumowanie kwartalne
CREATE OR REPLACE PROCEDURE generate_quarterly_summary(
    p_year IN NUMBER DEFAULT EXTRACT(YEAR FROM SYSDATE),
    p_quarter IN NUMBER DEFAULT CEIL(EXTRACT(MONTH FROM SYSDATE) / 3)
) IS
    v_start_date DATE := TO_DATE(p_year || '-' || LPAD((p_quarter-1)*3+1, 2, '0') || '-01', 'YYYY-MM-DD');
    v_end_date DATE := ADD_MONTHS(v_start_date, 3);
BEGIN
    -- Usuń istniejące podsumowanie dla tego kwartału
    DELETE FROM sales_summaries 
    WHERE period_type = 'QUARTERLY' 
    AND period_start = v_start_date;
    
    -- Wstaw nowe podsumowanie
    INSERT INTO sales_summaries (
        period_type, period_start, period_end,
        total_quantity, total_revenue, avg_order_value,
        top_selling_product, top_selling_category
    )
    WITH quarterly_stats AS (
        SELECT 
            SUM(od.quantity) as total_quantity,
            SUM(od.quantity * od.unit_price) as total_revenue,
            AVG(od.quantity * od.unit_price) as avg_order_value
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
    ),
    top_products AS (
        SELECT 
            p.product_name,
            SUM(od.quantity) as total_quantity
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN products p ON od.product_id = p.product_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
        GROUP BY p.product_name
        ORDER BY total_quantity DESC
        FETCH FIRST 1 ROW ONLY
    ),
    top_categories AS (
        SELECT 
            c.category_name,
            SUM(od.quantity) as total_quantity
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN products p ON od.product_id = p.product_id
        JOIN product_categories c ON p.category_id = c.category_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
        GROUP BY c.category_name
        ORDER BY total_quantity DESC
        FETCH FIRST 1 ROW ONLY
    )
    SELECT 
        'QUARTERLY', v_start_date, v_end_date,
        qs.total_quantity, qs.total_revenue, qs.avg_order_value,
        tp.product_name, tc.category_name
    FROM quarterly_stats qs
    CROSS JOIN top_products tp
    CROSS JOIN top_categories tc;
    
    COMMIT;
END generate_quarterly_summary;
/

-- Procedura generująca podsumowanie roczne
CREATE OR REPLACE PROCEDURE generate_yearly_summary(
    p_year IN NUMBER DEFAULT EXTRACT(YEAR FROM SYSDATE)
) IS
    v_start_date DATE := TO_DATE(p_year || '-01-01', 'YYYY-MM-DD');
    v_end_date DATE := ADD_MONTHS(v_start_date, 12);
BEGIN
    -- Usuń istniejące podsumowanie dla tego roku
    DELETE FROM sales_summaries 
    WHERE period_type = 'YEARLY' 
    AND period_start = v_start_date;
    
    -- Wstaw nowe podsumowanie
    INSERT INTO sales_summaries (
        period_type, period_start, period_end,
        total_quantity, total_revenue, avg_order_value,
        top_selling_product, top_selling_category
    )
    WITH yearly_stats AS (
        SELECT 
            SUM(od.quantity) as total_quantity,
            SUM(od.quantity * od.unit_price) as total_revenue,
            AVG(od.quantity * od.unit_price) as avg_order_value
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
    ),
    top_products AS (
        SELECT 
            p.product_name,
            SUM(od.quantity) as total_quantity
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN products p ON od.product_id = p.product_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
        GROUP BY p.product_name
        ORDER BY total_quantity DESC
        FETCH FIRST 1 ROW ONLY
    ),
    top_categories AS (
        SELECT 
            c.category_name,
            SUM(od.quantity) as total_quantity
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN products p ON od.product_id = p.product_id
        JOIN product_categories c ON p.category_id = c.category_id
        WHERE o.order_date >= v_start_date AND o.order_date < v_end_date
        GROUP BY c.category_name
        ORDER BY total_quantity DESC
        FETCH FIRST 1 ROW ONLY
    )
    SELECT 
        'YEARLY', v_start_date, v_end_date,
        ys.total_quantity, ys.total_revenue, ys.avg_order_value,
        tp.product_name, tc.category_name
    FROM yearly_stats ys
    CROSS JOIN top_products tp
    CROSS JOIN top_categories tc;
    
    COMMIT;
END generate_yearly_summary;
/

-- Wyzwalacz do automatycznego generowania podsumowań
CREATE OR REPLACE TRIGGER trg_generate_summaries
AFTER INSERT OR UPDATE ON orders
FOR EACH ROW
DECLARE
    v_order_date DATE := TRUNC(:NEW.order_date);
BEGIN
    -- Generuj podsumowanie dzienne
    generate_daily_summary(v_order_date);
    
    -- Generuj podsumowanie miesięczne
    generate_monthly_summary(
        EXTRACT(YEAR FROM v_order_date),
        EXTRACT(MONTH FROM v_order_date)
    );
    
    -- Generuj podsumowanie kwartalne
    generate_quarterly_summary(
        EXTRACT(YEAR FROM v_order_date),
        CEIL(EXTRACT(MONTH FROM v_order_date) / 3)
    );
    
    -- Generuj podsumowanie roczne
    generate_yearly_summary(EXTRACT(YEAR FROM v_order_date));
EXCEPTION
    WHEN OTHERS THEN
        -- Logowanie błędu
        INSERT INTO operation_logs (
            table_name, operation_type, user_name,
            additional_info
        ) VALUES (
            'sales_summaries', 'ERROR', USER,
            'Błąd podczas generowania podsumowań: ' || SQLERRM
        );
END trg_generate_summaries;
/

-- Funkcja do pobierania danych do wykresów
CREATE OR REPLACE FUNCTION get_sales_chart_data(
    p_period_type IN VARCHAR2,
    p_start_date IN DATE,
    p_end_date IN DATE,
    p_category_id IN NUMBER DEFAULT NULL
) RETURN SYS_REFCURSOR IS
    v_result SYS_REFCURSOR;
BEGIN
    OPEN v_result FOR
        SELECT 
            period_start,
            period_end,
            total_quantity,
            total_revenue,
            avg_order_value,
            top_selling_product,
            top_selling_category
        FROM sales_summaries
        WHERE period_type = p_period_type
        AND period_start >= p_start_date
        AND period_end <= p_end_date
        AND (p_category_id IS NULL OR category_id = p_category_id)
        ORDER BY period_start;
    
    RETURN v_result;
END get_sales_chart_data;
/

-- Procedura do generowania wszystkich podsumowań dla danego okresu
CREATE OR REPLACE PROCEDURE generate_all_summaries(
    p_start_date IN DATE,
    p_end_date IN DATE
) IS
    v_current_date DATE := p_start_date;
BEGIN
    WHILE v_current_date <= p_end_date LOOP
        -- Generuj podsumowania dla każdego dnia w okresie
        generate_daily_summary(v_current_date);
        v_current_date := v_current_date + 1;
    END LOOP;
    
    -- Generuj podsumowania miesięczne
    FOR month_rec IN (
        SELECT DISTINCT 
            EXTRACT(YEAR FROM period_start) as year,
            EXTRACT(MONTH FROM period_start) as month
        FROM sales_summaries
        WHERE period_type = 'DAILY'
        AND period_start >= p_start_date
        AND period_end <= p_end_date
    ) LOOP
        generate_monthly_summary(month_rec.year, month_rec.month);
    END LOOP;
    
    -- Generuj podsumowania kwartalne
    FOR quarter_rec IN (
        SELECT DISTINCT 
            EXTRACT(YEAR FROM period_start) as year,
            CEIL(EXTRACT(MONTH FROM period_start) / 3) as quarter
        FROM sales_summaries
        WHERE period_type = 'DAILY'
        AND period_start >= p_start_date
        AND period_end <= p_end_date
    ) LOOP
        generate_quarterly_summary(quarter_rec.year, quarter_rec.quarter);
    END LOOP;
    
    -- Generuj podsumowania roczne
    FOR year_rec IN (
        SELECT DISTINCT EXTRACT(YEAR FROM period_start) as year
        FROM sales_summaries
        WHERE period_type = 'DAILY'
        AND period_start >= p_start_date
        AND period_end <= p_end_date
    ) LOOP
        generate_yearly_summary(year_rec.year);
    END LOOP;
    
    COMMIT;
END generate_all_summaries;
/ 