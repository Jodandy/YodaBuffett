-- Manual SQL commands to fix the foreign key constraint issue
-- Run these commands in psql or pgAdmin

-- Connect to database first:
-- psql postgresql://yodabuffett:password@localhost:5432/yodabuffett

-- 1. Check current foreign key constraints
SELECT 
    tc.constraint_name,
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc 
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY' 
AND tc.table_name = 'daily_price_data';

-- 2. Drop foreign key constraints (replace CONSTRAINT_NAME with actual names from above)
-- Common constraint names that might exist:
ALTER TABLE daily_price_data DROP CONSTRAINT IF EXISTS daily_price_data_company_id_fkey;
ALTER TABLE daily_price_data DROP CONSTRAINT IF EXISTS fk_daily_price_data_company;
ALTER TABLE daily_price_data DROP CONSTRAINT IF EXISTS daily_price_company_fkey;

-- 3. Test insertion
INSERT INTO daily_price_data (
    symbol, date, open_price, high_price, low_price, close_price, provider
) VALUES ('TEST', '2024-01-01', 100.0, 101.0, 99.0, 100.5, 'test')
ON CONFLICT DO NOTHING;

-- 4. Clean up test data
DELETE FROM daily_price_data 
WHERE symbol = 'TEST' AND provider = 'test';

-- 5. Verify constraints are gone
SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'daily_price_data';