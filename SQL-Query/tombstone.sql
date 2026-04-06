EXPLAIN ANALYZE DELETE FROM users WHERE id = 10;
CREATE EXTENSION IF NOT EXISTS pageinspect;
-- Kết quả: Cực nhanh, thường < 0.05ms.
SELECT * FROM heap_page_items(get_raw_page('users', 0))