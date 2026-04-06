-- Đo thời gian cho cả cụm thao tác
BEGIN;
EXPLAIN ANALYZE UPDATE users SET name = (SELECT name FROM users ORDER BY id DESC LIMIT 1) WHERE id = 3;
EXPLAIN ANALYZE DELETE FROM users WHERE id = (SELECT id FROM users ORDER BY id DESC LIMIT 1);
COMMIT;

-- Kết quả: Mỗi lệnh chỉ tốn khoảng 0.05ms - 0.1ms. Tổng cộng < 1ms.