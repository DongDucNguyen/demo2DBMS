-- Tạo bảng
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name CHAR(12),
    status CHAR(10)
);

-- Chèn 1 triệu dòng (mất khoảng 1-2 giây)
INSERT INTO users (name, status)
SELECT 'User ' || i, 'ACTIVE'
FROM generate_series(1, 1000000) s(i);