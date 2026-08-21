-- Chạy tự động 1 lần khi container Postgres khởi tạo volume trống.

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    price       NUMERIC(12,2) NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- FULL: ghi đủ ảnh "before" của mọi cột vào WAL, để event UPDATE/DELETE
-- có đầy đủ giá trị (không bị null ở cột non-key). Đánh đổi bằng WAL nặng hơn.
-- Bảng lớn nên cân nhắc giữ mặc định (chỉ ghi cột thuộc primary key).
ALTER TABLE products REPLICA IDENTITY FULL;

-- Vài dòng seed để thấy snapshot ban đầu chảy vào cache khi consumer chạy.
INSERT INTO products (name, price) VALUES
    ('But bi Thien Long', 5000),
    ('Vo Campus 200 trang', 12000);
