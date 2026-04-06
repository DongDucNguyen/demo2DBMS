import os
import time

FILENAME = 'heap_file_1M.csv'

# TÍNH TOÁN KÍCH THƯỚC CỐ ĐỊNH CHO MỖI DÒNG (RECORD)
# Cấu trúc: ID (7 ký tự) + Dấu phẩy (1 ký tự) + Name (14 ký tự) + Xuống dòng (\n - 1 byte)
# Tổng cộng = 23 bytes / 1 record
RECORD_SIZE = 23 

def create_1m_records_csv():
    print(f"1. Đang tạo file {FILENAME} (Fixed-length CSV)...")
    start_time = time.time()
    
    # Phải dùng chế độ 'wb' (ghi nhị phân) để ép ký tự xuống dòng đúng 1 byte '\n'
    # Nếu dùng 'w' trên Windows, nó sẽ tự sinh ra '\r\n' (2 bytes) làm sai kích thước.
    with open(FILENAME, 'wb') as f:
        for i in range(1, 1000001):
            # :<7 và :<14 là format ép độ dài cố định, thiếu thì tự bù khoảng trắng
            record = f"{i:<7},{f'User {i}':<14}\n".encode('utf-8') 
            f.write(record)
            
    print(f"   -> Hoàn tất trong {time.time() - start_time:.2f}s!")
    print(f"   -> Dung lượng file: {os.path.getsize(FILENAME)} bytes\n")


# ════════════════════════════════════════════════════════════════
#  CÁCH 1 — DỒN RECORD (Compact / Shift Forward)
#  Độ phức tạp: O(N − i)  →  chậm khi xóa ở đầu file
#  Cơ chế: đọc từng record từ vị trí (i+1) đến cuối,
#           ghi đè nó vào vị trí trước đó (i, i+1, …),
#           sau đó cắt ngắn file đi 1 record.
# ════════════════════════════════════════════════════════════════
def demo_method_1_shift(index_to_delete):
    print(f"1. Demo Cách 1 — Dồn (Shift): xóa index {index_to_delete} (ID = {index_to_delete + 1})")

    with open(FILENAME, 'rb+') as f:
        # ── Xác định kích thước file & tổng số record ──
        f.seek(0, os.SEEK_END)
        file_size    = f.tell()
        total_records = file_size // RECORD_SIZE

        if index_to_delete >= total_records:
            print("   -> Lỗi: index ngoài phạm vi!\n")
            return

        moves = 0
        # ── Duyệt từng record SAU vị trí xóa, dịch lùi 1 slot ──
        for j in range(index_to_delete + 1, total_records):
            # Đọc record tại vị trí j  (1 lần đọc đĩa)
            f.seek(j * RECORD_SIZE)
            record = f.read(RECORD_SIZE)

            # Ghi record đó vào vị trí j-1  (1 lần ghi đĩa)
            f.seek((j - 1) * RECORD_SIZE)
            f.write(record)
            moves += 1

        # ── Cắt ngắn file: loại bỏ slot cuối (bây giờ là bản sao thừa) ──
        f.truncate(file_size - RECORD_SIZE)

    print(f"   -> Đã dồn xong. Số lần đọc+ghi đĩa: {moves:,} lần")
    print(f"   -> Dung lượng file sau khi xóa: {os.path.getsize(FILENAME):,} bytes\n")
    return moves


# ════════════════════════════════════════════════════════════════
#  CÁCH 3 — FREE LIST / TOMBSTONE
#  Độ phức tạp: O(1)  →  nhanh nhất, file KHÔNG bị dịch chuyển
#  Cơ chế:
#    • Ghi đè byte đầu của record bằng ký tự '*' (flag DELETED)
#    • Lưu danh sách các slot trống vào file "free_list.txt"
#      (Trong DB thực, free list nằm ở header của file dữ liệu)
# ════════════════════════════════════════════════════════════════
FREE_LIST_FILE = 'free_list.txt'

def demo_method_3_freelist(index_to_delete):
    print(f"3. Demo Cách 3 — Tombstone + Free List: xóa index {index_to_delete} (ID = {index_to_delete + 1})")

    # ── Bước 1: Nhảy trực tiếp đến offset của record (O(1) seek) ──
    offset = index_to_delete * RECORD_SIZE
    print(f"   -> Tính offset = {index_to_delete} × {RECORD_SIZE} = {offset} bytes")

    with open(FILENAME, 'rb+') as f:
        f.seek(offset)
        original = f.read(RECORD_SIZE)
        print(f"   -> Record gốc : {original.decode('utf-8', errors='replace').strip()}")

        # ── Bước 2: Ghi flag '*' vào byte đầu tiên của record ──
        #    Phần còn lại của record vẫn còn nguyên trên đĩa
        #    (slot chưa bị thu hồi vật lý — chỉ bị đánh dấu)
        f.seek(offset)
        tombstone = b'*' + original[1:]   # chỉ thay byte đầu
        f.write(tombstone)
        print(f"   -> Đã ghi tombstone: {tombstone.decode('utf-8', errors='replace').strip()}")

    # ── Bước 3: Cập nhật Free List (trong DB thực đây là header block) ──
    free_slots = _load_free_list()
    free_slots.insert(0, index_to_delete)   # thêm vào đầu danh sách (LIFO)
    _save_free_list(free_slots)
    chain = ' -> '.join(str(s) for s in free_slots) + ' -> NULL'
    print(f"   -> Free List (HEAD): {chain}")
    print(f"   -> Dung lượng file KHÔNG THAY ĐỔI: {os.path.getsize(FILENAME):,} bytes")
    print(f"   -> Slot {index_to_delete} sẽ được tái sử dụng khi INSERT tiếp theo\n")


def _load_free_list():
    """Đọc free list từ file lưu trữ (mô phỏng header block của DB)."""
    if not os.path.exists(FREE_LIST_FILE):
        return []
    with open(FREE_LIST_FILE, 'r') as f:
        content = f.read().strip()
        return [int(x) for x in content.split(',') if x] if content else []


def _save_free_list(slots):
    """Ghi free list ra file (mô phỏng ghi header block)."""
    with open(FREE_LIST_FILE, 'w') as f:
        f.write(','.join(str(s) for s in slots))


def demo_method_2_swap(index_to_delete):
    print(f"2. Demo Cách 2 — Thay Record Cuối: xóa index {index_to_delete} (ID = {index_to_delete + 1})")

    with open(FILENAME, 'rb+') as f:
        # Tính tổng số record hiện có
        f.seek(0, os.SEEK_END)
        file_size     = f.tell()
        total_records = file_size // RECORD_SIZE

        # BƯỚC 1: Đọc record cuối (O(1) seek)
        f.seek((total_records - 1) * RECORD_SIZE)
        last_record = f.read(RECORD_SIZE)
        print(f"   -> Record cuối (sẽ thay vào): {last_record.decode('utf-8', errors='replace').strip()}")

        # BƯỚC 2: Nhảy tới vị trí cần xóa và ghi đè (O(1) seek + 1 write)
        f.seek(index_to_delete * RECORD_SIZE)
        f.write(last_record)

        # BƯỚC 3: Cắt bỏ slot cuối (file shrinks by 1 record)
        f.truncate(file_size - RECORD_SIZE)

    print(f"   -> Dung lượng file sau khi xóa: {os.path.getsize(FILENAME):,} bytes (giảm đúng {RECORD_SIZE} bytes)\n")


def compare():
    print("=" * 60)
    print("  DEMO: Các chiến lược Xóa Record trong Fixed-Length File")
    print(f"  File: {FILENAME}  |  RECORD_SIZE = {RECORD_SIZE} bytes")
    print(f"  Xóa record tại index i = {DELETE_INDEX}")
    print("=" * 60 + "\n")

    # ── Tạo file dữ liệu gốc ──────────────────────────────────
    create_1m_records_csv()
    # Xóa free list cũ nếu có
    if os.path.exists(FREE_LIST_FILE):
        os.remove(FREE_LIST_FILE)

    # ── CÁCH 1: DỒN ──────────────────────────────────────────
    # Cần tạo bản sao để 3 demo cùng bắt đầu từ dữ liệu giống nhau
    import shutil
    shutil.copy(FILENAME, FILENAME + '.bak')

    t1_start = time.perf_counter()
    moves = demo_method_1_shift(DELETE_INDEX)
    t1_ms = (time.perf_counter() - t1_start) * 1000
    print(f"   ⏱  Thời gian Cách 1 (Dồn):        {t1_ms:>10.4f} ms  |  {moves:,} lần ghi đĩa\n")

    # ── Khôi phục file gốc cho Cách 2 ────────────────────────
    shutil.copy(FILENAME + '.bak', FILENAME)

    t2_start = time.perf_counter()
    demo_method_2_swap(DELETE_INDEX)
    t2_ms = (time.perf_counter() - t2_start) * 1000
    print(f"   ⏱  Thời gian Cách 2 (Thay cuối):  {t2_ms:>10.4f} ms  |  1 lần ghi đĩa\n")

    # ── Khôi phục file gốc cho Cách 3 ────────────────────────
    shutil.copy(FILENAME + '.bak', FILENAME)
    if os.path.exists(FREE_LIST_FILE):
        os.remove(FREE_LIST_FILE)

    t3_start = time.perf_counter()
    demo_method_3_freelist(DELETE_INDEX)
    t3_ms = (time.perf_counter() - t3_start) * 1000
    print(f"   ⏱  Thời gian Cách 3 (Tombstone):  {t3_ms:>10.4f} ms  |  1 lần ghi đĩa\n")

    # ── Dọn dẹp file backup ───────────────────────────────────
    os.remove(FILENAME + '.bak')

    # ── Bảng kết quả ─────────────────────────────────────────
    print("=" * 60)
    print("  KẾT QUẢ BENCHMARK")
    print("=" * 60)
    print(f"  {'Phương pháp':<28} {'Thời gian (ms)':>14}  {'Ghi đĩa':>12}")
    print(f"  {'-'*55}")
    print(f"  {'① Dồn (Shift)':<28} {t1_ms:>14.4f}  {moves:>10,} lần")
    print(f"  {'② Thay Record Cuối (Replace)':<28} {t2_ms:>14.4f}  {'1':>10} lần")
    print(f"  {'③ Tombstone + Free List':<28} {t3_ms:>14.4f}  {'1':>10} lần")
    print(f"  {'-'*55}")
    if t2_ms > 0:
        print(f"  Cách 1 chậm hơn Cách 2 khoảng: {t1_ms/t2_ms:.1f}×")
    if t3_ms > 0:
        print(f"  Cách 1 chậm hơn Cách 3 khoảng: {t1_ms/t3_ms:.1f}×")
    print("=" * 60)

if __name__ == '__main__':
    DELETE_INDEX = 3 
    compare()