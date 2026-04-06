import os
import time
import random
import shutil

# ════════════════════════════════════════════════════════════════
#  CẤU TRÚC FIXED-LENGTH RECORDS (lưu file text, dùng đọc ghi)
#
#  Student(student_id, full_name, class_name, email, phone)
#    student_id :  8 bytes  (S0000001 … S1000000)
#    full_name  : 30 bytes
#    class_name : 10 bytes  (e.g. CNTT01-K22)
#    email      : 30 bytes
#    phone      : 12 bytes
#    newline    :  1 byte
#    TỔNG       : 91 bytes / record
#
#  Course(course_id, course_name, credits, dept_name)
#    course_id  :  8 bytes  (CS000001 … CS500000)  ← 6 chữ số
#    course_name: 35 bytes
#    credits    :  2 bytes  (01-10)
#    dept_name  : 25 bytes
#    newline    :  1 byte
#    TỔNG       : 71 bytes / record
#
#  Enrollment(student_id, course_id, semester, score)
#    student_id :  8 bytes
#    course_id  :  8 bytes  (theo khóa ngoại từ Course)
#    semester   :  8 bytes  (e.g. 2024-1  / 2024-2  )
#    score      :  6 bytes  (00.000 … 10.000)
#    newline    :  1 byte
#    TỔNG       : 31 bytes / record   ← bảng chính để demo xóa
# ════════════════════════════════════════════════════════════════

# ── File names ──────────────────────────────────────────────────
STUDENT_FILE    = 'students.txt'
COURSE_FILE     = 'courses.txt'
ENROLLMENT_FILE = 'enrollments.txt'
FREE_LIST_FILE  = 'free_list.txt'

# ── Record sizes (bytes) ─────────────────────────────────────────
STUDENT_SIZE    = 91   # 8+30+10+30+12+1 = 91
COURSE_SIZE     = 71   # 8+35+2+25+1 = 71  (course_id tăng lên 8B)
RECORD_SIZE     = 31   # Enrollment 8+8+8+6+1 = 31  (course_id 8B)

# ── Tham số sinh dữ liệu ─────────────────────────────────────────
NUM_STUDENTS    = 500_000  
NUM_COURSES     = 500_000      
NUM_ENROLLMENTS = 5_000_000 

# ── Dữ liệu mẫu để sinh ngẫu nhiên ──────────────────────────────
_FIRST = ['Nguyen','Tran','Le','Pham','Hoang','Bui','Ngo','Do','Duong','Ly',
          'Dinh','Dang','Vu','Truong','Phan','Vo','Dao','Huynh','Cao','Ha']
_MID   = ['Thi','Van','Duc','Anh','Tuan','Minh','Hoang','Thu','Ngoc','Phuoc']
_LAST  = ['An','Binh','Chau','Dat','Em','Ga','Ha','Hung','Kien','Lan',
          'Mai','Nam','Oanh','Phu','Quynh','Rong','Son','Thinh','Uyen','Viet']
_DEPT  = ['CNTT','DTVT','QTKD','KT','XD','MT','HH','SV','VL','TH']
_COURSE_PREFIXES = ['Lap trinh','Co so du lieu','Giai tich','Dai so','Mang may tinh',
                    'Tri tue nhan tao','Ky thuat phan mem','He dieu hanh','Bao mat',
                    'Phan tich thiet ke']
_SEMESTERS = ['2022-1  ','2022-2  ','2023-1  ','2023-2  ','2024-1  ','2024-2  ']


# ════════════════════════════════════════════════════════════════
#  1. TẠO FILE SINH VIÊN  (100 K records, 91 bytes/record)
# ════════════════════════════════════════════════════════════════
def create_students():
    print(f"[1/3] Đang tạo {STUDENT_FILE} ({NUM_STUDENTS:,} sinh viên)...")
    t = time.time()
    rng = random.Random(42)
    with open(STUDENT_FILE, 'wb') as f:
        for i in range(1, NUM_STUDENTS + 1):
            sid   = f"S{i:07d}"                        # 8 bytes
            fname = f"{rng.choice(_FIRST)} {rng.choice(_MID)} {rng.choice(_LAST)}"
            fname = fname[:30]
            dept_idx = rng.randint(0, len(_DEPT) - 1)
            cls   = f"{_DEPT[dept_idx]}{rng.randint(1,9):02d}-K{rng.randint(20,24)}"  # e.g. CNTT03-K22
            email = f"sv{i:07d}@edu.vn"
            phone = f"09{rng.randint(0,9)}{rng.randint(1000000,9999999)}"

            record = (
                f"{sid:<8}"   # student_id  8B
                f"{fname:<30}"# full_name  30B
                f"{cls:<10}"  # class_name 10B
                f"{email:<30}"# email      30B
                f"{phone:<12}"# phone      12B
                "\n"          #             1B  →  91B total
            ).encode('utf-8')

            assert len(record) == STUDENT_SIZE, f"student record size={len(record)}"
            f.write(record)

    print(f"   -> Xong trong {time.time()-t:.2f}s | "
          f"Kích thước: {os.path.getsize(STUDENT_FILE):,} bytes\n")


# ════════════════════════════════════════════════════════════════
#  2. TẠO FILE MÔN HỌC  (500 records, 71 bytes/record)
# ════════════════════════════════════════════════════════════════
def create_courses():
    print(f"[2/3] Đang tạo {COURSE_FILE} ({NUM_COURSES} môn học)...")
    t = time.time()
    rng = random.Random(7)
    with open(COURSE_FILE, 'wb') as f:
        for i in range(1, NUM_COURSES + 1):
            cid   = f"CS{i:06d}"                       # 8 bytes (CS000001…CS500000)
            cname = f"{rng.choice(_COURSE_PREFIXES)} {rng.randint(1,9)}"
            cname = cname[:35]
            credits = rng.randint(2, 5)
            dept    = rng.choice(_DEPT)

            record = (
                f"{cid:<8}"    # course_id   8B
                f"{cname:<35}" # course_name 35B
                f"{credits:02d}"# credits     2B
                f"{dept:<25}"  # dept_name  25B
                "\n"           #             1B  →  71B total
            ).encode('utf-8')

            assert len(record) == COURSE_SIZE, f"course record size={len(record)}"
            f.write(record)

    print(f"   -> Xong trong {time.time()-t:.2f}s | "
          f"Kích thước: {os.path.getsize(COURSE_FILE):,} bytes\n")


# ════════════════════════════════════════════════════════════════
#  3. TẠO FILE ĐĂNG KÝ  (1 triệu records, 30 bytes/record)
#     Đây là bảng chính dùng để DEMO 3 CHIẾN LƯỢC XÓA
# ════════════════════════════════════════════════════════════════
def create_enrollments():
    print(f"[3/3] Đang tạo {ENROLLMENT_FILE} ({NUM_ENROLLMENTS:,} bản ghi đăng ký)...")
    t = time.time()
    rng = random.Random(99)
    with open(ENROLLMENT_FILE, 'wb') as f:
        for _ in range(NUM_ENROLLMENTS):
            sid  = f"S{rng.randint(1, NUM_STUDENTS):07d}"    # 8 bytes
            cid  = f"CS{rng.randint(1, NUM_COURSES):06d}"    # 8 bytes (CS000001…)
            sem  = rng.choice(_SEMESTERS)                    # 8 bytes (đã pad)
            score = rng.uniform(0, 10)

            record = (
                f"{sid:<8}"         # student_id  8B
                f"{cid:<8}"         # course_id   8B
                f"{sem:<8}"         # semester    8B
                f"{score:06.3f}"    # score       6B  (e.g. 08.750)
                "\n"                #             1B  →  31B total
            ).encode('utf-8')

            assert len(record) == RECORD_SIZE, f"enrollment record size={len(record)}"
            f.write(record)

    print(f"   -> Xong trong {time.time()-t:.2f}s | "
          f"Kích thước: {os.path.getsize(ENROLLMENT_FILE):,} bytes "
          f"(≈ {os.path.getsize(ENROLLMENT_FILE)/1024/1024:.1f} MB)\n")


def create_all_files():
    """Tạo 3 file dữ liệu nếu chưa tồn tại."""
    need = False
    for fn in [STUDENT_FILE, COURSE_FILE, ENROLLMENT_FILE]:
        if not os.path.exists(fn):
            need = True
            break
    if not need:
        print(">> Tất cả file dữ liệu đã tồn tại, bỏ qua bước tạo.\n")
        return
    create_students()
    create_courses()
    create_enrollments()


# ════════════════════════════════════════════════════════════════
#  ĐỌC 1 BẢN GHI ENROLLMENT TẠI INDEX i  (minh họa O(1) access)
# ════════════════════════════════════════════════════════════════
def read_enrollment(index):
    """Đọc bản ghi Enrollment tại chỉ số index (0-based) bằng direct seek."""
    offset = index * RECORD_SIZE
    with open(ENROLLMENT_FILE, 'rb') as f:
        f.seek(offset)
        raw = f.read(RECORD_SIZE)
    sid   = raw[0:8].decode().strip()    # [0: 8]  student_id  8B
    cid   = raw[8:16].decode().strip()    # [8:16]  course_id   8B
    sem   = raw[16:24].decode().strip()   # [16:24] semester    8B
    score = raw[24:30].decode().strip()   # [24:30] score       6B
    return {'student_id': sid, 'course_id': cid, 'semester': sem, 'score': score}


# ════════════════════════════════════════════════════════════════
#  CÁCH 1 — DỒN RECORD (Compact / Shift Forward)   O(N − i)
# ════════════════════════════════════════════════════════════════
def demo_method_1_shift(index_to_delete):
    print(f"1. Demo Cách 1 — Dồn (Shift): xóa index {index_to_delete}")

    with open(ENROLLMENT_FILE, 'rb+') as f:
        f.seek(0, os.SEEK_END)
        file_size     = f.tell()
        total_records = file_size // RECORD_SIZE

        if index_to_delete >= total_records:
            print("   -> Lỗi: index ngoài phạm vi!\n")
            return 0

        moves = 0
        for j in range(index_to_delete + 1, total_records):
            f.seek(j * RECORD_SIZE)
            record = f.read(RECORD_SIZE)
            f.seek((j - 1) * RECORD_SIZE)
            f.write(record)
            moves += 1

        f.truncate(file_size - RECORD_SIZE)

    print(f"   -> Đã dồn xong. Số lần ghi đĩa: {moves:,}")
    print(f"   -> Kích thước file sau xóa: {os.path.getsize(ENROLLMENT_FILE):,} bytes\n")
    return moves


# ════════════════════════════════════════════════════════════════
#  CÁCH 2 — THAY BẰNG RECORD CUỐI (Replace with Last)   O(1)
# ════════════════════════════════════════════════════════════════
def demo_method_2_swap(index_to_delete):
    print(f"2. Demo Cách 2 — Thay Record Cuối: xóa index {index_to_delete}")

    with open(ENROLLMENT_FILE, 'rb+') as f:
        f.seek(0, os.SEEK_END)
        file_size     = f.tell()
        total_records = file_size // RECORD_SIZE

        # BƯỚC 1: Đọc record cuối (O(1) seek — không tính ghi đĩa)
        f.seek((total_records - 1) * RECORD_SIZE)
        last_record = f.read(RECORD_SIZE)
        print(f"   -> Record cuối: {last_record.decode(errors='replace').strip()}")

        # BƯỚC 2: Ghi đè record cuối vào vị trí cần xóa  — GHI ĐĨA LẦN 1
        f.seek(index_to_delete * RECORD_SIZE)
        f.write(last_record)
        print(f"   -> [Ghi đĩa #1] Ghi nội dung record cuối vào slot {index_to_delete}")

        # BƯỚC 3: Cắt bỏ slot cuối (truncate = thao tác ghi metadata) — GHI ĐĨA LẦN 2
        f.truncate(file_size - RECORD_SIZE)
        print(f"   -> [Ghi đĩa #2] Truncate file — cắt bỏ slot cuối (giảm {RECORD_SIZE} bytes)")

    print(f"   -> Kích thước file sau xóa: {os.path.getsize(ENROLLMENT_FILE):,} bytes\n")


# ════════════════════════════════════════════════════════════════
#  CÁCH 3 — FREE LIST / TOMBSTONE   O(1)
# ════════════════════════════════════════════════════════════════
def _load_free_list():
    if not os.path.exists(FREE_LIST_FILE):
        return []
    with open(FREE_LIST_FILE, 'r') as f:
        content = f.read().strip()
        return [int(x) for x in content.split(',') if x] if content else []

def _save_free_list(slots):
    with open(FREE_LIST_FILE, 'w') as f:
        f.write(','.join(str(s) for s in slots))

def demo_method_3_freelist(index_to_delete):
    print(f"3. Demo Cách 3 — Tombstone + Free List: xóa index {index_to_delete}")

    offset = index_to_delete * RECORD_SIZE
    print(f"   -> Tính offset = {index_to_delete} × {RECORD_SIZE} = {offset} bytes")

    with open(ENROLLMENT_FILE, 'rb+') as f:
        f.seek(offset)
        original = f.read(RECORD_SIZE)
        print(f"   -> Record gốc : {original.decode(errors='replace').strip()}")

        # Ghi flag '*' vào byte đầu tiên
        f.seek(offset)
        tombstone = b'*' + original[1:]
        f.write(tombstone)
        print(f"   -> Đã ghi tombstone: {tombstone.decode(errors='replace').strip()}")

    # Cập nhật Free List
    free_slots = _load_free_list()
    free_slots.insert(0, index_to_delete)
    _save_free_list(free_slots)
    chain = ' -> '.join(str(s) for s in free_slots) + ' -> NULL'
    print(f"   -> Free List (HEAD): {chain}")
    print(f"   -> Kích thước file KHÔNG THAY ĐỔI: {os.path.getsize(ENROLLMENT_FILE):,} bytes")
    print(f"   -> Slot {index_to_delete} sẽ được tái sử dụng khi INSERT tiếp theo\n")


# ════════════════════════════════════════════════════════════════
#  COMPARE — Chạy & đo thời gian 3 phương pháp
# ════════════════════════════════════════════════════════════════
def compare():
    print("=" * 65)
    print("  DEMO: Các chiến lược Xóa Record trong Fixed-Length File")
    print(f"  Schema: Enrollment(student_id, course_id, semester, score)")
    print(f"  File  : {ENROLLMENT_FILE}  |  RECORD_SIZE = {RECORD_SIZE} bytes")
    print(f"  Tổng  : {NUM_ENROLLMENTS:,} bản ghi  |  Xóa index i = {DELETE_INDEX:,}")
    print("=" * 65 + "\n")

    # ── Tạo (hoặc tái sử dụng) file dữ liệu ─────────────────────
    create_all_files()

    # In thử một vài bản ghi để kiểm tra
    print("  [Kiểm tra] 3 bản ghi đầu tiên trong Enrollment:")
    for idx in range(3):
        rec = read_enrollment(idx)
        print(f"    [{idx}] {rec}")
    print()

    # Xóa free list cũ nếu có
    if os.path.exists(FREE_LIST_FILE):
        os.remove(FREE_LIST_FILE)

    # ── Sao lưu file gốc để 3 cách cùng bắt đầu từ dữ liệu giống nhau ──
    backup = ENROLLMENT_FILE + '.bak'
    print(f"  Đang sao lưu {ENROLLMENT_FILE} -> {backup} ...")
    shutil.copy(ENROLLMENT_FILE, backup)
    print("  Sao lưu xong.\n")

    # ── CÁCH 1: DỒN ──────────────────────────────────────────────
    t1_start = time.perf_counter()
    moves = demo_method_1_shift(DELETE_INDEX)
    t1_ms = (time.perf_counter() - t1_start) * 1000

    # ── Khôi phục cho Cách 2 ─────────────────────────────────────
    shutil.copy(backup, ENROLLMENT_FILE)

    t2_start = time.perf_counter()
    demo_method_2_swap(DELETE_INDEX)
    t2_ms = (time.perf_counter() - t2_start) * 1000

    # ── Khôi phục cho Cách 3 ─────────────────────────────────────
    shutil.copy(backup, ENROLLMENT_FILE)
    if os.path.exists(FREE_LIST_FILE):
        os.remove(FREE_LIST_FILE)

    t3_start = time.perf_counter()
    demo_method_3_freelist(DELETE_INDEX)
    t3_ms = (time.perf_counter() - t3_start) * 1000

    # ── Dọn dẹp ──────────────────────────────────────────────────
    os.remove(backup)

    # ── Bảng kết quả ─────────────────────────────────────────────
    print("=" * 65)
    print("  KẾT QUẢ BENCHMARK  (xóa bản ghi số {:,} / {:,})".format(
          DELETE_INDEX, NUM_ENROLLMENTS))
    print("=" * 65)
    print(f"  {'Phương pháp':<30} {'Thời gian (ms)':>14}  {'Ghi đĩa':>10}")
    print(f"  {'-'*57}")
    print(f"  {'① Dồn (Shift)':<30} {t1_ms:>14.4f}  {moves:>8,} lần")
    print(f"  {'② Thay Record Cuối (Replace)':<30} {t2_ms:>14.4f}  {'2':>8} lần  (ghi + truncate)")
    print(f"  {'③ Tombstone + Free List':<30} {t3_ms:>14.4f}  {'1':>8} lần")
    print(f"  {'-'*57}")
    if t2_ms > 0:
        print(f"  Cách 1 chậm hơn Cách 2 khoảng: {t1_ms/t2_ms:.1f}×")
    if t3_ms > 0:
        print(f"  Cách 1 chậm hơn Cách 3 khoảng: {t1_ms/t3_ms:.1f}×")
    print("=" * 65)


if __name__ == '__main__':
    DELETE_INDEX = 3          # Xóa bản ghi thứ 4 (index 0-based)
    #compare()
    demo_method_2_swap(DELETE_INDEX)