import os
import time
import shutil

# ════════════════════════════════════════════════════════════════
#  DEMO XÓA SINH VIÊN — CASCADE DELETE (3 CHIẾN LƯỢC)
#
#  Khi xóa 1 sinh viên khỏi students.txt,
#  phải xóa TOÀN BỘ enrollment liên quan trong enrollments.txt
#  ➜ Đây là thao tác CASCADE DELETE trong DBMS thực tế.
#
#  Schema:
#    Student    (students.txt)    — 91 bytes/record
#    Enrollment (enrollments.txt) — 30 bytes/record
#
#  Student record layout:
#    [0: 8]  student_id   8B
#    [8:38]  full_name   30B
#   [38:48]  class_name  10B
#   [48:78]  email       30B
#   [78:90]  phone       12B
#   [90:91]  newline      1B
#
#  Enrollment (enrollments.txt) — 31 bytes/record
#    [0: 8]  student_id   8B
#    [8:16]  course_id    8B  (CS000001…CS500000 — 6 chữ số)
#   [16:24]  semester     8B
#   [24:30]  score        6B
#   [30:31]  newline      1B
# ════════════════════════════════════════════════════════════════

STUDENT_FILE    = 'students.txt'
ENROLLMENT_FILE = 'enrollments.txt'
STU_FREE_LIST   = 'student_free_list.txt'
ENR_FREE_LIST   = 'enroll_free_list.txt'

STU_SIZE = 91
ENR_SIZE = 31


# ────────────────────────────────────────────────────────────────
#  HELPERS
# ────────────────────────────────────────────────────────────────
def parse_student(raw: bytes) -> dict:
    return {
        'student_id': raw[0:8].decode('utf-8', errors='replace').strip(),
        'full_name':  raw[8:38].decode('utf-8', errors='replace').strip(),
        'class_name': raw[38:48].decode('utf-8', errors='replace').strip(),
        'email':      raw[48:78].decode('utf-8', errors='replace').strip(),
        'phone':      raw[78:90].decode('utf-8', errors='replace').strip(),
    }

def parse_enrollment(raw: bytes) -> dict:
    return {
        'student_id': raw[0:8].decode('utf-8', errors='replace').strip(),
        'course_id':  raw[8:16].decode('utf-8', errors='replace').strip(),
        'semester':   raw[16:24].decode('utf-8', errors='replace').strip(),
        'score':      raw[24:30].decode('utf-8', errors='replace').strip(),
    }

def read_student(index: int) -> bytes:
    with open(STUDENT_FILE, 'rb') as f:
        f.seek(index * STU_SIZE)
        return f.read(STU_SIZE)

def print_student(label: str, raw: bytes):
    r = parse_student(raw)
    print(f"   {label}: [{r['student_id']}] {r['full_name']} | {r['class_name']} | {r['email']}")

def stu_total() -> int:
    return os.path.getsize(STUDENT_FILE) // STU_SIZE

def enr_total() -> int:
    return os.path.getsize(ENROLLMENT_FILE) // ENR_SIZE

def load_fl(path: str) -> list:
    if not os.path.exists(path): return []
    txt = open(path).read().strip()
    return [int(x) for x in txt.split(',') if x] if txt else []

def save_fl(path: str, slots: list):
    open(path, 'w').write(','.join(str(s) for s in slots))


# ────────────────────────────────────────────────────────────────
#  SCAN ENROLLMENTS — tìm tất cả index có student_id khớp  O(M)
# ────────────────────────────────────────────────────────────────
def scan_enrollment_indices(student_id: str) -> list:
    """Quét toàn bộ enrollments.txt, trả về list index khớp student_id."""
    sid_bytes = student_id.encode('utf-8').ljust(8)[:8]
    indices   = []
    total     = enr_total()
    CHUNK     = 10_000
    with open(ENROLLMENT_FILE, 'rb') as f:
        for start in range(0, total, CHUNK):
            end = min(start + CHUNK, total)
            buf = f.read((end - start) * ENR_SIZE)
            for i in range(end - start):
                if buf[i * ENR_SIZE: i * ENR_SIZE + 8] == sid_bytes:
                    indices.append(start + i)
    return indices


# ════════════════════════════════════════════════════════════════
#  CÁCH 1 — DỒN (Shift Forward)   O(N−i) + O(K × M)
#  • Xóa student bằng shift
#  • Xóa từng enrollment liên quan bằng shift
#    (xóa từ index cao xuống thấp để tránh lệch index)
# ════════════════════════════════════════════════════════════════
def _enr_shift_one(file_size: int, index: int, f) -> int:
    """Dồn 1 enrollment record tại index trong file handle đã mở rb+."""
    total = file_size // ENR_SIZE
    moves = 0
    for j in range(index + 1, total):
        f.seek(j * ENR_SIZE)
        rec = f.read(ENR_SIZE)
        f.seek((j - 1) * ENR_SIZE)
        f.write(rec)
        moves += 1
    f.truncate(file_size - ENR_SIZE)
    return moves

def cascade_delete_method_1(stu_index: int):
    sep = '─' * 60
    print(f"\n{sep}")
    print(f"  CÁCH 1 — Dồn (Shift Forward) — CASCADE DELETE")
    print(f"  Student O(N−i)  +  Enrollment O(K × M)")
    print(sep)

    raw_stu   = read_student(stu_index)
    stu       = parse_student(raw_stu)
    sid       = stu['student_id']
    print_student("Sinh viên cần xóa", raw_stu)

    # ── PHASE 1: Xóa student bằng Shift ──────────────────────────
    print(f"\n  [PHASE 1] Xóa student index={stu_index} khỏi students.txt ...")
    t_stu = time.perf_counter()
    total_stu = stu_total()
    stu_moves = 0
    with open(STUDENT_FILE, 'rb+') as f:
        f.seek(0, os.SEEK_END)
        fsz = f.tell()
        for j in range(stu_index + 1, total_stu):
            f.seek(j * STU_SIZE)
            rec = f.read(STU_SIZE)
            f.seek((j - 1) * STU_SIZE)
            f.write(rec)
            stu_moves += 1
        f.truncate(fsz - STU_SIZE)
    t_stu = (time.perf_counter() - t_stu) * 1000
    print(f"  -> Dồn {stu_moves:,} record. Còn lại: {stu_total():,} sinh viên")

    # ── PHASE 2: Quét + xóa enrollment liên quan ──────────────────
    print(f"\n  [PHASE 2] Quét enrollments.txt tìm student_id = '{sid}' ...")
    t_scan = time.perf_counter()
    enr_indices = scan_enrollment_indices(sid)
    t_scan = (time.perf_counter() - t_scan) * 1000
    print(f"  -> Quét {enr_total():,} bản ghi — tìm thấy {len(enr_indices)} enrollment liên quan")
    if enr_indices:
        print(f"  -> Các index cần xóa: {enr_indices[:10]}{'...' if len(enr_indices)>10 else ''}")

    # Xóa từ index cao → thấp để tránh lệch index sau mỗi lần shift
    enr_indices_desc = sorted(enr_indices, reverse=True)
    t_del = time.perf_counter()
    total_enr_moves = 0
    with open(ENROLLMENT_FILE, 'rb+') as f:
        for idx in enr_indices_desc:
            f.seek(0, os.SEEK_END)
            fsz    = f.tell()
            total_enr_moves += _enr_shift_one(fsz, idx, f)
    t_del = (time.perf_counter() - t_del) * 1000

    print(f"  -> Xóa xong {len(enr_indices)} enrollment — dồn {total_enr_moves:,} lần")
    print(f"  -> Enrollment còn lại: {enr_total():,}")

    print(f"\n  ✅ CASCADE DELETE HOÀN TẤT (Cách 1)")
    print(f"  Thời gian xóa student  : {t_stu:.2f} ms  ({stu_moves:,} ghi đĩa)")
    print(f"  Thời gian quét enroll  : {t_scan:.2f} ms  (O(M) = O({enr_total():,}))")
    print(f"  Thời gian xóa enroll   : {t_del:.2f} ms  ({total_enr_moves:,} ghi đĩa)")
    return stu_moves, total_enr_moves, t_stu + t_scan + t_del


# ════════════════════════════════════════════════════════════════
#  CÁCH 2 — THAY RECORD CUỐI (Replace with Last)   O(1) + O(K)
#  • Xóa student bằng swap (2 lần ghi đĩa)
#  • Xóa từng enrollment liên quan bằng swap
# ════════════════════════════════════════════════════════════════
def cascade_delete_method_2(stu_index: int):
    sep = '─' * 60
    print(f"\n{sep}")
    print(f"  CÁCH 2 — Thay Record Cuối (Swap) — CASCADE DELETE")
    print(f"  Student O(1)  +  Enrollment O(K) — Mất thứ tự!")
    print(sep)

    raw_stu = read_student(stu_index)
    stu     = parse_student(raw_stu)
    sid     = stu['student_id']
    print_student("Sinh viên cần xóa", raw_stu)

    # ── PHASE 1: Xóa student bằng Swap ───────────────────────────
    print(f"\n  [PHASE 1] Xóa student index={stu_index} (swap với cuối) ...")
    t_stu = time.perf_counter()
    with open(STUDENT_FILE, 'rb+') as f:
        f.seek(0, os.SEEK_END)
        fsz   = f.tell()
        total = fsz // STU_SIZE
        f.seek((total - 1) * STU_SIZE)
        last  = f.read(STU_SIZE)
        f.seek(stu_index * STU_SIZE)
        f.write(last)
        f.truncate(fsz - STU_SIZE)
    t_stu = (time.perf_counter() - t_stu) * 1000
    print(f"  -> [Ghi đĩa #1] Ghi record cuối vào slot {stu_index}")
    print(f"  -> [Ghi đĩa #2] Truncate file")
    print(f"  -> Còn lại: {stu_total():,} sinh viên")

    # ── PHASE 2: Quét + xóa enrollment liên quan bằng swap ───────
    print(f"\n  [PHASE 2] Quét enrollments.txt tìm student_id = '{sid}' ...")
    t_scan = time.perf_counter()
    enr_indices = scan_enrollment_indices(sid)
    t_scan = (time.perf_counter() - t_scan) * 1000
    print(f"  -> Quét {enr_total():,} bản ghi — tìm thấy {len(enr_indices)} enrollment liên quan")

    if not enr_indices:
        print("  -> Không có enrollment nào cần xóa.")
    else:
        print(f"  -> Danh sách index cần xóa (sắp xếp tăng dần): {sorted(enr_indices)}")
        print(f"  -> Xử lý từ index CAO → THẤP để tránh lệch index sau mỗi lần xóa\n")

        def fmt_enr(raw: bytes, idx: int) -> str:
            e = parse_enrollment(raw)
            return (f"[idx={idx}] sid={e['student_id']} | "
                    f"cid={e['course_id']} | sem={e['semester']} | score={e['score']}")

        # Swap: xóa từ index cao → thấp
        enr_indices_desc = sorted(enr_indices, reverse=True)
        t_del = time.perf_counter()
        enr_writes = 0
        step = 0
        with open(ENROLLMENT_FILE, 'rb+') as f:
            for idx in enr_indices_desc:
                f.seek(0, os.SEEK_END)
                fsz   = f.tell()
                total = fsz // ENR_SIZE
                if idx >= total:
                    print(f"  ⚠ idx={idx} vượt quá kích thước hiện tại ({total}), bỏ qua.")
                    continue

                step += 1

                # Đọc record cần xóa
                f.seek(idx * ENR_SIZE)
                del_raw = f.read(ENR_SIZE)

                # Đọc record cuối
                last_idx = total - 1
                f.seek(last_idx * ENR_SIZE)
                last_raw = f.read(ENR_SIZE)

                print(f"  Lần {step}: Xóa enrollment tại index {idx}")
                print(f"    [XÓA ] {fmt_enr(del_raw, idx)}")

                if idx == last_idx:
                    # Record cần xóa chính là record cuối → chỉ truncate
                    print(f"    [NOTE ] Đây đã là record cuối → chỉ cần truncate, không cần swap")
                    f.truncate(fsz - ENR_SIZE)
                    enr_writes += 1   # truncate
                    print(f"    [Ghi đĩa #1] Truncate — file giảm {ENR_SIZE} bytes")
                else:
                    # Ghi record cuối vào vị trí idx
                    print(f"    [THAY] {fmt_enr(last_raw, last_idx)}  ← dời về slot {idx}")
                    f.seek(idx * ENR_SIZE)
                    f.write(last_raw)
                    enr_writes += 1   # write
                    print(f"    [Ghi đĩa #1] Ghi record cuối (idx={last_idx}) → slot {idx}")

                    # Truncate slot cuối
                    f.truncate(fsz - ENR_SIZE)
                    enr_writes += 1   # truncate
                    print(f"    [Ghi đĩa #2] Truncate — bỏ slot {last_idx} (trùng dữ liệu)")

                print(f"    -> File còn {(fsz - ENR_SIZE) // ENR_SIZE:,} enrollment\n")

        t_del = (time.perf_counter() - t_del) * 1000

    print(f"  ✅ Xóa xong {len(enr_indices)} enrollment")
    print(f"  -> Tổng ghi đĩa enrollment : {enr_writes}")
    print(f"  -> Enrollment còn lại      : {enr_total():,}")

    print(f"\n{'─'*60}")
    print(f"  ✅ CASCADE DELETE HOÀN TẤT (Cách 2 — Swap)")
    print(f"  Thời gian xóa student  : {t_stu:.4f} ms   (2 ghi đĩa: write + truncate)")
    print(f"  Thời gian quét enroll  : {t_scan:.4f} ms   O(M={enr_total()+len(enr_indices):,})")
    print(f"  Thời gian xóa enroll   : {t_del:.4f} ms   ({enr_writes} ghi đĩa)")
    print(f"  ⚠  Thứ tự cả 2 file đã thay đổi (swap làm đảo vị trí)")
    return enr_writes, t_stu + t_scan + t_del


# ════════════════════════════════════════════════════════════════
#  CÁCH 3 — TOMBSTONE + FREE LIST   O(1) + O(K)
#  • Ghi '*' vào byte đầu của student record
#  • Ghi '*' vào byte đầu của từng enrollment liên quan
#  • File KHÔNG thay đổi kích thước
#  • ĐÂY LÀ CÁCH CHUẨN TRONG DBMS THỰC TẾ
# ════════════════════════════════════════════════════════════════
def cascade_delete_method_3(stu_index: int):
    sep = '─' * 60
    print(f"\n{sep}")
    print(f"  CÁCH 3 — Tombstone + Free List — CASCADE DELETE")
    print(f"  Student O(1)  +  Enrollment O(K) — File KHÔNG thu nhỏ")
    print(sep)

    raw_stu = read_student(stu_index)
    stu     = parse_student(raw_stu)
    sid     = stu['student_id']
    print_student("Sinh viên cần xóa", raw_stu)

    # ── PHASE 1: Tombstone student ────────────────────────────────
    print(f"\n  [PHASE 1] Tombstone student index={stu_index} ...")
    t_stu = time.perf_counter()
    offset = stu_index * STU_SIZE
    with open(STUDENT_FILE, 'rb+') as f:
        f.seek(offset)
        original  = f.read(STU_SIZE)
        tombstone = b'*' + original[1:]
        f.seek(offset)
        f.write(tombstone)
    fl_stu = load_fl(STU_FREE_LIST)
    fl_stu.insert(0, stu_index)
    save_fl(STU_FREE_LIST, fl_stu)
    t_stu = (time.perf_counter() - t_stu) * 1000
    print(f"  -> [Ghi đĩa #1] Byte đầu = '*' tại offset {offset}")
    print(f"  -> Student free list: {fl_stu[:5]}{'...' if len(fl_stu)>5 else ''} → NULL")

    # ── PHASE 2: Quét + tombstone enrollments ────────────────────
    print(f"\n  [PHASE 2] Quét enrollments.txt tìm student_id = '{sid}' ...")
    t_scan = time.perf_counter()
    enr_indices = scan_enrollment_indices(sid)
    t_scan = (time.perf_counter() - t_scan) * 1000
    print(f"  -> Tìm thấy {len(enr_indices)} enrollment liên quan")

    t_del = time.perf_counter()
    with open(ENROLLMENT_FILE, 'rb+') as f:
        for idx in enr_indices:
            off = idx * ENR_SIZE
            f.seek(off)
            orig = f.read(ENR_SIZE)
            f.seek(off)
            f.write(b'*' + orig[1:])
    fl_enr = load_fl(ENR_FREE_LIST)
    fl_enr = enr_indices + fl_enr
    save_fl(ENR_FREE_LIST, fl_enr)
    t_del = (time.perf_counter() - t_del) * 1000

    print(f"  -> [Ghi đĩa ×{len(enr_indices)}] Tombstone {len(enr_indices)} enrollment")
    print(f"  -> Enroll free list: {fl_enr[:5]}{'...' if len(fl_enr)>5 else ''} → NULL")

    print(f"\n  ✅ CASCADE DELETE HOÀN TẤT (Cách 3)")
    print(f"  Thời gian xóa student  : {t_stu:.2f} ms   (1 ghi đĩa)")
    print(f"  Thời gian quét enroll  : {t_scan:.2f} ms   O(M)")
    print(f"  Thời gian xóa enroll   : {t_del:.2f} ms   ({len(enr_indices)} ghi đĩa)")
    print(f"  Kích thước students.txt    : {os.path.getsize(STUDENT_FILE):,} B (KHÔNG ĐỔI)")
    print(f"  Kích thước enrollments.txt : {os.path.getsize(ENROLLMENT_FILE):,} B (KHÔNG ĐỔI)")
    return len(enr_indices), t_stu + t_scan + t_del


# ════════════════════════════════════════════════════════════════
#  COMPARE — Chạy cả 3 cách với backup/restore
# ════════════════════════════════════════════════════════════════
def compare(delete_index: int = 5):
    for fn in [STUDENT_FILE, ENROLLMENT_FILE]:
        if not os.path.exists(fn):
            print(f"❌ Không tìm thấy {fn}. Hãy chạy demo_db.py trước!")
            return

    raw_stu = read_student(delete_index)
    sid     = parse_student(raw_stu)['student_id']

    print("═" * 60)
    print("  CASCADE DELETE — XÓA SINH VIÊN + ENROLLMENT LIÊN QUAN")
    print(f"  students.txt    : {stu_total():,} bản ghi × {STU_SIZE} B")
    print(f"  enrollments.txt : {enr_total():,} bản ghi × {ENR_SIZE} B")
    print(f"  Sinh viên cần xóa : index={delete_index}  student_id={sid}")
    print("═" * 60)

    # Xóa free list cũ
    for fl in [STU_FREE_LIST, ENR_FREE_LIST]:
        if os.path.exists(fl): os.remove(fl)

    # Sao lưu cả 2 file
    bak_stu = STUDENT_FILE + '.bak'
    bak_enr = ENROLLMENT_FILE + '.bak'
    print(f"\n  📦 Sao lưu 2 file dữ liệu...")
    shutil.copy(STUDENT_FILE,    bak_stu)
    shutil.copy(ENROLLMENT_FILE, bak_enr)

    # ── CÁCH 1 ──────────────────────────────────────────────────
    t1_start = time.perf_counter()
    stu_mv, enr_mv, _ = cascade_delete_method_1(delete_index)
    t1 = (time.perf_counter() - t1_start) * 1000

    shutil.copy(bak_stu, STUDENT_FILE)
    shutil.copy(bak_enr, ENROLLMENT_FILE)
    for fl in [STU_FREE_LIST, ENR_FREE_LIST]:
        if os.path.exists(fl): os.remove(fl)

    # ── CÁCH 2 ──────────────────────────────────────────────────
    t2_start = time.perf_counter()
    enr_writes2, _ = cascade_delete_method_2(delete_index)
    t2 = (time.perf_counter() - t2_start) * 1000

    shutil.copy(bak_stu, STUDENT_FILE)
    shutil.copy(bak_enr, ENROLLMENT_FILE)
    for fl in [STU_FREE_LIST, ENR_FREE_LIST]:
        if os.path.exists(fl): os.remove(fl)

    # ── CÁCH 3 ──────────────────────────────────────────────────
    t3_start = time.perf_counter()
    enr_count3, _ = cascade_delete_method_3(delete_index)
    t3 = (time.perf_counter() - t3_start) * 1000

    # Dọn dẹp
    os.remove(bak_stu)
    os.remove(bak_enr)

    # ── BẢNG KẾT QUẢ ────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"  BENCHMARK CASCADE DELETE  (student_id = {sid})")
    print(f"{'═'*60}")
    print(f"  {'Cách':<26} {'Tổng t.gian':>11}  Ghi đĩa (stu+enr)    Độ phức tạp")
    print(f"  {'─'*57}")
    print(f"  {'① Shift':<26} {t1:>10.1f}ms  {stu_mv:,}+{enr_mv:,}={stu_mv+enr_mv:,}  O(N)+O(K×M)")
    print(f"  {'② Replace with Last':<26} {t2:>10.1f}ms  2+{enr_writes2}={2+enr_writes2}           O(1)+O(K)")
    print(f"  {'③ Tombstone+Free List':<26} {t3:>10.1f}ms  1+{enr_count3}={1+enr_count3}           O(1)+O(K)")
    print(f"  {'─'*57}")
    print(f"  Lưu ý: Cả 3 cách đều cần quét O(M) enrollment để tìm bản ghi liên quan.")
    print(f"  Sự khác biệt là ở bước XÓA: O(N) vs O(1) vs O(1).")
    print(f"{'═'*60}")


if __name__ == '__main__':
    DELETE_INDEX = 5    # Xóa sinh viên ở vị trí index 5 (0-based)
    # compare(DELETE_INDEX)
    cascade_delete_method_2(DELETE_INDEX)