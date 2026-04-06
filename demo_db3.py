import os
import time
import shutil

# ════════════════════════════════════════════════════════════════
#  DEMO XÓA MÔN HỌC — CASCADE DELETE (3 CHIẾN LƯỢC)
#
#  Khi xóa 1 môn học khỏi courses.txt,
#  phải xóa TOÀN BỘ enrollment liên quan trong enrollments.txt
#
#  Schema:
#    Course     (courses.txt)     — 71 bytes/record
#    Enrollment (enrollments.txt) — 31 bytes/record
#
#  Course record layout:
#    [0: 8]  course_id    8B  (CS000001 … CS500000)
#    [8:43]  course_name 35B
#   [43:45]  credits      2B
#   [45:70]  dept_name   25B
#   [70:71]  newline      1B
#
#  Enrollment record layout:
#    [0: 8]  student_id   8B
#    [8:16]  course_id    8B  ← khóa ngoại liên kết với Course
#   [16:24]  semester     8B
#   [24:30]  score        6B
#   [30:31]  newline      1B
# ════════════════════════════════════════════════════════════════

COURSE_FILE     = 'courses.txt'
ENROLLMENT_FILE = 'enrollments.txt'
CRS_FREE_LIST   = 'course_free_list.txt'
ENR_FREE_LIST   = 'enroll_free_list.txt'

CRS_SIZE = 71
ENR_SIZE = 31


# ────────────────────────────────────────────────────────────────
#  HELPERS
# ────────────────────────────────────────────────────────────────
def parse_course(raw: bytes) -> dict:
    return {
        'course_id':   raw[0:8].decode('utf-8', errors='replace').strip(),
        'course_name': raw[8:43].decode('utf-8', errors='replace').strip(),
        'credits':     raw[43:45].decode('utf-8', errors='replace').strip(),
        'dept_name':   raw[45:70].decode('utf-8', errors='replace').strip(),
    }

def parse_enrollment(raw: bytes) -> dict:
    return {
        'student_id': raw[0:8].decode('utf-8', errors='replace').strip(),
        'course_id':  raw[8:16].decode('utf-8', errors='replace').strip(),
        'semester':   raw[16:24].decode('utf-8', errors='replace').strip(),
        'score':      raw[24:30].decode('utf-8', errors='replace').strip(),
    }

def read_course(index: int) -> bytes:
    with open(COURSE_FILE, 'rb') as f:
        f.seek(index * CRS_SIZE)
        return f.read(CRS_SIZE)

def print_course(label: str, raw: bytes):
    r = parse_course(raw)
    print(f"   {label}:")
    print(f"      course_id  : {r['course_id']}")
    print(f"      Tên môn    : {r['course_name']}")
    print(f"      Tín chỉ   : {r['credits']}")
    print(f"      Khoa       : {r['dept_name']}")

def fmt_enr(raw: bytes, idx: int) -> str:
    e = parse_enrollment(raw)
    return (f"[idx={idx}] sid={e['student_id']} | "
            f"cid={e['course_id']} | sem={e['semester']} | score={e['score']}")

def crs_total() -> int:
    return os.path.getsize(COURSE_FILE) // CRS_SIZE

def enr_total() -> int:
    return os.path.getsize(ENROLLMENT_FILE) // ENR_SIZE

def load_fl(path: str) -> list:
    if not os.path.exists(path): return []
    txt = open(path).read().strip()
    return [int(x) for x in txt.split(',') if x] if txt else []

def save_fl(path: str, slots: list):
    open(path, 'w').write(','.join(str(s) for s in slots))


# ────────────────────────────────────────────────────────────────
#  SCAN ENROLLMENTS — tìm tất cả index có course_id khớp   O(M)
#  course_id nằm ở bytes [8:16] của mỗi enrollment record
# ────────────────────────────────────────────────────────────────
def scan_enrollment_by_course(course_id: str) -> list:
    """Quét enrollments.txt, trả về list index có course_id khớp."""
    cid_bytes = course_id.encode('utf-8').ljust(8)[:8]
    indices   = []
    total     = enr_total()
    CHUNK     = 20_000
    with open(ENROLLMENT_FILE, 'rb') as f:
        for start in range(0, total, CHUNK):
            end = min(start + CHUNK, total)
            buf = f.read((end - start) * ENR_SIZE)
            for i in range(end - start):
                off = i * ENR_SIZE
                # course_id ở bytes [8:16] của mỗi record
                if buf[off + 8 : off + 16] == cid_bytes:
                    indices.append(start + i)
    return indices


# ════════════════════════════════════════════════════════════════
#  CÁCH 1 — DỒN (Shift Forward)   O(N−i) + O(K × M)
#  • Xóa course bằng shift: dồn tất cả record sau slot i lên 1
#  • Với mỗi enrollment liên quan: shift xóa (từ cao → thấp)
# ════════════════════════════════════════════════════════════════
def _shift_course_file(index: int, f) -> int:
    """Dồn courses.txt: xóa record tại index, trả về số lần ghi."""
    f.seek(0, os.SEEK_END)
    total = f.tell() // CRS_SIZE
    moves = 0
    for j in range(index + 1, total):
        f.seek(j * CRS_SIZE)
        rec = f.read(CRS_SIZE)
        f.seek((j - 1) * CRS_SIZE)
        f.write(rec)
        moves += 1
    f.truncate(total * CRS_SIZE - CRS_SIZE)
    return moves

def _shift_enr_one(index: int, f) -> int:
    """Dồn enrollments.txt: xóa record tại index, trả về số lần ghi."""
    f.seek(0, os.SEEK_END)
    total = f.tell() // ENR_SIZE
    moves = 0
    for j in range(index + 1, total):
        f.seek(j * ENR_SIZE)
        rec = f.read(ENR_SIZE)
        f.seek((j - 1) * ENR_SIZE)
        f.write(rec)
        moves += 1
    f.truncate(total * ENR_SIZE - ENR_SIZE)
    return moves

def cascade_delete_method_1(crs_index: int):
    sep = '─' * 60
    print(f"\n{sep}")
    print(f"  CÁCH 1 — Dồn (Shift Forward) — CASCADE DELETE")
    print(f"  Course O(N−i)  +  Enrollment O(K × M)")
    print(sep)

    raw_crs = read_course(crs_index)
    crs     = parse_course(raw_crs)
    cid     = crs['course_id']
    print_course("Môn học cần xóa", raw_crs)

    # ── PHASE 1: Xóa course bằng Shift ───────────────────────────
    print(f"\n  [PHASE 1] Xóa course index={crs_index} khỏi courses.txt ...")
    t_crs = time.perf_counter()
    crs_moves = 0
    with open(COURSE_FILE, 'rb+') as f:
        crs_moves = _shift_course_file(crs_index, f)
    t_crs = (time.perf_counter() - t_crs) * 1000
    print(f"  -> Dồn {crs_moves:,} record. Còn lại: {crs_total():,} môn học")
    print(f"  -> Số lần ghi đĩa: {crs_moves:,} (write) + 1 (truncate) = {crs_moves+1:,}")

    # ── PHASE 2: Quét + xóa enrollment liên quan ──────────────────
    print(f"\n  [PHASE 2] Quét enrollments.txt tìm course_id = '{cid}' ...")
    t_scan = time.perf_counter()
    enr_indices = scan_enrollment_by_course(cid)
    t_scan = (time.perf_counter() - t_scan) * 1000
    print(f"  -> Quét {enr_total():,} bản ghi — tìm thấy {len(enr_indices)} enrollment liên quan")
    if enr_indices:
        print(f"  -> Các index (tăng dần): {sorted(enr_indices)[:10]}{'...' if len(enr_indices)>10 else ''}")

    # Xóa từ index cao → thấp để tránh lệch index
    enr_indices_desc = sorted(enr_indices, reverse=True)
    t_del = time.perf_counter()
    total_enr_moves = 0
    with open(ENROLLMENT_FILE, 'rb+') as f:
        for idx in enr_indices_desc:
            total_enr_moves += _shift_enr_one(idx, f)
    t_del = (time.perf_counter() - t_del) * 1000

    print(f"  -> Dồn xong. Tổng lần ghi đĩa: {total_enr_moves:,}")
    print(f"  -> Enrollment còn lại: {enr_total():,}")

    print(f"\n  ✅ CASCADE DELETE HOÀN TẤT (Cách 1 — Shift)")
    print(f"  Thời gian xóa course   : {t_crs:.4f} ms  ({crs_moves+1:,} ghi đĩa)")
    print(f"  Thời gian quét enroll  : {t_scan:.4f} ms  O(M={enr_total()+len(enr_indices):,})")
    print(f"  Thời gian xóa enroll   : {t_del:.4f} ms  ({total_enr_moves:,} ghi đĩa)")
    return crs_moves, total_enr_moves, t_crs + t_scan + t_del


# ════════════════════════════════════════════════════════════════
#  CÁCH 2 — THAY RECORD CUỐI (Swap)   O(1) + O(K)
#  • Đọc record cuối → ghi đè vào slot crs_index → truncate
#  • Với mỗi enrollment: swap với enrollment cuối → truncate
#    (xóa từ index cao → thấp để tránh swap nhầm)
# ════════════════════════════════════════════════════════════════
def cascade_delete_method_2(crs_index: int):
    sep = '─' * 60
    print(f"\n{sep}")
    print(f"  CÁCH 2 — Thay Record Cuối (Swap) — CASCADE DELETE")
    print(f"  Course O(1)  +  Enrollment O(K) — Mất thứ tự!")
    print(sep)

    raw_crs = read_course(crs_index)
    crs     = parse_course(raw_crs)
    cid     = crs['course_id']
    print_course("Môn học cần xóa", raw_crs)

    # ── PHASE 1: Swap course ──────────────────────────────────────
    print(f"\n  [PHASE 1] Xóa course index={crs_index} (swap với cuối) ...")
    t_crs = time.perf_counter()
    with open(COURSE_FILE, 'rb+') as f:
        f.seek(0, os.SEEK_END)
        fsz   = f.tell()
        total = fsz // CRS_SIZE
        last_idx = total - 1
        f.seek(last_idx * CRS_SIZE)
        last_raw = f.read(CRS_SIZE)
        if crs_index != last_idx:
            f.seek(crs_index * CRS_SIZE)
            f.write(last_raw)
            print(f"  -> [Ghi đĩa #1] Ghi course cuối ({parse_course(last_raw)['course_id']}) → slot {crs_index}")
        else:
            print(f"  -> Là record cuối — chỉ truncate")
        f.truncate(fsz - CRS_SIZE)
    print(f"  -> [Ghi đĩa #2] Truncate courses.txt")
    t_crs = (time.perf_counter() - t_crs) * 1000
    print(f"  -> Còn lại: {crs_total():,} môn học")

    # ── PHASE 2: Quét + swap enrollment liên quan ────────────────
    print(f"\n  [PHASE 2] Quét enrollments.txt tìm course_id = '{cid}' ...")
    t_scan = time.perf_counter()
    enr_indices = scan_enrollment_by_course(cid)
    t_scan = (time.perf_counter() - t_scan) * 1000
    print(f"  -> Quét {enr_total():,} bản ghi — tìm thấy {len(enr_indices)} enrollment liên quan")

    if not enr_indices:
        print("  -> Không có enrollment nào cần xóa.")
        enr_writes = 0
        t_del = 0.0
    else:
        print(f"  -> Danh sách index (tăng dần): {sorted(enr_indices)[:10]}{'...' if len(enr_indices)>10 else ''}")
        print(f"  -> Xử lý từ index CAO → THẤP\n")

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
                    print(f"  ⚠ idx={idx} vượt quá file hiện tại ({total}), bỏ qua.")
                    continue

                step += 1
                last_idx = total - 1

                # Đọc record cần xóa
                f.seek(idx * ENR_SIZE)
                del_raw = f.read(ENR_SIZE)

                # Đọc record cuối
                f.seek(last_idx * ENR_SIZE)
                last_raw = f.read(ENR_SIZE)

                print(f"  Lần {step}: Xóa enrollment tại index {idx}")
                print(f"    [XÓA ] {fmt_enr(del_raw, idx)}")

                if idx == last_idx:
                    print(f"    [NOTE ] Đây đã là record cuối → chỉ truncate")
                    f.truncate(fsz - ENR_SIZE)
                    enr_writes += 1
                    print(f"    [Ghi đĩa #1] Truncate — bỏ slot {last_idx}")
                else:
                    print(f"    [THAY] {fmt_enr(last_raw, last_idx)}  ← dời về slot {idx}")
                    f.seek(idx * ENR_SIZE)
                    f.write(last_raw)
                    enr_writes += 1
                    print(f"    [Ghi đĩa #1] Ghi record cuối (idx={last_idx}) → slot {idx}")
                    f.truncate(fsz - ENR_SIZE)
                    enr_writes += 1
                    print(f"    [Ghi đĩa #2] Truncate — bỏ slot {last_idx}")

                print(f"    -> File còn {(fsz - ENR_SIZE) // ENR_SIZE:,} enrollment\n")

        t_del = (time.perf_counter() - t_del) * 1000

    print(f"  ✅ Xóa xong {len(enr_indices)} enrollment ({enr_writes} ghi đĩa)")
    print(f"  -> Enrollment còn lại: {enr_total():,}")

    print(f"\n{'─'*60}")
    print(f"  ✅ CASCADE DELETE HOÀN TẤT (Cách 2 — Swap)")
    print(f"  Thời gian xóa course   : {t_crs:.4f} ms  (2 ghi đĩa: write+truncate)")
    print(f"  Thời gian quét enroll  : {t_scan:.4f} ms  O(M={enr_total()+len(enr_indices):,})")
    print(f"  Thời gian xóa enroll   : {t_del:.4f} ms  ({enr_writes} ghi đĩa)")
    print(f"  ⚠  Thứ tự cả 2 file đã thay đổi (swap đảo vị trí)")
    return enr_writes, t_crs + t_scan + t_del


# ════════════════════════════════════════════════════════════════
#  CÁCH 3 — TOMBSTONE + FREE LIST   O(1) + O(K)
#  • Ghi '*' vào byte đầu của course record
#  • Ghi '*' vào byte đầu của từng enrollment liên quan
#  • File KHÔNG thay đổi kích thước
#  • Slots được lưu vào free list để tái sử dụng khi INSERT
# ════════════════════════════════════════════════════════════════
def cascade_delete_method_3(crs_index: int):
    sep = '─' * 60
    print(f"\n{sep}")
    print(f"  CÁCH 3 — Tombstone + Free List — CASCADE DELETE")
    print(f"  Course O(1)  +  Enrollment O(K) — File KHÔNG thu nhỏ")
    print(sep)

    raw_crs = read_course(crs_index)
    crs     = parse_course(raw_crs)
    cid     = crs['course_id']
    print_course("Môn học cần xóa", raw_crs)

    # ── PHASE 1: Tombstone course ─────────────────────────────────
    print(f"\n  [PHASE 1] Tombstone course index={crs_index} ...")
    t_crs = time.perf_counter()
    offset = crs_index * CRS_SIZE
    with open(COURSE_FILE, 'rb+') as f:
        f.seek(offset)
        original  = f.read(CRS_SIZE)
        tombstone = b'*' + original[1:]
        f.seek(offset)
        f.write(tombstone)
    fl_crs = load_fl(CRS_FREE_LIST)
    fl_crs.insert(0, crs_index)
    save_fl(CRS_FREE_LIST, fl_crs)
    t_crs = (time.perf_counter() - t_crs) * 1000
    print(f"  -> [Ghi đĩa #1] Byte đầu = '*' tại offset {offset} (course_id: {cid} → *{cid[1:]})")
    print(f"  -> Course free list: {fl_crs[:8]}{'...' if len(fl_crs)>8 else ''} → NULL")

    # ── PHASE 2: Quét + tombstone enrollments ────────────────────
    print(f"\n  [PHASE 2] Quét enrollments.txt tìm course_id = '{cid}' ...")
    t_scan = time.perf_counter()
    enr_indices = scan_enrollment_by_course(cid)
    t_scan = (time.perf_counter() - t_scan) * 1000
    print(f"  -> Tìm thấy {len(enr_indices)} enrollment liên quan")

    if not enr_indices:
        print("  -> Không có enrollment nào cần đánh dấu.")
        t_del = 0.0
    else:
        print(f"  -> Các index: {sorted(enr_indices)[:10]}{'...' if len(enr_indices)>10 else ''}\n")
        t_del = time.perf_counter()
        with open(ENROLLMENT_FILE, 'rb+') as f:
            for i, idx in enumerate(enr_indices, 1):
                off = idx * ENR_SIZE
                f.seek(off)
                orig = f.read(ENR_SIZE)
                enr = parse_enrollment(orig)
                tombstone = b'*' + orig[1:]
                f.seek(off)
                f.write(tombstone)
                print(f"  [{i:>3}] Tombstone idx={idx} | "
                      f"cid={enr['course_id']} | sid={enr['student_id']} | "
                      f"sem={enr['semester']} | score={enr['score']}")
        fl_enr = load_fl(ENR_FREE_LIST)
        fl_enr = sorted(enr_indices) + fl_enr
        save_fl(ENR_FREE_LIST, fl_enr)
        t_del = (time.perf_counter() - t_del) * 1000
        print(f"\n  -> Enroll free list: {fl_enr[:5]}{'...' if len(fl_enr)>5 else ''} → NULL")

    print(f"\n  ✅ CASCADE DELETE HOÀN TẤT (Cách 3 — Tombstone)")
    print(f"  Thời gian xóa course   : {t_crs:.4f} ms  (1 ghi đĩa)")
    print(f"  Thời gian quét enroll  : {t_scan:.4f} ms  O(M={enr_total():,})")
    print(f"  Thời gian xóa enroll   : {t_del:.4f} ms  ({len(enr_indices)} ghi đĩa)")
    print(f"  Kích thước courses.txt     : {os.path.getsize(COURSE_FILE):,} B (KHÔNG ĐỔI)")
    print(f"  Kích thước enrollments.txt : {os.path.getsize(ENROLLMENT_FILE):,} B (KHÔNG ĐỔI)")
    return len(enr_indices), t_crs + t_scan + t_del


# ════════════════════════════════════════════════════════════════
#  COMPARE — Chạy cả 3 cách với backup/restore
# ════════════════════════════════════════════════════════════════
def compare(delete_index: int = 10):
    for fn in [COURSE_FILE, ENROLLMENT_FILE]:
        if not os.path.exists(fn):
            print(f"❌ Không tìm thấy {fn}. Hãy chạy demo_db.py trước!")
            return

    raw_crs = read_course(delete_index)
    cid     = parse_course(raw_crs)['course_id']

    print("═" * 60)
    print("  CASCADE DELETE — XÓA MÔN HỌC + ENROLLMENT LIÊN QUAN")
    print(f"  courses.txt     : {crs_total():,} bản ghi × {CRS_SIZE} B")
    print(f"  enrollments.txt : {enr_total():,} bản ghi × {ENR_SIZE} B")
    print(f"  Môn học cần xóa : index={delete_index}  course_id={cid}")
    print("═" * 60)

    for fl in [CRS_FREE_LIST, ENR_FREE_LIST]:
        if os.path.exists(fl): os.remove(fl)

    bak_crs = COURSE_FILE + '.bak'
    bak_enr = ENROLLMENT_FILE + '.bak'
    print(f"\n  📦 Sao lưu 2 file dữ liệu...")
    shutil.copy(COURSE_FILE,    bak_crs)
    shutil.copy(ENROLLMENT_FILE, bak_enr)

    # ── CÁCH 1 ──────────────────────────────────────────────────
    t1s = time.perf_counter()
    crs_mv, enr_mv, _ = cascade_delete_method_1(delete_index)
    t1 = (time.perf_counter() - t1s) * 1000

    shutil.copy(bak_crs, COURSE_FILE)
    shutil.copy(bak_enr, ENROLLMENT_FILE)
    for fl in [CRS_FREE_LIST, ENR_FREE_LIST]:
        if os.path.exists(fl): os.remove(fl)

    # ── CÁCH 2 ──────────────────────────────────────────────────
    t2s = time.perf_counter()
    enr_writes2, _ = cascade_delete_method_2(delete_index)
    t2 = (time.perf_counter() - t2s) * 1000

    shutil.copy(bak_crs, COURSE_FILE)
    shutil.copy(bak_enr, ENROLLMENT_FILE)
    for fl in [CRS_FREE_LIST, ENR_FREE_LIST]:
        if os.path.exists(fl): os.remove(fl)

    # ── CÁCH 3 ──────────────────────────────────────────────────
    t3s = time.perf_counter()
    enr_count3, _ = cascade_delete_method_3(delete_index)
    t3 = (time.perf_counter() - t3s) * 1000

    os.remove(bak_crs)
    os.remove(bak_enr)

    # ── BẢNG KẾT QUẢ ────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"  BENCHMARK CASCADE DELETE  (course_id = {cid})")
    print(f"{'═'*60}")
    print(f"  {'Cách':<26} {'Tổng t.gian':>11}  Ghi đĩa (course+enr)   Độ p.tạp")
    print(f"  {'─'*57}")
    print(f"  {'① Shift':<26} {t1:>10.1f}ms  {crs_mv}+{enr_mv}={crs_mv+enr_mv}              O(N)+O(K×M)")
    print(f"  {'② Replace with Last':<26} {t2:>10.1f}ms  2+{enr_writes2}={2+enr_writes2}                O(1)+O(K)")
    print(f"  {'③ Tombstone+Free List':<26} {t3:>10.1f}ms  1+{enr_count3}={1+enr_count3}                O(1)+O(K)")
    print(f"  {'─'*57}")
    print(f"  Lưu ý: Scan O(M={enr_total():,}) là bắt buộc với cả 3 cách")
    print(f"  (vì không có index trên course_id trong enrollments.txt)")
    print(f"{'═'*60}")


if __name__ == '__main__':
    DELETE_INDEX = 10   # Xóa môn học ở vị trí index 10 (0-based)
    cascade_delete_method_2(DELETE_INDEX)
