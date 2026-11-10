"""
PIXHAWK GROUND STATION - tema hijau ala film (pygame + pymavlink)

Install : pip install pygame pymavlink pyserial
Contoh  :
python Try.py                        # AUTO: cari Pixhawk di USB, sambung ulang sendiri
python Try.py COM9 115200            # pakai port tertentu
python Try.py udpin:0.0.0.0:14550    # lewat UDP (Mission Planner / SITL)
python Try.py --demo                 # data simulasi tanpa Pixhawk

Tombol  : ESC keluar | F12 screenshot | L rekam CSV | R reset statistik
          T scanline | G glow | F batas FPS | S supersampling | UP/DOWN/A tunda
"""
import csv
import math
import multiprocessing as mp
import os
import queue
import sys
import threading
import time
from collections import deque

os.environ.setdefault("PYGAME_HIDE_SUPPORT_PROMPT", "1")
import pygame  # noqa: E402
from pymavlink import mavutil  # noqa: E402

W, H = 1280, 720

# ---------------------------------------------------------------- pengaturan
SMOOTH_TIME = 0.12     # penghalusan angka biasa (alt, speed, dll)
ATT_SMOOTH = 0.03      # penghalusan tambahan horizon/kompas (kecil saja)
ATT_DELAY = None       # jeda tayang attitude: None = otomatis, atau detik (mis. 0.1)
SS = 2                 # supersampling horizon (2 = halus, 1 = ringan)
GRAPH_SECONDS = 12.0   # lebar waktu grafik

# ---------------------------------------------------------------- warna (hijau ala film)
BG = (0, 8, 3)
PANEL = (1, 20, 9)
GRID = (0, 46, 21)
G1 = (70, 255, 130)       # terang
G2 = (0, 205, 95)         # sedang
G3 = (0, 125, 58)         # redup
G4 = (0, 66, 30)          # sangat redup
SKY = (0, 30, 15)
GROUND = (0, 78, 38)
FLOOR = (0, 118, 56)
PALE = (185, 255, 215)    # putih kehijauan
AMBER = (255, 190, 40)
RED = (255, 70, 70)

# KNOWN_VIDS: VID USB yang umum dipakai board PX4/ArduPilot
KNOWN_VIDS = {0x3162, 0x3185, 0x2DAE, 0x26AC, 0x1209}

# ---------------------------------------------------------------- data bersama
state = {
    "roll": 0.0, "pitch": 0.0, "yaw": 0.0,
    "heading": 0, "alt": 0.0, "gs": 0.0, "as": None, "climb": 0.0, "thr": 0,
    "lat": 0.0, "lon": 0.0,
    "volt": None, "curr": None, "batt": -1,
    "fix": 0, "sats": 0,
    "armed": False, "mode": "-",
    "connected": False, "last_hb": 0.0,
    "msg_count": 0, "last_type": "-",
    "rates": {}, "rate_total": 0.0,
    "rc": [0] * 8,
    "status": "Mulai...", "error": "", "port": "-",
}
lock = threading.RLock()
running = True

console = deque(maxlen=80)                    # (waktu, severity, teks)
att_buf = deque(maxlen=120)                   # (waktu_lokal, roll, pitch, yaw)
att_meta = {"off": None, "last_boot": None, "dt": 0.05}
_cnt = {}
_win = [time.time()]

# Antrean keluar: dipakai proses pembaca untuk mengirim data baru ke proses tampilan
outbox_att = deque(maxlen=1000)
outbox_log = deque(maxlen=300)


def log(text, sev=6):
    with lock:
        entry = (time.time(), sev, text)
        console.append(entry)
        outbox_log.append(entry)


def set_status(text, error=""):
    with lock:
        state["status"] = text
        state["error"] = error


# ---------------------------------------------------------------- MAVLink
def find_port():
    """Cari port Pixhawk otomatis (dipanggil ulang tiap sambung ulang)."""
    try:
        from serial.tools import list_ports
    except ImportError:
        return None
    ports = list(list_ports.comports())
    for p in ports:
        if p.vid in KNOWN_VIDS:
            return p.device
    for p in ports:
        d = (p.description or "").lower()
        if any(k in d for k in ("px4", "ardupilot", "pixhawk", "usb serial")):
            return p.device
    return ports[0].device if ports else None


def request_streams(m):
    """Minta HANYA data yang dipakai (bukan semua) dengan laju yang cukup."""
    ts, tc = m.target_system, m.target_component
    try:  # ArduPilot
        m.mav.request_data_stream_send(ts, tc, mavutil.mavlink.MAV_DATA_STREAM_ALL, 0, 0)
        for stream, hz in (
                (mavutil.mavlink.MAV_DATA_STREAM_EXTRA1, 50),             # ATTITUDE
                (mavutil.mavlink.MAV_DATA_STREAM_EXTRA2, 5),              # VFR_HUD
                (mavutil.mavlink.MAV_DATA_STREAM_POSITION, 5),            # GLOBAL_POSITION_INT
                (mavutil.mavlink.MAV_DATA_STREAM_EXTENDED_STATUS, 2),     # SYS_STATUS, GPS_RAW_INT
                (mavutil.mavlink.MAV_DATA_STREAM_RC_CHANNELS, 5)):        # RC_CHANNELS
            m.mav.request_data_stream_send(ts, tc, stream, hz, 1)
    except Exception:  # noqa: BLE001
        pass
    try:  # PX4 / MAVLink 2
        for msg_id, us in ((30, 20000), (74, 200000), (33, 200000),
                           (1, 500000), (24, 500000), (65, 200000)):
            m.mav.command_long_send(ts, tc, mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL, 0,
                                    msg_id, us, 0, 0, 0, 0, 0)
    except Exception:  # noqa: BLE001
        pass


def handle(msg):
    t = msg.get_type()
    with lock:
        state["msg_count"] += 1
        state["last_type"] = t
        _cnt[t] = _cnt.get(t, 0) + 1
        now = time.time()
        if now - _win[0] >= 1.0:
            dt = now - _win[0]
            state["rates"] = {k: v / dt for k, v in _cnt.items()}
            state["rate_total"] = sum(_cnt.values()) / dt
            _cnt.clear()
            _win[0] = now

        if t == "HEARTBEAT":
            if msg.type == mavutil.mavlink.MAV_TYPE_GCS:
                return
            armed = bool(msg.base_mode & mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED)
            try:
                mode = mavutil.mode_string_v10(msg)
            except Exception:  # noqa: BLE001
                mode = str(msg.custom_mode)
            if state["connected"]:
                if armed != state["armed"]:
                    log("*** ARMED ***" if armed else "DISARMED", 4 if armed else 6)
                if mode != state["mode"]:
                    log(f"MODE -> {mode}")
            state["connected"] = True
            state["last_hb"] = now
            state["armed"] = armed
            state["mode"] = mode
        elif t == "ATTITUDE":
            state["roll"] = math.degrees(msg.roll)
            state["pitch"] = math.degrees(msg.pitch)
            state["yaw"] = math.degrees(msg.yaw)
            # Waktu sampel dari jam Pixhawk (time_boot_ms) dipetakan ke jam lokal,
            # supaya jarak antar sampel akurat walau data tiba berkelompok.
            boot = msg.time_boot_ms / 1000.0
            am = att_meta
            if am["last_boot"] is not None and boot < am["last_boot"]:
                am["off"] = None
                att_buf.clear()
            off = time.perf_counter() - boot
            am["off"] = off if am["off"] is None else min(am["off"] + 0.0002, off)
            if am["last_boot"] is not None and boot > am["last_boot"]:
                am["dt"] += 0.1 * ((boot - am["last_boot"]) - am["dt"])
            am["last_boot"] = boot
            sample = (boot + am["off"], state["roll"], state["pitch"], state["yaw"])
            att_buf.append(sample)
            outbox_att.append(sample)
        elif t == "VFR_HUD":
            state["heading"] = msg.heading
            state["gs"] = msg.groundspeed
            state["as"] = msg.airspeed if math.isfinite(msg.airspeed) else None
            state["climb"] = msg.climb
            state["thr"] = msg.throttle
        elif t == "GLOBAL_POSITION_INT":
            state["lat"] = msg.lat / 1e7
            state["lon"] = msg.lon / 1e7
            state["alt"] = msg.relative_alt / 1000.0
        elif t == "SYS_STATUS":
            state["volt"] = None if msg.voltage_battery == 0xFFFF else msg.voltage_battery / 1000.0
            state["curr"] = None if msg.current_battery < 0 else msg.current_battery / 100.0
            state["batt"] = msg.battery_remaining
        elif t == "GPS_RAW_INT":
            state["fix"] = msg.fix_type
            state["sats"] = msg.satellites_visible
        elif t == "RC_CHANNELS":
            state["rc"] = [msg.chan1_raw, msg.chan2_raw, msg.chan3_raw, msg.chan4_raw,
                           msg.chan5_raw, msg.chan6_raw, msg.chan7_raw, msg.chan8_raw]
        elif t == "STATUSTEXT":
            text = msg.text
            if isinstance(text, bytes):
                text = text.decode(errors="ignore")
            log(text.replace("\x00", "").strip(), msg.severity)


def reader(conn, baud):
    """Thread pembaca MAVLink dengan sambung ulang otomatis."""
    auto = conn.lower() == "auto"
    is_serial = auto or conn.upper().startswith("COM") or conn.startswith("/dev/")

    while running:
        port = conn
        if auto:
            port = find_port()
            if port is None:
                with lock:
                    state["connected"] = False
                    state["port"] = "-"
                set_status("Menunggu Pixhawk dicolok ke USB ...")
                time.sleep(1)
                continue
        with lock:
            state["port"] = port

        try:
            set_status(f"Membuka {port} ...")
            m = (mavutil.mavlink_connection(port, baud=baud) if is_serial
                 else mavutil.mavlink_connection(port))
        except Exception as e:  # noqa: BLE001
            with lock:
                state["connected"] = False
            set_status(f"Gagal membuka {port}, mencoba lagi ...", str(e))
            time.sleep(1.5)
            continue

        got = False
        for waited in range(1, 11):
            if not running:
                break
            set_status(f"Port {port} terbuka, menunggu heartbeat ({waited}/10 dtk) ...")
            if m.recv_match(type="HEARTBEAT", blocking=True, timeout=1) is not None:
                got = True
                break
        if not got:
            set_status(f"Tidak ada heartbeat dari {port}. Mencari ulang ...",
                       "Cek baud rate / firmware / program lain yang memakai port.")
            try:
                m.close()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1)
            continue

        set_status("Terhubung")
        log(f"LINK OK  {port}  sys={m.target_system}")
        with lock:
            att_buf.clear()
            att_meta.update({"off": None, "last_boot": None, "dt": 0.05})
        request_streams(m)
        last_rx = last_req = time.time()
        while running:
            try:
                msg = m.recv_match(blocking=True, timeout=1)
            except Exception as e:  # noqa: BLE001
                set_status("Koneksi terputus, mencoba lagi ...", str(e))
                break
            now = time.time()
            if msg is None:
                if now - last_rx > 5:
                    set_status("Data berhenti, mencoba sambung ulang ...")
                    break
                continue
            last_rx = now
            handle(msg)
            if now - last_req > 10:
                request_streams(m)
                last_req = now
        with lock:
            state["connected"] = False
        log("LINK LOST", 4)
        try:
            m.close()
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)


def reader_proc(conn, baud, q):
    """Dijalankan di PROSES TERPISAH: membaca & mengurai MAVLink, lalu mengirim
    ringkasan ke proses tampilan. Dengan begitu pengurai pesan (Python murni)
    tidak berebut dengan penggambaran (tidak ada GIL bersama)."""
    threading.Thread(target=reader, args=(conn, baud), daemon=True).start()
    while True:
        time.sleep(0.01)
        with lock:
            snap = dict(state)
            snap["att_dt"] = att_meta["dt"]
            atts = list(outbox_att)
            outbox_att.clear()
            logs = list(outbox_log)
            outbox_log.clear()
        try:
            q.put_nowait((snap, atts, logs))
        except queue.Full:
            with lock:                      # kembalikan supaya tidak hilang
                outbox_att.extendleft(reversed(atts))
                outbox_log.extendleft(reversed(logs))


def demo_thread():
    t0 = time.time()
    log("MODE DEMO - data simulasi", 6)
    msgs = ["PreArm: Compass not calibrated", "EKF3 IMU0 is using GPS",
            "Mission: 4 WP loaded", "Battery failsafe cleared"]
    mi, next_msg = 0, 4.0
    while running:
        t = time.time() - t0
        with lock:
            state["connected"] = True
            state["last_hb"] = time.time()
            state["armed"] = int(t) % 30 > 4
            state["mode"] = "DEMO"
            state["roll"] = 30 * math.sin(t * 0.8)
            state["pitch"] = 15 * math.sin(t * 0.5)
            state["yaw"] = wrap180(t * 20)
            state["heading"] = int(t * 20) % 360
            state["alt"] = 20 + 10 * math.sin(t * 0.3)
            state["climb"] = 3 * math.cos(t * 0.3)
            state["gs"] = 5 + 2 * math.sin(t)
            state["as"] = state["gs"]
            state["thr"] = int(50 + 30 * math.sin(t * 0.4))
            state["lat"] = -6.2088 + 0.0004 * math.sin(t * 0.25)
            state["lon"] = 106.8456 + 0.0006 * math.cos(t * 0.25)
            state["volt"] = 16.0 - (t % 120) * 0.01
            state["curr"] = 12 + 3 * math.sin(t)
            state["batt"] = int(100 - (t % 120) * 0.8)
            state["fix"], state["sats"] = 3, 12
            state["rc"] = [1500 + int(400 * math.sin(t * (0.4 + 0.15 * i))) for i in range(8)]
            state["msg_count"] += 1
            state["rates"] = {"ATTITUDE": 50.0}
            state["rate_total"] = 90.0
            att_buf.append((time.perf_counter(), state["roll"], state["pitch"], state["yaw"]))
            att_meta["dt"] = 0.02
        if t > next_msg:
            log(msgs[mi % len(msgs)], 4 if mi % 2 == 0 else 6)
            mi += 1
            next_msg += 6
        time.sleep(0.02)


# ---------------------------------------------------------------- bantu
F = {}
_tcache = {}
_big = {}


def txt(font, s, color):
    """Cache hasil render teks supaya tidak dirender ulang tiap frame."""
    key = (id(font), s, color)
    img = _tcache.get(key)
    if img is None:
        if len(_tcache) > 900:
            _tcache.clear()
        img = font.render(s, True, color)
        _tcache[key] = img
    return img


def rot(x, y, ang_deg, cx, cy):
    a = math.radians(ang_deg)
    return (cx + x * math.cos(a) - y * math.sin(a),
            cy + x * math.sin(a) + y * math.cos(a))


def wrap180(a):
    return (a + 180) % 360 - 180


def interp_att(buf, rt, fallback):
    """Interpolasi kubik (Hermite) attitude pada waktu rt dari buffer sampel.
    Lebih halus daripada linear karena kecepatan gerak ikut kontinu."""
    n = len(buf)
    if n < 2:
        return fallback
    if rt <= buf[0][0]:
        return buf[0][1:]
    if rt >= buf[-1][0]:
        return buf[-1][1:]
    j = n - 1
    while j > 1 and buf[j - 1][0] > rt:
        j -= 1
    i1, i2 = j - 1, j
    i0, i3 = max(i1 - 1, 0), min(i2 + 1, n - 1)
    t1, t2 = buf[i1][0], buf[i2][0]
    h = t2 - t1
    if h <= 0:
        return buf[i2][1:]
    u = (rt - t1) / h
    h00 = 2 * u ** 3 - 3 * u ** 2 + 1
    h10 = u ** 3 - 2 * u ** 2 + u
    h01 = -2 * u ** 3 + 3 * u ** 2
    h11 = u ** 3 - u ** 2
    d02 = buf[i2][0] - buf[i0][0]
    d13 = buf[i3][0] - buf[i1][0]
    out = []
    for k in (1, 2, 3):
        p1 = buf[i1][k]
        p2 = p1 + wrap180(buf[i2][k] - p1)
        p0 = p1 + wrap180(buf[i0][k] - p1)
        p3 = p2 + wrap180(buf[i3][k] - p2)
        m1 = (p2 - p0) / d02 if d02 > 0 else 0.0
        m2 = (p3 - p1) / d13 if d13 > 0 else 0.0
        out.append(h00 * p1 + h10 * h * m1 + h01 * p2 + h11 * h * m2)
    return tuple(out)


def damp(cur, vel, target, smooth_time, dt):
    """Penghalusan orde-2 (critically damped spring)."""
    omega = 2.0 / smooth_time
    x = omega * dt
    e = 1.0 / (1.0 + x + 0.48 * x * x + 0.235 * x * x * x)
    change = cur - target
    temp = (vel + omega * change) * dt
    vel = (vel - omega * temp) * e
    return target + (change + temp) * e, vel


# ---------------------------------------------------------------- komponen gambar
def brackets(surf, rect, color=G1):
    n = 12
    for (x, y, dx, dy) in ((rect.left, rect.top, 1, 1), (rect.right - 1, rect.top, -1, 1),
                           (rect.left, rect.bottom - 1, 1, -1), (rect.right - 1, rect.bottom - 1, -1, -1)):
        pygame.draw.line(surf, color, (x, y), (x + dx * n, y), 2)
        pygame.draw.line(surf, color, (x, y), (x, y + dy * n), 2)


def panel(surf, rect, title=None):
    pygame.draw.rect(surf, PANEL, rect)
    pygame.draw.rect(surf, G4, rect, 1)
    brackets(surf, rect)
    if title:
        surf.blit(txt(F["s"], f"[ {title} ]", G2), (rect.x + 18, rect.y + 5))


def kv(surf, x, y, w, label, value, vcol=G1):
    surf.blit(txt(F["s"], label, G3), (x, y + 2))
    v = txt(F["m"], value, vcol)
    surf.blit(v, (x + w - v.get_width(), y))


def draw_horizon(surf, rect, roll, pitch, heading):
    panel(surf, rect)
    w, h = rect.width * SS, rect.height * SS
    big = _big.get((w, h))
    if big is None:
        big = pygame.Surface((w, h))
        _big[(w, h)] = big
    cx, cy = w / 2, h / 2
    ppd = h / 60.0
    d = pitch * ppd
    L = w * 2

    big.fill(SKY)
    pts = [rot(x, y, -roll, cx, cy) for x, y in [(-L, d), (L, d), (L, d + L), (-L, d + L)]]
    pygame.draw.polygon(big, GROUND, pts)
    # lantai bergaris (perspektif) ala Tron
    for k in range(1, 8):
        y = d + (k ** 1.8) * 7 * SS
        pygame.draw.line(big, FLOOR, rot(-L, y, -roll, cx, cy), rot(L, y, -roll, cx, cy), SS)
    for xk in range(-900, 901, 150):
        pygame.draw.line(big, FLOOR, rot(0, d, -roll, cx, cy),
                         rot(xk * SS, d + 420 * SS, -roll, cx, cy), SS)
    pygame.draw.line(big, G1, rot(-L, d, -roll, cx, cy), rot(L, d, -roll, cx, cy), 3 * SS)

    labels = []
    for deg in range(-40, 45, 5):
        if deg == 0:
            continue
        y = d - deg * ppd
        major = deg % 10 == 0
        wt = (46 if major else 22) * SS
        pygame.draw.line(big, G1, rot(-wt, y, -roll, cx, cy), rot(wt, y, -roll, cx, cy),
                         2 * SS if major else SS)
        if major:
            for sgn in (1, -1):
                lp = rot(sgn * (wt + 16 * SS), y, -roll, cx, cy)
                labels.append((str(abs(deg)), rect.x + lp[0] / SS, rect.y + lp[1] / SS))

    surf.blit(pygame.transform.smoothscale(big, rect.size) if SS != 1 else big, rect.topleft)

    old = surf.get_clip()
    surf.set_clip(rect)
    for s, lx, ly in labels:
        img = txt(F["s"], s, G1)
        surf.blit(img, img.get_rect(center=(lx, ly)))

    cx1, cy1 = rect.center
    # skala roll (berputar mengikuti horizon) + penunjuk tetap
    R = 128
    arc = [rot(R * math.sin(math.radians(a)), -R * math.cos(math.radians(a)), -roll, cx1, cy1)
           for a in range(-60, 61, 5)]
    pygame.draw.lines(surf, G3, False, arc, 1)
    for a in (-60, -45, -30, -20, -10, 0, 10, 20, 30, 45, 60):
        ln = 14 if a in (0, -30, 30, -60, 60) else 8
        s_, c_ = math.sin(math.radians(a)), math.cos(math.radians(a))
        p_out = rot(R * s_, -R * c_, -roll, cx1, cy1)
        p_in = rot((R - ln) * s_, -(R - ln) * c_, -roll, cx1, cy1)
        pygame.draw.line(surf, G1, p_out, p_in, 2)
    pygame.draw.polygon(surf, PALE, [(cx1, cy1 - R + 8), (cx1 - 7, cy1 - R - 6), (cx1 + 7, cy1 - R - 6)])

    # simbol pesawat
    for (ax, ay, bx, by) in ((cx1 - 64, cy1, cx1 - 16, cy1), (cx1 + 16, cy1, cx1 + 64, cy1),
                             (cx1 - 16, cy1, cx1 - 16, cy1 + 10), (cx1 + 16, cy1, cx1 + 16, cy1 + 10)):
        pygame.draw.line(surf, PALE, (ax, ay), (bx, by), 4)
    pygame.draw.circle(surf, PALE, (cx1, cy1), 4)
    surf.set_clip(old)

    # pita heading di atas
    tape = pygame.Rect(rect.x + 70, rect.y + 8, rect.w - 140, 28)
    pygame.draw.rect(surf, (0, 14, 6), tape)
    pygame.draw.rect(surf, G3, tape, 1)
    ppd_h = tape.w / 60.0
    surf.set_clip(tape)
    start = int(math.floor(heading - 31))
    for deg in range(start, start + 63):
        if deg % 5:
            continue
        x = tape.centerx + (deg - heading) * ppd_h
        major = deg % 10 == 0
        pygame.draw.line(surf, G2, (x, tape.bottom - 1), (x, tape.bottom - (11 if major else 6)), 1)
        if major:
            d360 = deg % 360
            lab = {0: "N", 90: "E", 180: "S", 270: "W"}.get(d360, f"{d360:03d}")
            img = txt(F["s"], lab, G1 if lab in "NESW" else G2)
            surf.blit(img, img.get_rect(midtop=(x, tape.y + 2)))
    surf.set_clip(old)
    box = pygame.Rect(0, 0, 50, 22)
    box.midtop = (tape.centerx, tape.bottom + 3)
    pygame.draw.rect(surf, (0, 14, 6), box)
    pygame.draw.rect(surf, G1, box, 1)
    img = txt(F["m"], f"{int(heading) % 360:03d}", G1)
    surf.blit(img, img.get_rect(center=box.center))
    pygame.draw.polygon(surf, G1, [(tape.centerx, tape.bottom - 1), (tape.centerx - 5, tape.bottom + 5),
                                   (tape.centerx + 5, tape.bottom + 5)])
    pygame.draw.rect(surf, G3, rect, 1)
    brackets(surf, rect)


def draw_compass(surf, rect, heading):
    panel(surf, rect, "COMPASS")
    cx, cy = rect.centerx, rect.y + 28 + 98
    r = 96
    pygame.draw.circle(surf, (0, 12, 5), (cx, cy), r)
    pygame.draw.circle(surf, G2, (cx, cy), r, 2)
    pygame.draw.circle(surf, G4, (cx, cy), r - 30, 1)
    for deg in range(0, 360, 10):
        a = math.radians(deg - heading - 90)
        big_tick = deg % 30 == 0
        l = 12 if big_tick else 6
        pygame.draw.line(surf, G1 if big_tick else G3,
                         (cx + (r - 2) * math.cos(a), cy + (r - 2) * math.sin(a)),
                         (cx + (r - 2 - l) * math.cos(a), cy + (r - 2 - l) * math.sin(a)), 2 if big_tick else 1)
        if big_tick:
            lab = {0: "N", 90: "E", 180: "S", 270: "W"}.get(deg, str(deg // 10))
            col = AMBER if lab == "N" else (G1 if lab in "ESW" else G2)
            img = txt(F["s"], lab, col)
            surf.blit(img, img.get_rect(center=(cx + (r - 24) * math.cos(a), cy + (r - 24) * math.sin(a))))
    pygame.draw.polygon(surf, PALE, [(cx, cy - 24), (cx - 11, cy + 15), (cx, cy + 7), (cx + 11, cy + 15)])
    pygame.draw.polygon(surf, G1, [(cx, cy - r - 2), (cx - 6, cy - r - 12), (cx + 6, cy - r - 12)])
    img = txt(F["l"], f"{int(heading) % 360:03d}°", G1)
    surf.blit(img, img.get_rect(center=(cx, cy + r + 24)))


def draw_graph(surf, rect, title, series, now):
    panel(surf, rect, title)
    inner = pygame.Rect(rect.x + 8, rect.y + 28, rect.w - 16, rect.h - 36)
    pygame.draw.rect(surf, (0, 12, 5), inner)
    for i in range(5):
        y = inner.y + inner.h * i / 4
        pygame.draw.line(surf, GRID, (inner.x, y), (inner.right, y))
    for sec in range(0, int(GRAPH_SECONDS) + 1, 2):
        x = inner.right - sec / GRAPH_SECONDS * inner.w
        pygame.draw.line(surf, GRID, (x, inner.y), (x, inner.bottom))
    for idx, ser in enumerate(series):
        data = [(t, v) for t, v in ser["buf"] if now - t <= GRAPH_SECONDS]
        lo, hi = ser.get("lo"), ser.get("hi")
        if lo is None and len(data) >= 2:
            vs = [v for _, v in data]
            mid = (min(vs) + max(vs)) / 2
            span = max(max(vs) - min(vs), ser.get("min_span", 1.0)) * 1.15
            lo, hi = mid - span / 2, mid + span / 2
        if ser.get("zero") and lo is not None:
            y0 = inner.bottom - (0 - lo) / (hi - lo) * inner.h
            pygame.draw.line(surf, G3, (inner.x, y0), (inner.right, y0))
        if len(data) >= 2 and lo is not None:
            pts = [(inner.right - (now - t) / GRAPH_SECONDS * inner.w,
                    min(inner.bottom, max(inner.y, inner.bottom - (v - lo) / (hi - lo) * inner.h)))
                   for t, v in data]
            pygame.draw.lines(surf, ser["color"], False, pts, 2)
        surf.blit(txt(F["s"], ser["text"], ser["color"]), (rect.x + 190 + idx * 150, rect.y + 5))


def draw_map(surf, rect, trail, cur, hdg, fix, sats, now):
    panel(surf, rect, "GPS TRACK / RADAR")
    inner = pygame.Rect(rect.x + 8, rect.y + 28, rect.w - 16, rect.h - 36)
    pygame.draw.rect(surf, (0, 12, 5), inner)
    for i in range(1, 6):
        x = inner.x + inner.w * i / 6
        pygame.draw.line(surf, GRID, (x, inner.y), (x, inner.bottom))
    for i in range(1, 4):
        y = inner.y + inner.h * i / 4
        pygame.draw.line(surf, GRID, (inner.x, y), (inner.right, y))
    cx, cy = inner.center
    rad = min(inner.w, inner.h) / 2 - 4
    pygame.draw.circle(surf, G4, (cx, cy), int(rad), 1)
    pygame.draw.circle(surf, G4, (cx, cy), int(rad / 2), 1)
    ang = (now * 80) % 360                                  # sapuan radar
    for k in range(10):
        a = math.radians(ang - k * 4 - 90)
        col = (0, max(0, 150 - k * 15), max(0, 70 - k * 7))
        pygame.draw.line(surf, col, (cx, cy), (cx + rad * math.cos(a), cy + rad * math.sin(a)), 2 if k == 0 else 1)

    old = surf.get_clip()
    surf.set_clip(inner)
    if trail and cur is not None:
        xs = [p[0] for p in trail]
        ys = [p[1] for p in trail]
        span = max(max(xs) - min(xs), max(ys) - min(ys), 40.0)
        scale = min(inner.w, inner.h) * 0.8 / span
        mx, my = (min(xs) + max(xs)) / 2, (min(ys) + max(ys)) / 2

        def tp(p):
            return (cx + (p[0] - mx) * scale, cy - (p[1] - my) * scale)

        pts = [tp(p) for p in trail]
        if len(pts) >= 2:
            pygame.draw.lines(surf, G2, False, pts, 2)
        hx, hy = tp((0, 0))
        pygame.draw.rect(surf, AMBER, (hx - 4, hy - 4, 8, 8), 1)
        px, py = tp(cur)
        tri = [rot(x, y, hdg, px, py) for x, y in ((0, -10), (-6, 7), (6, 7))]
        pygame.draw.polygon(surf, PALE, tri)
        surf.blit(txt(F["s"], f"span ~{span:.0f} m", G3), (inner.x + 6, inner.bottom - 20))
    else:
        if int(now * 2) % 2 == 0:
            img = txt(F["m"], "NO GPS FIX", AMBER)
            surf.blit(img, img.get_rect(center=(cx, cy)))
        surf.blit(txt(F["s"], f"SAT: {sats}", G3), (inner.x + 6, inner.bottom - 20))
    surf.set_clip(old)
    surf.blit(txt(F["s"], "N", G1), (inner.right - 16, inner.y + 4))


def draw_console(surf, rect):
    panel(surf, rect, "SYSTEM CONSOLE")
    with lock:
        lines = list(console)[-8:]
    y = rect.y + 30
    for ts, sev, text in lines:
        col = RED if sev <= 3 else AMBER if sev == 4 else G2
        stamp = time.strftime("%H:%M:%S", time.localtime(ts))
        line = f"{stamp} {text}"
        if len(line) > 52:
            line = line[:51] + "~"
        surf.blit(txt(F["s"], line, col), (rect.x + 14, y))
        y += 16
    if int(time.time() * 2) % 2 == 0:
        surf.blit(txt(F["s"], "_", G1), (rect.x + 14, y))


def draw_rc(surf, rect, rc):
    panel(surf, rect, "RC CHANNELS")
    if not any(rc):
        img = txt(F["m"], "NO RC DATA", G3)
        surf.blit(img, img.get_rect(center=rect.center))
        return
    for i, v in enumerate(rc):
        col_i, row_i = divmod(i, 4)
        x = rect.x + 16 + col_i * 214
        y = rect.y + 34 + row_i * 32
        surf.blit(txt(F["s"], f"CH{i + 1}", G2), (x, y))
        bar = pygame.Rect(x + 40, y + 2, 120, 14)
        pygame.draw.rect(surf, (0, 12, 5), bar)
        frac = max(0.0, min(1.0, (v - 1000) / 1000.0))
        if frac > 0:
            pygame.draw.rect(surf, G2, (bar.x, bar.y, int(bar.w * frac), bar.h))
        pygame.draw.line(surf, G1, (bar.centerx, bar.y - 2), (bar.centerx, bar.bottom + 1))
        pygame.draw.rect(surf, G3, bar, 1)
        surf.blit(txt(F["s"], f"{v:4d}", G1), (bar.right + 8, y))


def fmt_time(sec):
    sec = int(sec)
    return f"{sec // 60:02d}:{sec % 60:02d}"


def main():
    global running, SS
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    demo = "--demo" in sys.argv
    conn = args[0] if args else "auto"
    baud = int(args[1]) if len(args) > 1 else 115200

    q = proc = None
    if demo:
        threading.Thread(target=demo_thread, daemon=True).start()
    else:
        q = mp.Queue(maxsize=300)
        proc = mp.Process(target=reader_proc, args=(conn, baud, q), daemon=True)
        proc.start()
    ts_corr = 0.0      # koreksi jam antar proses (jaga-jaga)

    pygame.init()
    vsync_ok = True
    try:   # vsync: gambar disinkronkan dengan refresh layar
        screen = pygame.display.set_mode((W, H), pygame.SCALED | pygame.RESIZABLE, vsync=1)
    except Exception:  # noqa: BLE001
        vsync_ok = False
        try:
            screen = pygame.display.set_mode((W, H), pygame.SCALED)
        except Exception:  # noqa: BLE001
            screen = pygame.display.set_mode((W, H))
    pygame.display.set_caption("PIXHAWK GROUND STATION")
    clock = pygame.time.Clock()
    names = "consolas,couriernew,courier,monospace"
    F["s"] = pygame.font.SysFont(names, 14)
    F["m"] = pygame.font.SysFont(names, 17, bold=True)
    F["l"] = pygame.font.SysFont(names, 28, bold=True)
    F["xl"] = pygame.font.SysFont(names, 36, bold=True)

    keys = ("roll", "pitch", "heading", "alt", "gs", "as", "thr", "volt", "climb", "curr")
    disp = {k: 0.0 for k in keys}
    vel = {k: 0.0 for k in keys}
    first = True
    last_t = time.perf_counter()
    manual_delay = ATT_DELAY
    delay = 0.1
    cap = 0 if vsync_ok else 60
    ft = deque(maxlen=60)

    hist = {k: deque(maxlen=1000) for k in ("roll", "pitch", "alt", "gs")}
    last_hist = 0.0
    trail = deque(maxlen=2000)
    home = cur_xy = last_xy = None
    max_alt = max_spd = 0.0
    armed_prev = False
    arm_t0 = None
    flight_acc = 0.0
    log_file = log_writer = None
    last_log = 0.0

    # scanline CRT: dibuat sebagai pengali (lebih murah daripada blit transparan)
    scan = pygame.Surface((W, H)).convert()
    scan.fill((255, 255, 255))
    for y in range(0, H, 3):
        pygame.draw.line(scan, (200, 200, 200), (0, y), (W, y))
    scan_on = True
    glow_on = True
    bloom_surf = None
    frame_i = 0
    slow = 0
    intro = pygame.Surface((W, H))
    intro.fill((0, 0, 0))
    t_start = time.perf_counter()
    log("PIXHAWK GROUND STATION siap", 6)
    s = dict(state)

    while True:
        for e in pygame.event.get():
            if e.type == pygame.QUIT or (e.type == pygame.KEYDOWN and e.key == pygame.K_ESCAPE):
                running = False
                if log_file:
                    log_file.close()
                if proc is not None:
                    proc.terminate()
                pygame.quit()
                return
            if e.type == pygame.KEYDOWN:
                if e.key == pygame.K_UP:
                    manual_delay = min(0.5, delay + 0.02)
                elif e.key == pygame.K_DOWN:
                    manual_delay = max(0.02, delay - 0.02)
                elif e.key == pygame.K_a:
                    manual_delay = None
                elif e.key == pygame.K_f:
                    cap = 60 if cap == 0 else 0
                elif e.key == pygame.K_s:
                    SS = 1 if SS == 2 else 2
                elif e.key == pygame.K_t:
                    scan_on = not scan_on
                elif e.key == pygame.K_g:
                    glow_on = not glow_on
                    slow = 0
                elif e.key == pygame.K_r:
                    max_alt = max_spd = 0.0
                    flight_acc, arm_t0 = 0.0, (time.perf_counter() if armed_prev else None)
                    trail.clear()
                    home = cur_xy = last_xy = None
                    log("Statistik & jejak GPS direset")
                elif e.key == pygame.K_m:      # jenis pesan apa yang paling banyak masuk?
                    top = sorted(s["rates"].items(), key=lambda kv: -kv[1])[:6]
                    log("TOP: " + ", ".join(f"{k} {v:.0f}" for k, v in top[:3]))
                    log("     " + ", ".join(f"{k} {v:.0f}" for k, v in top[3:]))
                elif e.key == pygame.K_F12:
                    name = time.strftime("screenshot_%Y%m%d_%H%M%S.png")
                    try:
                        pygame.image.save(screen, name)
                        log(f"Screenshot: {name}")
                    except Exception as ex:  # noqa: BLE001
                        log(f"Screenshot gagal: {ex}", 3)
                elif e.key == pygame.K_l:
                    if log_file:
                        log_file.close()
                        log_file = log_writer = None
                        log("Rekam CSV: OFF")
                    else:
                        name = time.strftime("telemetry_%Y%m%d_%H%M%S.csv")
                        try:
                            log_file = open(name, "w", newline="", encoding="utf-8")
                            log_writer = csv.writer(log_file)
                            log_writer.writerow(["time", "roll", "pitch", "yaw", "alt", "gspd", "aspd",
                                                 "climb", "throttle", "volt", "curr", "lat", "lon",
                                                 "fix", "sats", "mode", "armed"])
                            log(f"Rekam CSV: ON -> {name}")
                        except Exception as ex:  # noqa: BLE001
                            log_file = log_writer = None
                            log(f"Rekam CSV gagal: {ex}", 3)

        # ---- ambil data terbaru dari proses pembaca
        if q is not None:
            got = None
            new_att, new_log = [], []
            try:
                while True:
                    snap, atts, logs = q.get_nowait()
                    got = snap
                    new_att += atts
                    new_log += logs
            except queue.Empty:
                pass
            if got is not None:
                with lock:
                    state.update(got)
                    console.extend(new_log)
                    if new_att:
                        pc = time.perf_counter()
                        if abs(new_att[-1][0] + ts_corr - pc) > 2.0:   # jam beda -> samakan
                            ts_corr = pc - new_att[-1][0] - 0.01
                        att_buf.extend((a[0] + ts_corr, a[1], a[2], a[3]) for a in new_att)

        with lock:
            s = dict(state)

        # ---- waktu
        now = time.perf_counter()
        raw = now - last_t
        dt = max(min(raw, 0.05), 1e-4)
        ft.append(raw)
        last_t = now
        wall = time.time()

        # ---- attitude: interpolasi kubik dari buffer, tayang sedikit di masa lalu
        with lock:
            buf = list(att_buf)
        dt_est = s.get("att_dt", att_meta["dt"])
        delay = manual_delay if manual_delay is not None else max(0.05, min(2.2 * dt_est, 0.3))
        a_roll, a_pitch, a_yaw = interp_att(buf, now - delay, (s["roll"], s["pitch"], s["yaw"]))

        target_v = {
            "roll": a_roll, "pitch": a_pitch, "heading": a_yaw % 360,
            "alt": s["alt"], "gs": s["gs"], "as": s["as"] or 0.0,
            "thr": s["thr"], "volt": s["volt"] or 0.0,
            "climb": s["climb"], "curr": s["curr"] or 0.0,
        }
        if first and s["connected"]:
            for k in keys:
                disp[k] = target_v[k]
            first = False
        for k in keys:
            if k in ("roll", "pitch", "heading"):
                tgt = disp[k] + wrap180(target_v[k] - disp[k])
                st = ATT_SMOOTH
            else:
                tgt = target_v[k]
                st = SMOOTH_TIME
            disp[k], vel[k] = damp(disp[k], vel[k], tgt, st, dt)
        disp["roll"] = wrap180(disp["roll"])
        disp["pitch"] = wrap180(disp["pitch"])
        disp["heading"] %= 360

        # ---- statistik
        max_alt = max(max_alt, s["alt"])
        max_spd = max(max_spd, s["gs"])
        if s["armed"] and not armed_prev:
            arm_t0 = now
        if (not s["armed"]) and armed_prev and arm_t0 is not None:
            flight_acc += now - arm_t0
            arm_t0 = None
        armed_prev = s["armed"]
        flight_s = flight_acc + ((now - arm_t0) if arm_t0 is not None else 0.0)

        if s["fix"] >= 2 and (abs(s["lat"]) > 1e-6 or abs(s["lon"]) > 1e-6):
            if home is None:
                home = (s["lat"], s["lon"])
            gx = (s["lon"] - home[1]) * 111320.0 * math.cos(math.radians(home[0]))
            gy = (s["lat"] - home[0]) * 110540.0
            cur_xy = (gx, gy)
            if last_xy is None or math.hypot(gx - last_xy[0], gy - last_xy[1]) > 0.5:
                trail.append((gx, gy))
                last_xy = (gx, gy)

        if now - last_hist >= 1 / 60:
            for k, v in (("roll", disp["roll"]), ("pitch", disp["pitch"]),
                         ("alt", disp["alt"]), ("gs", disp["gs"])):
                hist[k].append((now, v))
            last_hist = now

        if log_writer and wall - last_log >= 0.1:
            last_log = wall
            try:
                log_writer.writerow([f"{wall:.3f}", f"{s['roll']:.2f}", f"{s['pitch']:.2f}", f"{s['yaw']:.2f}",
                                     f"{s['alt']:.2f}", f"{s['gs']:.2f}", s["as"], f"{s['climb']:.2f}",
                                     s["thr"], s["volt"], s["curr"], s["lat"], s["lon"],
                                     s["fix"], s["sats"], s["mode"], int(s["armed"])])
            except Exception:  # noqa: BLE001
                log_file = log_writer = None

        link_ok = s["connected"] and (wall - s["last_hb"] < 3)

        # ================================================================ GAMBAR
        screen.fill(BG)

        # --- bar atas
        pygame.draw.rect(screen, PANEL, (0, 0, W, 48))
        pygame.draw.line(screen, G3, (0, 48), (W, 48))
        blink = int(wall * 2) % 2 == 0
        lcol = G1 if link_ok else (RED if blink else G4)
        pygame.draw.circle(screen, lcol, (28, 24), 8)
        screen.blit(txt(F["l"], "LINK OK" if link_ok else "NO LINK", lcol), (46, 9))
        arm_txt = "ARMED" if s["armed"] else "DISARMED"
        screen.blit(txt(F["l"], arm_txt, RED if s["armed"] else G2), (230, 9))
        screen.blit(txt(F["l"], f"MODE {s['mode']}", AMBER), (420, 9))
        att = s["rates"].get("ATTITUDE", 0.0)
        att_col = RED if att < 5 else AMBER if att < 15 else G1
        screen.blit(txt(F["s"], f"ATT {att:4.1f} Hz | TOT {s['rate_total']:5.1f}/s | {s['port'] if not demo else 'DEMO'}", att_col), (700, 6))
        worst = max(ft) * 1000 if ft else 0.0
        screen.blit(txt(F["s"], f"FPS {clock.get_fps():3.0f} | WORST {worst:3.0f}ms | LAG {delay * 1000:3.0f}ms | VSYNC {'ON' if vsync_ok else 'off'}", G2), (700, 26))
        screen.blit(txt(F["m"], time.strftime("%H:%M:%S"), G1), (W - 100, 14))

        # --- panel utama
        hz = pygame.Rect(24, 64, 500, 330)
        draw_horizon(screen, hz, disp["roll"], disp["pitch"], disp["heading"])
        draw_compass(screen, pygame.Rect(540, 64, 260, 262), disp["heading"])

        # blok angka
        nb = pygame.Rect(540, 340, 260, 356)
        panel(screen, nb, "FLIGHT DATA")
        fix_txt = {0: "NO GPS", 1: "NO FIX", 2: "2D", 3: "3D", 4: "DGPS", 5: "RTK-F", 6: "RTK"}.get(s["fix"], str(s["fix"]))
        batt = s["batt"]
        bcol = G1 if batt > 40 or batt < 0 else AMBER if batt > 20 else RED
        rows = [
            ("ALT", f"{disp['alt']:7.1f} m", G1),
            ("CLIMB", f"{disp['climb']:+6.1f} m/s", G1),
            ("GSPD", f"{disp['gs']:6.1f} m/s", G1),
            ("ASPD", f"{disp['as']:6.1f} m/s" if s["as"] is not None else "-- m/s", G1),
            ("THR", f"{disp['thr']:5.0f} %", G1),
            ("BATT V", f"{disp['volt']:.2f} V" if s["volt"] is not None else "-- V", bcol),
            ("BATT %", f"{batt} %" if batt >= 0 else "--", bcol),
            ("CURR", f"{disp['curr']:.1f} A" if s["curr"] is not None else "-- A", G1),
            ("GPS", f"{fix_txt} {s['sats']}sv", G1 if s["fix"] >= 3 else AMBER),
            ("LAT", f"{s['lat']:.6f}", G1),
            ("LON", f"{s['lon']:.6f}", G1),
            ("MAX ALT", f"{max_alt:.1f} m", G2),
            ("MAX SPD", f"{max_spd:.1f} m/s", G2),
            ("FLIGHT", fmt_time(flight_s), G2),
        ]
        for i, (lab, val, col) in enumerate(rows):
            kv(screen, nb.x + 16, nb.y + 30 + i * 22, nb.w - 32, lab, val, col)

        # grafik
        draw_graph(screen, pygame.Rect(24, 410, 500, 140), "ROLL / PITCH", [
            {"buf": hist["roll"], "color": G1, "lo": -45, "hi": 45, "zero": True,
             "text": f"ROLL {disp['roll']:+6.1f}"},
            {"buf": hist["pitch"], "color": PALE, "lo": -45, "hi": 45,
             "text": f"PITCH {disp['pitch']:+6.1f}"},
        ], now)
        draw_graph(screen, pygame.Rect(24, 556, 500, 140), "ALTITUDE / SPEED", [
            {"buf": hist["alt"], "color": G1, "min_span": 5.0, "text": f"ALT {disp['alt']:6.1f}m"},
            {"buf": hist["gs"], "color": PALE, "min_span": 2.0, "text": f"GS {disp['gs']:5.1f}m/s"},
        ], now)

        # kolom kanan
        draw_map(screen, pygame.Rect(816, 64, 440, 260), trail, cur_xy, disp["heading"],
                 s["fix"], s["sats"], wall)
        draw_console(screen, pygame.Rect(816, 340, 440, 170))
        draw_rc(screen, pygame.Rect(816, 526, 440, 170), s["rc"])

        # footer
        screen.blit(txt(F["s"], "ESC keluar | F12 screenshot | L rekam CSV" + (" [REC]" if log_file else "") +
                        " | R reset | T scanline | G glow | F cap | S SS | M pesan | UP/DOWN/A tunda", G3), (24, 702))
        screen.blit(txt(F["s"], f"cap:{cap if cap else 'bebas'} SS:{SS} glow:{'ON' if glow_on else 'off'}", G3),
                    (W - 250, 702))

        # --- efek film
        if glow_on:
            try:
                if bloom_surf is None or frame_i % 2 == 0:   # bloom diperbarui tiap 2 frame
                    small = pygame.transform.smoothscale(screen, (W // 8, H // 8))
                    small.fill((110, 110, 110), special_flags=pygame.BLEND_RGB_MULT)
                    bloom_surf = pygame.transform.smoothscale(small, (W, H))
                screen.blit(bloom_surf, (0, 0), special_flags=pygame.BLEND_RGB_ADD)
            except Exception:  # noqa: BLE001
                glow_on = False
        if scan_on:
            screen.blit(scan, (0, 0), special_flags=pygame.BLEND_RGB_MULT)

        # --- intro "boot" singkat
        age = now - t_start
        if age < 1.9:
            alpha = 255 if age < 0.9 else int(255 * (1 - (age - 0.9) / 1.0))
            intro.set_alpha(max(0, alpha))
            screen.blit(intro, (0, 0))
            lines = ["PIXHAWK GROUND STATION", "> INITIALIZING TELEMETRY LINK ...", "> LOADING AVIONICS DISPLAY ..."]
            shown = int(age * 60)
            y = H // 2 - 50
            for i, ln in enumerate(lines):
                part = ln[:max(0, shown)]
                shown -= len(ln)
                if part:
                    img = txt(F["xl"] if i == 0 else F["m"], part, G1)
                    img.set_alpha(max(0, alpha))
                    screen.blit(img, (W // 2 - 260, y))
                y += 52 if i == 0 else 28

        # --- adaptif: matikan efek berat kalau frame lambat
        if raw > 0.028:
            slow += 1
        else:
            slow = max(0, slow - 1)
        if slow > 150 and glow_on:
            glow_on = False
            slow = 0
            log("Glow dimatikan otomatis (frame lambat). Tekan G untuk menyalakan.", 4)
        elif slow > 300 and SS == 2:
            SS = 1
            slow = 0
            log("Supersampling diturunkan ke 1 (frame lambat).", 4)

        frame_i += 1
        pygame.display.flip()
        if cap:
            clock.tick_busy_loop(cap)
        else:
            clock.tick()


if __name__ == "__main__":
    main()