#!/usr/bin/env python3
"""Generate synthetic NIBE LOG files for missing days Jan 19 – Feb 17."""
import os, math, random
from datetime import date

OUTPUT_DIR = "/Users/patman/dev/data/sample/nibe"
TICKS_PER_DAY = 17280  # 24h * 3600s / 5s

HEADER = "Divisors\t\t1\t1\t10\t10\t10\t10\t10\t10\t100\t1\t10\t10\t10\t1\t1\t10\t1\t1\t1\t1\t10\t10\t10\t10\t10\t10\t10\t10\t10\t10\t10\t10\t1"
COLS   = ("Date\tTime\tversion(43001)\tR-version(44331)\tBT1(40004)\tBT6(40014)\t"
          "BT7(40013)\tBT25(40071)\tBT71(40152)\tBT63(40121)\tTot.Int.Add(43084)\t"
          "Alarm number(45001)\tCalc. Supply(43009)\tDegree Minutes(40940)\t"
          "BT1-Average(40067)\tRelays PCA-Base(43514)\tcompr. freq. calc(44775)\t"
          "compr. freq. act(44701)\tcompr. state(44457)\tcompr. protection mode(44702)\t"
          "GP12-speed(44396)\tGP10-on/off(43189)\tEB101-BT3(44055)\tEB101-BT12(44058)\t"
          "EB101-BT14(44059)\tEB101-BT15(44060)\tEB101-BT16(44363)\tEB101-BT17(44061)\t"
          "EB101-BT28(44362)\tEB101-BP4(44699)\tEB101-BP4-raw(44698)\t"
          "Saturation temp.(34000)\tSat. temp. sent(34001)\t"
          "EB101-low pressure sensor(44700)\tPrio(43086)")

# (outdoor_avg_c, outdoor_amp_c, is_post_fix)
DAYS_CONFIG = {
    "2026-01-19": (-3.0, 4.0, False),
    "2026-01-20": (-1.5, 3.5, False),
    "2026-01-21": (+1.0, 3.0, False),
    "2026-01-22": (+0.5, 3.0, False),
    "2026-01-23": (-2.0, 3.5, False),
    "2026-01-24": (-5.0, 3.5, False),
    "2026-01-25": (-7.0, 4.0, False),
    "2026-01-26": (-9.0, 4.5, False),
    "2026-01-27": (-8.0, 4.0, False),
    "2026-01-28": (-6.0, 3.5, False),
    "2026-01-29": (-4.0, 3.0, False),
    "2026-01-30": (-3.0, 3.5, False),
    "2026-01-31": (-5.0, 4.0, False),
    "2026-02-01": (-4.0, 3.5, False),
    "2026-02-02": (-2.0, 3.0, False),
    "2026-02-03": (-3.0, 3.5, False),
    "2026-02-04": (-5.0, 4.0, False),
    "2026-02-05": (-7.5, 4.5, False),
    "2026-02-06": (-6.0, 4.0, False),
    "2026-02-07": (-4.0, 3.5, False),
    "2026-02-08": (-3.5, 3.0, False),
    "2026-02-09": (-5.0, 3.5, False),
    "2026-02-10": (-7.0, 4.0, False),
    "2026-02-11": (-8.5, 4.5, False),
    "2026-02-12": (-7.0, 4.0, True),
    "2026-02-13": (-9.0, 4.5, True),
    "2026-02-14": (-10.0, 4.5, True),
    "2026-02-15": (-11.0, 5.0, True),
    "2026-02-16": (-10.5, 4.5, True),
    "2026-02-17": (-8.0, 4.0, True),
}


def get_filename(date_str):
    d = date.fromisoformat(date_str)
    if d.month == 1:
        return f"1104{d.day:02d}01.LOG"
    else:
        return f"2602{d.day:02d}01.LOG"


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


class DaySimulator:
    def __init__(self, date_str, outdoor_avg, outdoor_amp, is_post_fix, seed):
        self.date_str = date_str
        self.outdoor_avg = outdoor_avg
        self.outdoor_amp = outdoor_amp
        self.is_post_fix = is_post_fix
        self.rng = random.Random(seed)

        cold = max(0.0, -outdoor_avg)

        # DM dynamics (raw per 60s period)
        # Post-fix DM calibrated against real Feb duty cycles:
        #   +1°C outdoor → 55% duty: dm_loss=12, dm_gain=10 → 12/(12+10)=54.5% ✓
        #   −7°C outdoor → 67% duty: dm_loss=20, dm_gain=10 → 20/(20+10)=66.7% ✓
        # Run cycle length: range(290)/dm_gain = 290/10 = 29 min (realistic).
        if is_post_fix:
            self.dm_loss_per_min = int(12 + cold * 1.2)  # good flow, demand-driven
            self.dm_gain_per_min = 10
            self.dt_raw_nominal  = 28  # 2.8K ΔT
        else:
            self.dm_loss_per_min = int(10 + cold * 0.5)  # restricted flow
            self.dm_gain_per_min = 4
            self.dt_raw_nominal  = 65  # 6.5K ΔT

        # Starting DM
        if is_post_fix:
            self.dm = -self.rng.randint(150, 500)
        elif outdoor_avg > 0:
            self.dm = -self.rng.randint(800, 2500)
        elif outdoor_avg > -5:
            self.dm = -self.rng.randint(2500, 4500)
        else:
            self.dm = -self.rng.randint(4000, 5400)

        self.dm_timer = self.rng.randint(0, 11)  # desync across days

        # Temperatures (float, rounded for output)
        self.bt25 = 260.0
        self.bt71 = 250.0
        self.bt14 = 490.0
        self.bt16 = 45.0
        # bt17 (suction-gas) uses a regime model: ~10-15 min episodes of
        # negative or positive superheat rather than per-tick noise.
        # Pre-fix: ~30% of running-time in negative-SH regime → ~28% neg-SH overall.
        # Post-fix: always positive-SH regime.
        self.bt17 = float(outdoor_avg * 10 + 30)   # idle start near outdoor+3°C
        self._sh_regime      = self._new_sh_regime()
        self._sh_regime_ticks    = 0
        self._sh_regime_interval = self.rng.randint(100, 200)
        self.bp4  = 148.0
        self.bt1_avg = outdoor_avg * 10

        # BT7 (hot water tank top)
        self.bt7 = 455.0 if is_post_fix else 432.0

        # Compressor state
        self.running  = self.dm < -300
        self.freq     = 40 if self.running else 0
        self.run_ticks = 0

        # Alarm / defrost
        self.alarm              = 0
        self.defrost_remaining  = 0
        self.defrost_interval   = (self.rng.randint(1400, 2200) if is_post_fix
                                   else self.rng.randint(700, 1100))
        self.ticks_since_defrost = self.rng.randint(0, self.defrost_interval)

        # Lockout after stop / defrost
        self.lockout = 0

        # Heater
        self.heater = 0

    def _new_sh_regime(self):
        """Return superheat offset (raw delta from bt16) for next episode.

        Negative-SH probability is temperature-driven (colder = more slugging),
        calibrated against real Jan 12-18 and Feb 18-28 data:
          Pre-fix  at −7°C  → ~15% prob  → ~12.7% neg_sh_pct  (matches Jan 12)
          Pre-fix  at −10°C → ~37% prob  → ~31.7% neg_sh_pct  (matches Jan 14)
          Post-fix at +1°C  → ~9%  prob  → ~7.7%  neg_sh_pct  (matches Feb 28)
          Post-fix at −8°C  → ~24% prob  → ~20.5% neg_sh_pct  (matches Feb 18)
        """
        outdoor_c = self.outdoor_avg  # daily average °C

        if self.is_post_fix:
            # neg_prob: 10% at 0°C, rises ~1.4% per °C colder
            neg_prob = min(0.35, 0.10 + max(0.0, -outdoor_c) * 0.014)
            if self.rng.random() < neg_prob:
                return int(self.rng.gauss(-15, 10))   # minor liquid slugging
            else:
                # Positive SH: slightly higher at milder outdoor (lower load).
                # Base 90 raw (9K) compensates for startup-transient drag
                # that pulls the running average below the regime mean.
                sh_mean = int(clamp(
                    90 + max(0.0, outdoor_c * 10 + 77) * 0.44,
                    65, 115,
                ))
                return int(self.rng.gauss(sh_mean, 12))
        else:
            # Pre-fix: base 15% at −7°C, rises ~7.3% per °C below −7°C
            neg_prob = min(0.48, 0.15 + max(0.0, -(outdoor_c + 7)) * 0.073)
            if self.rng.random() < neg_prob:
                return int(self.rng.gauss(-20, 12))  # liquid slugging
            else:
                return int(self.rng.gauss(110, 12))   # normal positive SH

    # ------------------------------------------------------------------ #
    def outdoor_raw(self, tick):
        """BT1 raw at tick. Cold minimum at 06:00 (tick 4320), warm max at 14:00."""
        angle = 2 * math.pi * (tick - 4320) / TICKS_PER_DAY
        val   = self.outdoor_avg + self.outdoor_amp * math.sin(angle)
        return int(val * 10 + self.rng.uniform(-1.5, 1.5))

    def target_freq(self, bt1_raw):
        outdoor = bt1_raw / 10.0
        if   outdoor >  2: return self.rng.randint(28, 40)
        elif outdoor > -2: return self.rng.randint(38, 52)
        elif outdoor > -6: return self.rng.randint(50, 66)
        elif outdoor > -10: return self.rng.randint(63, 76)
        else:               return self.rng.randint(73, 82)

    # ------------------------------------------------------------------ #
    def step(self, tick):
        bt1 = self.outdoor_raw(tick)

        # --- DM update (every 60 s = 12 ticks) ---
        self.dm_timer += 1
        if self.dm_timer >= 12:
            self.dm_timer = 0
            if self.running:
                delta = self.dm_gain_per_min + self.rng.randint(-3, 3)
            elif self.alarm == 183:
                delta = -(self.dm_loss_per_min // 2)
            else:
                delta = -(self.dm_loss_per_min + self.rng.randint(-1, 2))
            self.dm = clamp(self.dm + delta, -5400, 50)

        # --- Defrost timing ---
        self.ticks_since_defrost += 1

        # --- State machine ---
        if self.defrost_remaining > 0:
            # In defrost
            self.alarm   = 183
            self.running = False
            self.freq    = max(0, self.freq - 5)
            self.defrost_remaining -= 1
            if self.defrost_remaining == 0:
                self.alarm              = 0
                self.ticks_since_defrost = 0
                self.dm = clamp(self.dm + self.rng.randint(50, 180), -5400, 50)
                self.lockout = 30  # 2.5-min min-off after defrost

        elif self.lockout > 0:
            self.lockout -= 1
            self.running  = False

        else:
            self.alarm = 0
            start_thresh = -350 if self.is_post_fix else -450
            stop_thresh  = -60  if self.is_post_fix else -80

            if not self.running and self.dm < start_thresh:
                self.running   = True
                self.run_ticks = 0
            elif self.running and self.dm > stop_thresh:
                self.running = False
                self.lockout = 72  # 6-min min-off
                self.freq    = self.freq  # keep for ramp-down

            # Trigger defrost?
            if self.ticks_since_defrost >= self.defrost_interval:
                dur = self.rng.randint(100, 144)  # 8-12 min
                self.defrost_remaining   = dur
                self.defrost_interval    = (self.rng.randint(1400, 2200) if self.is_post_fix
                                            else self.rng.randint(700, 1100))
                self.running = False

        # --- Frequency ramp ---
        if self.running:
            self.run_ticks += 1
            tf = self.target_freq(bt1)
            if self.freq < tf:
                self.freq = min(tf, self.freq + 2)
            else:
                self.freq = max(tf, self.freq - 1)
        else:
            self.freq = max(0, self.freq - 4)

        # --- Heater (pre-fix only) ---
        if not self.is_post_fix:
            if   self.dm <= -5000: self.heater = 600
            elif self.dm <= -4200: self.heater = 300
            else:                  self.heater = 0
        else:
            self.heater = 0

        # --- Temperature evolution ---
        α_fast = 0.08
        α_slow = 0.015

        if self.running:
            α = 0.05
            t_bt25 = clamp(270 + max(0, -bt1 * 0.22), 240, 310)
            ff     = max(0.5, self.freq / 65.0)
            dt_raw = int(self.dt_raw_nominal * ff)
            t_bt71 = t_bt25 - dt_raw
            t_bt14 = clamp(550 + int(self.freq * 1.1), 530, 720)
            if self.is_post_fix:
                # Post-fix: proper EEV operation, evap ≈ outdoor − 5°C.
                # Calibrated: at −8°C outdoor → bt16_run ≈ −13°C (real: −13.4°C)
                #             at +1°C outdoor → bt16_run ≈ −4°C  (real: −3.6°C)
                t_bt16 = clamp(bt1 - 50, -170, -10)
            else:
                # Pre-fix: liquid flooding → cold evaporator ≈ outdoor − 9°C.
                # Calibrated: at −7°C outdoor → bt16_run ≈ −16°C (real: −16.1°C)
                #             at −10°C outdoor → bt16_run ≈ −19°C (real: −19.1°C)
                t_bt16 = clamp(bt1 - 90, -220, -100)
            t_bp4  = clamp(148 + int(self.bt25 * 0.04), 140, 185)
            lp_base= clamp(int(70 + bt1 / 5) - 20, 30, 65)
        else:
            α = α_slow
            t_bt25 = clamp(252 + max(0, -bt1 * 0.15), 230, 295)
            t_bt71 = t_bt25 - clamp(10 - int(self.dm * 0.002), 5, 30)
            if self.alarm == 183:
                t_bt14 = self.bt14 + 15  # discharge cools after stop
                t_bt16 = clamp(self.bt16 + 50, -50, 200)  # evap warms during defrost
            else:
                t_bt14 = clamp(450 + int(self.bt25 * 0.06), 420, 520)
                t_bt16 = clamp(int(bt1 * 0.8 + 25), -60, 80)
            t_bp4  = clamp(138 + int(self.bt25 * 0.03), 130, 165)
            lp_base= clamp(int(70 + bt1 / 5), 45, 85)

        self.bt25 += (t_bt25 - self.bt25) * α
        self.bt71 += (t_bt71 - self.bt71) * α
        self.bt14 += (t_bt14 - self.bt14) * α_fast
        self.bt16 += (t_bt16 - self.bt16) * α_fast
        self.bp4  += (t_bp4  - self.bp4 ) * 0.04
        self.bt1_avg = self.bt1_avg * 0.999 + bt1 * 0.001

        # bt17 (suction-gas): regime-based episodes lasting 100-200 ticks (~8-17 min)
        if self.running:
            self._sh_regime_ticks += 1
            if self._sh_regime_ticks >= self._sh_regime_interval:
                self._sh_regime          = self._new_sh_regime()
                self._sh_regime_interval = self.rng.randint(100, 200)
                self._sh_regime_ticks    = 0
            bt17_target = float(int(round(self.bt16)) + self._sh_regime)
        else:
            bt17_target = float(bt1 + 30)   # idle: drift to outdoor + 3°C
        self.bt17 += (bt17_target - self.bt17) * α_fast

        # BT7 hot water tank — slow drift
        bt7_target = 455.0 if self.is_post_fix else 432.0
        self.bt7 += (bt7_target - self.bt7) * 0.001

        # --- Integer outputs ---
        bt25 = int(round(self.bt25))
        bt71 = int(round(self.bt71))
        bt14 = int(round(self.bt14))
        bt16 = int(round(self.bt16))
        bt17 = int(round(self.bt17))
        bp4  = int(round(self.bp4))
        bt7  = int(round(self.bt7))
        lp   = clamp(lp_base + self.rng.randint(-2, 2), 30, 90)
        bt1_avg_int = int(round(self.bt1_avg))

        # Derived sensors
        bt6  = clamp(int(391 + max(0, -bt1) * 0.04) + self.rng.randint(-2, 2), 385, 400)
        bt63 = bt25 + self.rng.randint(-3, 3)
        bt3  = bt71 + self.rng.randint(-5, 5)
        bt12 = bt71 + self.rng.randint(-8, 8)
        bt15 = bt71 + self.rng.randint(-5, 5)
        # bt28 = outdoor module sensor: slightly below outdoor when running (fan cooling effect)
        bt28_offset = -20 if self.running else -3
        bt28 = bt1 + bt28_offset + self.rng.randint(-5, 5)
        bp4_raw_col = int(bp4 * 2.42)

        calc_supply = clamp(int(285 + max(0, -bt1) * 0.31), 270, 340)

        if self.running:
            gp12 = clamp(int(26 + self.freq * 0.26), 0, 65)
        elif self.freq > 5:
            gp12 = clamp(int(self.freq * 0.26), 0, 40)
        else:
            gp12 = 0

        compr_state = 1 if self.running else 0
        freq_act    = self.freq * 10

        if self.running or self.heater > 0 or self.alarm == 183:
            relays = 6
        else:
            relays = 2

        prio = 30 if self.running else 10

        return [
            9542, 4,
            bt1, bt6, bt7, bt25, bt71, bt63,
            self.heater, self.alarm,
            calc_supply, self.dm, bt1_avg_int,
            relays, self.freq, freq_act, compr_state, 0,
            gp12, 1,
            bt3, bt12, bt14, bt15, bt16, bt17, bt28,
            bp4, bp4_raw_col,
            99, 99, lp, prio,
        ]


def generate_day(date_str, outdoor_avg, outdoor_amp, is_post_fix):
    seed = abs(hash(date_str)) % (2**31)
    sim  = DaySimulator(date_str, outdoor_avg, outdoor_amp, is_post_fix, seed)

    lines = [HEADER, COLS]
    for tick in range(TICKS_PER_DAY):
        total_sec = tick * 5
        h = total_sec // 3600
        m = (total_sec % 3600) // 60
        s = total_sec % 60
        time_str = f"{h:02d}:{m:02d}:{s:02d}"
        vals = sim.step(tick)
        lines.append(f"{date_str}\t{time_str}\t" + "\t".join(str(v) for v in vals))

    return "\n".join(lines) + "\n"


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for date_str, (outdoor_avg, outdoor_amp, is_post_fix) in DAYS_CONFIG.items():
        filename = get_filename(date_str)
        filepath = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(filepath):
            print(f"  skip  {filename}")
            continue
        tag = "post-fix" if is_post_fix else "pre-fix"
        print(f"  gen   {filename}  {outdoor_avg:+.1f}°C  {tag} ...", end="", flush=True)
        content = generate_day(date_str, outdoor_avg, outdoor_amp, is_post_fix)
        with open(filepath, "w") as f:
            f.write(content)
        print(f"  {len(content) // 1024} KB")
    print("Done.")


if __name__ == "__main__":
    main()
