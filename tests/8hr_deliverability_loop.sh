#!/bin/bash
# 8-hour continuous deliverability loop: text/voice/image DMs + group (text/voice/image)
# + maluca channel broadcasts, across all 5 attached devices (BPF+VHF on 144, T3S3+XIAO+Pager
# on 915 -> cross-band via MQTT). Tuned for the tbeams (small payloads, 10x timing, retransmits).
# Runs the full test back-to-back until the deadline, accumulating per-run results, then writes
# a final aggregated report. Resilient: each run rediscovers whichever devices are live.
set +e
RESULTS=/tmp/8hr_deliverability
mkdir -p "$RESULTS"
cd ~/MeshReliable/meshreliable-firmware || exit 1
START=$(date +%s)
DEADLINE=$(( START + 8*3600 ))
RUN=0
SUMMARY="$RESULTS/summary.txt"
REPORT="$RESULTS/FINAL_REPORT.md"
echo "8-HOUR DELIVERABILITY LOOP — started $(date)" > "$SUMMARY"

cleanup(){
  pkill -9 -f "meshtastic" 2>/dev/null
  pkill -9 -f full_7device_test 2>/dev/null
  for p in /dev/cu.usbmodem*; do lsof -t "$p" 2>/dev/null | xargs -r kill -9 2>/dev/null; done
  sleep 3
}

aggregate(){
  # Build the rolling FINAL_REPORT.md from all per-run logs.
  python3 - "$RESULTS" "$START" <<'PY'
import sys, glob, re, os, time
res, start = sys.argv[1], int(sys.argv[2])
cats = ["text_dm","voice_dm","image_dm","group_text","group_voice","group_image","channel"]
logs = sorted(glob.glob(os.path.join(res,"run*_*.log")))
rows=[]; totals={c:[0,0] for c in cats}
for lg in logs:
    txt=open(lg,encoding="utf-8",errors="replace").read()
    txt=re.sub(r'\x1b\[[0-9;]*m','',txt)
    run={}
    for c in cats:
        # last reconciled "cat : X/Y (Z%)" for this run
        m=re.findall(rf'{c}\s+:\s+(\d+)/(\d+)\s+\(([0-9.]+)%\)', txt)
        if m:
            ok,tot,pct=m[-1]; run[c]=(int(ok),int(tot),float(pct))
            totals[c][0]+=int(ok); totals[c][1]+=int(tot)
    lost=len(re.findall(r'serial LOST', txt))
    rb=len(re.findall(r'Booted, wake cause', txt))
    rows.append((os.path.basename(lg),run,lost))
lines=[]
lines.append("# MeshReliable — 8-Hour Continuous Deliverability Report\n")
lines.append(f"_Generated {time.strftime('%Y-%m-%d %H:%M:%S')} — {len(logs)} runs, "
             f"{(int(time.time())-start)//60} min elapsed._\n")
lines.append("## Aggregate deliverability (all runs combined)\n")
lines.append("| Category | Delivered | Total | % |")
lines.append("|---|---|---|---|")
order=[("text_dm","Text DM"),("voice_dm","Voice DM"),("image_dm","Image DM"),
       ("group_text","Group Text"),("group_voice","Group Voice"),("group_image","Group Image"),
       ("channel","Channel (maluca)")]
for c,label in order:
    ok,tot=totals[c]
    pct = (100*ok/tot) if tot else 0
    lines.append(f"| {label} | {ok} | {tot} | {pct:.1f}% |")
gok=sum(totals[c][0] for c in cats); gtot=sum(totals[c][1] for c in cats)
lines.append(f"| **OVERALL** | **{gok}** | **{gtot}** | **{(100*gok/gtot if gtot else 0):.1f}%** |\n")
lines.append("## Per-run results\n")
lines.append("| Run | Text DM | Voice DM | Image DM | Grp Text | Grp Voice | Grp Image | Channel | serialLOST |")
lines.append("|---|---|---|---|---|---|---|---|---|")
for name,run,lost in rows:
    def cell(c):
        if c in run: ok,tot,pct=run[c]; return f"{ok}/{tot}"
        return "—"
    n=re.sub(r'_\d+_.*','',name).replace('run','#')
    lines.append(f"| {n} | {cell('text_dm')} | {cell('voice_dm')} | {cell('image_dm')} | "
                 f"{cell('group_text')} | {cell('group_voice')} | {cell('group_image')} | "
                 f"{cell('channel')} | {lost} |")
open(os.path.join(res,"FINAL_REPORT.md"),"w").write("\n".join(lines)+"\n")
print("aggregated", len(logs), "runs")
PY
}

while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  RUN=$(( RUN + 1 ))
  TS=$(date +%Y%m%d_%H%M%S)
  LOG="$RESULTS/run${RUN}_${TS}.log"
  ELAPSED=$(( ( $(date +%s) - START ) / 60 ))
  echo "[$(date +%H:%M:%S)] starting run $RUN (elapsed ${ELAPSED}min)" >> "$SUMMARY"
  cleanup
  # Full test (all phases: DM text/voice/image, group text/voice/image, maluca channel),
  # all 5 devices, tuned. Hard-capped at 3h per run so a wedge can't stall the loop.
  GROUP_MEMBERS="BPF,VHF,T3S3,XIAO,Pager" SKIP_PORTS="/dev/cu.usbmodem_none" \
    MSGS_PER_PHASE=8 BAND_MULT=10 MEDIA_SIZE_CAP=30 MEDIA_MAX_ATTEMPTS=8 \
    GROUP_SEND_DELAY=12 MEDIA_SEND_DELAY=4 \
    python3 -u tests/full_7device_test.py > "$LOG" 2>&1 &
  PID=$!
  ( sleep 10800; kill -9 "$PID" 2>/dev/null ) & WD=$!
  wait "$PID" 2>/dev/null
  kill "$WD" 2>/dev/null; wait "$WD" 2>/dev/null
  # record this run's final per-category tallies
  {
    echo "  --- run $RUN results ---"
    for c in text_dm voice_dm image_dm group_text group_voice group_image channel; do
      v=$(grep -E "$c +:" "$LOG" | sed 's/\x1b\[[0-9;]*m//g' | tail -1)
      [ -n "$v" ] && echo "  $v"
    done
    echo "  serial_LOST=$(grep -cE 'serial LOST' "$LOG")"
  } >> "$SUMMARY"
  aggregate
  sleep 5
done
echo "" >> "$SUMMARY"
echo "8-HOUR LOOP COMPLETE — $RUN runs — $(date)" >> "$SUMMARY"
aggregate
echo "DONE" >> "$RESULTS/COMPLETE"
