#!/bin/bash
# Parallel value dimension backfill
# Splits the date range into chunks and runs them concurrently

cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate

echo "Starting parallel value dimension backfill..."
echo "Running 4 parallel processes for different date ranges"
echo ""

# Run 4 parallel processes for different year ranges
# Use python -u for unbuffered output so logs appear in real-time
python -u historical_dimensions_backfill.py --dimension value --no-resume --start-date 2021-06-01 --end-date 2022-06-30 --frequency monthly > /tmp/value_backfill_1.log 2>&1 &
PID1=$!
echo "Started 2021-06 to 2022-06 (PID: $PID1)"

python -u historical_dimensions_backfill.py --dimension value --no-resume --start-date 2022-07-01 --end-date 2023-06-30 --frequency monthly > /tmp/value_backfill_2.log 2>&1 &
PID2=$!
echo "Started 2022-07 to 2023-06 (PID: $PID2)"

python -u historical_dimensions_backfill.py --dimension value --no-resume --start-date 2023-07-01 --end-date 2024-06-30 --frequency monthly > /tmp/value_backfill_3.log 2>&1 &
PID3=$!
echo "Started 2023-07 to 2024-06 (PID: $PID3)"

python -u historical_dimensions_backfill.py --dimension value --no-resume --start-date 2024-07-01 --end-date 2026-02-28 --frequency monthly > /tmp/value_backfill_4.log 2>&1 &
PID4=$!
echo "Started 2024-07 to 2026-02 (PID: $PID4)"

echo ""
echo "All processes started. Monitor with:"
echo "  tail -f /tmp/value_backfill_*.log"
echo ""
echo "Or check progress:"
echo "  grep -h 'calculated' /tmp/value_backfill_*.log | tail -20"
echo ""
echo "Waiting for all processes to complete..."

wait $PID1 $PID2 $PID3 $PID4

echo ""
echo "All processes completed!"
echo ""
echo "Summary from each process:"
grep -h "Total time\|Calculated:" /tmp/value_backfill_*.log
