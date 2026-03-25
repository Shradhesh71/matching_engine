set -euo pipefail

SERVER1="http://127.0.0.1:3000"
SERVER2="http://127.0.0.1:3001"

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass()    { echo -e "${GREEN}  PASS${NC} $1"; }
fail()    { echo -e "${RED}  FAIL${NC} $1"; exit 1; }
section() { echo -e "\n${BLUE}━━━ $1 ━━━${NC}"; }
info()    { echo -e "${YELLOW}  $1${NC}"; }

# ── helpers ───────────────────────────────────────────────────────────────────

post_order() {
  local server=$1 side=$2 price=$3 qty=$4
  curl -sf -X POST "$server/orders" \
    -H "Content-Type: application/json" \
    -d "{\"side\":\"$side\",\"price\":$price,\"qty\":$qty}"
}

get_orderbook() {
  local server=$1
  curl -sf "$server/orderbook"
}

# Drain all resting orders at a given price by crossing them from the other side.
# Usage: drain_book  (submits large crossing orders to empty the whole book)
drain_book() {
  # Buy high enough to sweep all asks; sell low enough to sweep all bids
  post_order $SERVER1 buy  999999 99999 > /dev/null
  post_order $SERVER1 sell 1      99999 > /dev/null
}

# ── wait for servers ──────────────────────────────────────────────────────────

section "Waiting for servers to be ready"
for server in $SERVER1 $SERVER2; do
  for i in $(seq 1 20); do
    if curl -sf "$server/orderbook" > /dev/null 2>&1; then
      pass "$server is up"
      break
    fi
    if [ $i -eq 20 ]; then
      fail "$server did not come up in time"
    fi
    sleep 0.5
  done
done

# ── Test 1: order book starts empty ───────────────────────────────────────────

section "Test 1: empty order book"
ob=$(get_orderbook $SERVER1)
bids=$(echo "$ob" | grep -o '"bids":\[\]' || true)
asks=$(echo "$ob" | grep -o '"asks":\[\]' || true)
[ -n "$bids" ] && [ -n "$asks" ] && pass "order book is empty" || fail "expected empty book, got: $ob"

# ── Test 2: submit a resting buy ──────────────────────────────────────────────

section "Test 2: submit resting buy via Server 1"
resp=$(post_order $SERVER1 buy 100 10)
echo "  response: $resp"
order_id=$(echo "$resp" | grep -o '"order_id":[0-9]*' | grep -o '[0-9]*')
[ -n "$order_id" ] && pass "got order_id=$order_id" || fail "no order_id in response"

fills=$(echo "$resp" | grep -o '"fills":\[\]' || true)
[ -n "$fills" ] && pass "no fills (resting order)" || fail "expected no fills for unmatched buy"

# ── Test 3: order book reflects the resting buy ───────────────────────────────

section "Test 3: order book shows the resting bid"
ob=$(get_orderbook $SERVER1)
echo "  $ob"
echo "$ob" | grep -q '"price":100' && pass "bid at 100 visible" || fail "bid at 100 not found"
echo "$ob" | grep -q '"qty":10'    && pass "qty=10 correct"      || fail "qty=10 not found"

# ── Test 4: same book visible from Server 2 ───────────────────────────────────

section "Test 4: Server 2 sees the same order book (shared engine)"
ob2=$(get_orderbook $SERVER2)
echo "  $ob2"
echo "$ob2" | grep -q '"price":100' && pass "Server 2 sees bid at 100" || fail "Server 2 missing bid"
echo "$ob2" | grep -q '"qty":10'    && pass "Server 2 sees qty=10"     || fail "Server 2 wrong qty"

# ── Test 5: matching via Server 2 ─────────────────────────────────────────────

section "Test 5: submit matching sell via Server 2 — must produce a fill"
resp=$(post_order $SERVER2 sell 100 10)
echo "  response: $resp"
echo "$resp" | grep -q '"maker_order_id"' && pass "fill produced"  || fail "no fill in response"
echo "$resp" | grep -q '"price":100'      && pass "fill price=100" || fail "wrong fill price"
echo "$resp" | grep -q '"qty":10'         && pass "fill qty=10"    || fail "wrong fill qty"

maker=$(echo "$resp" | grep -o '"maker_order_id":[0-9]*' | grep -o '[0-9]*')
[ "$maker" = "$order_id" ] && pass "maker_order_id=$order_id correct" || fail "wrong maker id: got $maker expected $order_id"

# ── Test 6: book is empty after full match ────────────────────────────────────

section "Test 6: book is empty after full fill"
ob=$(get_orderbook $SERVER1)
echo "  $ob"
bids=$(echo "$ob" | grep -o '"bids":\[\]' || true)
asks=$(echo "$ob" | grep -o '"asks":\[\]' || true)
[ -n "$bids" ] && [ -n "$asks" ] && pass "book cleared after fill" || fail "book not empty: $ob"

# ── Test 7: partial fill ──────────────────────────────────────────────────────

section "Test 7: partial fill — taker larger than maker"
post_order $SERVER1 buy 200 5 > /dev/null    # maker: buy 5 @ 200
resp=$(post_order $SERVER2 sell 200 20)       # taker: sell 20 @ 200  → fill 5, rest 15 on ask
echo "  response: $resp"
echo "$resp" | grep -q '"qty":5' && pass "partial fill qty=5" || fail "wrong partial fill qty"

ob=$(get_orderbook $SERVER1)
echo "  book after partial: $ob"
echo "$ob" | grep -q '"qty":15' && pass "15 qty resting on ask side" || fail "expected 15 resting on asks"

# Clean up: cross the resting ask
post_order $SERVER1 buy 200 15 > /dev/null
info "cleared resting ask from test 7"

# ── Test 8: price-time priority ───────────────────────────────────────────────

section "Test 8: price-time priority — earliest order at same price fills first"

# Place two buys at same price from different servers
resp1=$(post_order $SERVER1 buy 150 5)
resp2=$(post_order $SERVER2 buy 150 5)
id1=$(echo "$resp1" | grep -o '"order_id":[0-9]*' | grep -o '[0-9]*')
id2=$(echo "$resp2" | grep -o '"order_id":[0-9]*' | grep -o '[0-9]*')
info "buy order ids: first=$id1, second=$id2"

# Sell 5 — should match id1 (arrived first, time priority)
resp=$(post_order $SERVER1 sell 150 5)
echo "  response: $resp"
maker=$(echo "$resp" | grep -o '"maker_order_id":[0-9]*' | grep -o '[0-9]*')
[ "$maker" = "$id1" ] && pass "earliest order filled first (id=$id1)" || fail "wrong maker: got $maker expected $id1"

# id2 is still resting at 150 — clear it so test 9 starts clean
post_order $SERVER1 sell 150 5 > /dev/null
info "cleared leftover bid from test 8"

# ── Test 9: no match when prices don't cross

section "Test 9: no match when bid < ask"

# Use prices that haven't appeared in any earlier test to avoid accidental crosses
post_order $SERVER1 buy  55 10 > /dev/null   # bid at 55
resp=$(post_order $SERVER2 sell 60 10)        # ask at 60 — bid(55) < ask(60), must NOT match
echo "  response: $resp"
fills=$(echo "$resp" | grep -o '"fills":\[\]' || true)
[ -n "$fills" ] && pass "no fill when bid=55 < ask=60" || fail "unexpected fill on non-crossing prices"

# Verify both orders are resting on the book
ob=$(get_orderbook $SERVER1)
echo "  book state: $ob"
echo "$ob" | grep -q '"price":55' && pass "bid at 55 resting" || fail "bid at 55 not found"
echo "$ob" | grep -q '"price":60' && pass "ask at 60 resting" || fail "ask at 60 not found"

# Clean up both resting orders
post_order $SERVER1 sell 55 10 > /dev/null   # clears bid at 55
post_order $SERVER1 buy  60 10 > /dev/null   # clears ask at 60
info "cleaned up resting orders from test 9"

# ── Test 10: no-double-match under concurrent submission 

section "Test 10: concurrent order submission — no double-match"
info "submitting 10 buys then 10 matching sells across both servers..."

# Place 10 resting buys at price 500 qty 1 each (sequentially to ensure they land)
for i in $(seq 1 10); do
  post_order $SERVER1 buy 500 1 > /dev/null
done

ob=$(get_orderbook $SERVER1)
info "book before concurrent sells: $ob"

# Fire 10 matching sells concurrently, alternating between both servers
TMPDIR_FILLS=$(mktemp -d)
for i in $(seq 1 10); do
  if [ $((i % 2)) -eq 0 ]; then
    post_order $SERVER1 sell 500 1 > "$TMPDIR_FILLS/$i" &
  else
    post_order $SERVER2 sell 500 1 > "$TMPDIR_FILLS/$i" &
  fi
done
wait

FILL_COUNT=0
for i in $(seq 1 10); do
  resp=$(cat "$TMPDIR_FILLS/$i")
  if echo "$resp" | grep -q '"maker_order_id"'; then
    FILL_COUNT=$((FILL_COUNT + 1))
  fi
done
rm -rf "$TMPDIR_FILLS"

info "fills produced: $FILL_COUNT / 10 expected"
[ "$FILL_COUNT" -eq 10 ] && pass "exactly 10 fills — no double-match, no missed match" \
                          || fail "expected 10 fills, got $FILL_COUNT"

ob=$(get_orderbook $SERVER1)
bids=$(echo "$ob" | grep -o '"bids":\[\]' || true)
[ -n "$bids" ] && pass "all bids consumed — book is clean" || fail "orphaned bids remain: $ob"

# ── Summary ──────────────────────────────────────────────────────────────────

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  All 10 tests passed.${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"



curl -X POST http://127.0.0.1:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"side":"buy","price":100,"qty":10}'

  curl -X POST http://127.0.0.1:3001/orders \
  -H "Content-Type: application/json" \
  -d '{"side":"sell","price":100,"qty":10}'

  curl http://127.0.0.1:3000/orderbook