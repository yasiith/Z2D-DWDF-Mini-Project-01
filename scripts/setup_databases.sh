#!/usr/bin/env bash
# ============================================================================
# setup_databases.sh — builds both databases from scratch.
#
#   1. creates abc_hub_operational + abc_hub_analytics
#   2. creates the operational schema and loads the 26 CSVs
#   3. runs every analytical DDL script in sql/02_analytics (in file order)
#
# Usage (Git Bash on Windows, or any Linux/macOS shell):
#   ./scripts/setup_databases.sh                    # full rebuild of both databases
#   ./scripts/setup_databases.sh --analytics-only   # (re)apply analytical DDL only
#   ./scripts/setup_databases.sh --reset-analytics  # drop + recreate the analytical DB
#                                                   # (then rebuild the NiFi flow so its
#                                                   #  extract watermarks reset too)
#
# Connection settings come from the standard libpq variables. Put the
# password in a pgpass file (never in this script):
#   Windows: %APPDATA%\postgresql\pgpass.conf   Linux/macOS: ~/.pgpass
#   line format:  localhost:5432:*:postgres:<password>
# ============================================================================
set -euo pipefail
shopt -s nullglob

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"
# Hide NOTICE chatter but keep WARNING/ERROR (errors still abort via ON_ERROR_STOP).
export PGOPTIONS="${PGOPTIONS:--c client_min_messages=warning}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PKG="$ROOT/ABC Hub Data Package/packages"
PSQL=(psql -w -X -v ON_ERROR_STOP=1 -q)

step() { printf '\n==> %s\n' "$*"; }

MODE="${1:-}"

if [[ "$MODE" == "--reset-analytics" ]]; then
  step "Dropping and recreating abc_hub_analytics"
  "${PSQL[@]}" -d postgres -c "DROP DATABASE IF EXISTS abc_hub_analytics WITH (FORCE)"
  "${PSQL[@]}" -d postgres -f "$ROOT/sql/00_setup/01_create_databases.sql" >/dev/null
fi

if [[ -z "$MODE" ]]; then
  step "Creating databases (if missing)"
  "${PSQL[@]}" -d postgres -f "$ROOT/sql/00_setup/01_create_databases.sql"

  step "Creating operational schema (drops and recreates schema public)"
  "${PSQL[@]}" -d abc_hub_operational -f "$PKG/ddl/operational_schema.sql"

  step "Loading 26 CSV files into abc_hub_operational"
  "${PSQL[@]}" -d abc_hub_operational -v datadir="$PKG/data" \
      -f "$ROOT/sql/00_setup/02_load_operational_data.sql"
fi

step "Building the analytical database (bronze / silver / gold)"
for f in "$ROOT"/sql/02_analytics/*.sql; do
  echo "    running $(basename "$f")"
  "${PSQL[@]}" -d abc_hub_analytics -f "$f"
done

step "Done. Row counts:"
"${PSQL[@]}" -d abc_hub_operational -Atc \
  "SELECT 'operational rows: ' || sum(n_live_tup) FROM pg_stat_user_tables WHERE schemaname = 'public'"
