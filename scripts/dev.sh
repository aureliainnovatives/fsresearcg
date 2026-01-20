
#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  up) docker compose -f infra/docker-compose.yml up -d ;;
  down) docker compose -f infra/docker-compose.yml down -v ;;
  logs) docker compose -f infra/docker-compose.yml logs -f ;;
  api) docker compose -f infra/docker-compose.yml exec api-gateway bash ;;
  spark) docker compose -f infra/docker-compose.yml exec spark bash ;;
  *)
    echo "Usage: scripts/dev.sh {up|down|logs|api|spark}"
    ;;
esac
