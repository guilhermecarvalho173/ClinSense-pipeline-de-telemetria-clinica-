#!/usr/bin/env bash
set -euo pipefail

echo "[ClinSense] Derrubando contêineres, redes e volumes órfãos..."
docker compose down -v --remove-orphans
docker volume prune -f

echo "=== Verificação: docker ps ==="
docker ps

echo "=== Verificação: docker volume ls ==="
docker volume ls