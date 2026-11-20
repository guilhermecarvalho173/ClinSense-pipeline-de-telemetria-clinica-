Write-Host "[ClinSense] Derrubando contêineres, redes e volumes órfãos..." -ForegroundColor Cyan
docker compose down -v --remove-orphans
docker volume prune -f

Write-Host "`n=== Verificação: docker ps ===" -ForegroundColor Green
docker ps

Write-Host "`n=== Verificação: docker volume ls ===" -ForegroundColor Green
docker volume ls