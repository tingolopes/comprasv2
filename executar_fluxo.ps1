[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

Clear-Host
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "   INICIANDO FLUXO DE ATUALIZAÇÃO - IFMS EM NÚMEROS" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Início: $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss')"
Write-Host ""

# Lista de scripts na ordem lógica de dependência
$Scripts = @(
    "extrator_atas.py",                                     # 2. Busca Atas (ARP)
    "extrator_atas_itens_saldos_unidadesParticipantes.py",  # 4. Enriquece Atas (Itens/Saldos/Unidades)
    
    "extrator_compras.py",                                  # 3. Busca Compras (Legado/14133)
    "extrator_compras_itens.py",                            # 5. Enriquece Compras

    "gerar_banco_atas_itens_saldos.py",                     # 6. Gera os 4 bancos de Atas (Mestre/Itens/Saldos/Partic)
    "gerar_banco_compras.py",                               # 7. Gera Banco Compras
    "gerar_banco_itens.py"                                  # 8. Gera Itens Compras
)

$Falhas = @()

# O 'in' é obrigatório aqui para percorrer a lista
foreach ($Script in $Scripts) {
    if (-not (Test-Path $Script)) {
        Write-Host " [?] Script não encontrado: $Script" -ForegroundColor Gray
        continue
    }

    Write-Host ">>> Executando: $Script ..." -ForegroundColor Yellow
    
    # Executa o script python e aguarda finalização
    python $Script
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host " [!] FALHA no script $Script (Código: $LASTEXITCODE)" -ForegroundColor Red
        $Falhas += $Script
    }
    else {
        Write-Host " [+] SUCESSO: $Script" -ForegroundColor Green
    }
    Write-Host ("-" * 60)
}

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "           RESUMO DA OPERAÇÃO" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

if ($Falhas.Count -eq 0) {
    Write-Host " 🎉 TODOS OS SCRIPTS FINALIZADOS COM SUCESSO!" -ForegroundColor Green
}
else {
    Write-Host " ⚠️ ALGUNS SCRIPTS APRESENTARAM ERROS:" -ForegroundColor Yellow
    foreach ($Erro in $Falhas) { 
        Write-Host "  - $Erro" -ForegroundColor Red 
    }
}

Write-Host "Término: $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss')"
Write-Host "==========================================================" -ForegroundColor Cyan

# Stop-Computer -Force # Descomente esta linha se quiser desligar o PC ao terminar