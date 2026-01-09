@echo off
chcp 65001 > nul
echo ========================================================
echo    RAM SNIPER PARALLEL - AUTOMATYCZNA PUBLIKACJA
echo ========================================================
echo.

echo 1. Konfiguracja Git (Naprawa bledu Author identity)...
git config user.email "automat@ram-sniper.local"
git config user.name "RAM Sniper Bot"

echo.
echo 2. Generowanie nowego raportu...
python ram_sniper_parallel.py

echo.
echo 3. Przygotowanie plikow dla GitHub Pages...
copy /Y raport_targeted_parallel.html index.html > nul

echo.
echo 4. Wysylanie na serwer GitHub...
REM Tworzymy galaz main jesli nie istnieje
git checkout -B main

REM Dodajemy tylko istniejace pliki
git add index.html ceny_targeted_parallel.json

REM Commitujemy zmiany
git commit -m "Automatyczna aktualizacja raportu: %date% %time%"

REM Wysylamy na serwer
git push origin main

echo.
echo ========================================================
echo [SUKCES] Raport opublikowany!
echo Link: https://marcin-goolu.github.io/ram-sniper/
echo ========================================================
echo.
pause
