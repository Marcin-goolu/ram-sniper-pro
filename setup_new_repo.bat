@echo off
chcp 65001 > nul
cls

echo ========================================================
echo    RAM SNIPER - KONFIGURACJA NOWEGO REPOZYTORIUM
echo ========================================================
echo.
echo Ten skrypt usunie stara historie Git i polaczy folder
echo z zupelnie nowym repozytorium na GitHubie.
echo.
echo Upewnij sie, ze utworzyles PUSTE repozytorium na GitHub!
echo.
pause

:: 1. Czyszczenie starego Gita
echo.
echo [1/5] Czyszczenie starej konfiguracji Git...
if exist ".git" (
    rmdir /s /q ".git"
    echo Usunieto stary folder .git. Czysta karta!
) else (
    echo Folder byl juz czysty.
)

:: 2. Inicjalizacja nowego
echo.
echo [2/5] Inicjalizacja nowego repozytorium...
git init
git branch -M main

:: 3. Konfiguracja użytkownika (aby nie pytal)
git config user.email "bot@ram-sniper.local"
git config user.name "RAM Sniper Bot"

:: 4. Dodawanie plików (z uwzględnieniem .gitignore)
echo.
echo [3/5] Dodawanie plikow...
git add .
git commit -m "Start projektu: Czysta instalacja"

:: 5. Podłączanie pod URL
echo.
echo ========================================================
echo WKLEJ TERAZ LINK DO SWOJEGO NOWEGO REPOZYTORIUM
echo (np. https://github.com/User/repo.git)
echo Kliknij prawym przyciskiem myszy, aby wkleic.
echo ========================================================
set /p repo_url="Link: "

echo.
echo [4/5] Laczenie z: %repo_url%
git remote add origin %repo_url%

:: 6. Pierwsze wysłanie
echo.
echo [5/5] Wysylanie plikow na serwer...
git push -u origin main

echo.
echo ========================================================
if %errorlevel% equ 0 (
    echo    SUKCES! REPOZYTORIUM PODLACZONE.
    echo.
    echo    Od teraz uzywaj pliku 'publish_report.bat' 
    echo    do codziennej aktualizacji cen.
    echo.
    echo    PAMIETAJ: Wejdz teraz na GitHub w Settings - Pages
    echo    i wlacz hosting z galezi 'main'.
) else (
    echo    WYSTAPIL BLAD. Sprawdz poprawnosc linku.
)
echo ========================================================
pause