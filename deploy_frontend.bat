@echo off
echo ========================================
echo    Обновление фронтенда smeta_app
echo ========================================

cd /d D:\git\ChGPT\smeta_app\frontend

echo [1/3] Сборка React приложения...
set REACT_APP_API_URL=http://168.222.140.21/api
call npm run build
if errorlevel 1 (
    echo ОШИБКА: Сборка не удалась!
    pause
    exit /b 1
)

echo [2/3] Загрузка на сервер...
scp -r build\* root@168.222.140.21:/var/www/smeta_app/frontend_build/
if errorlevel 1 (
    echo ОШИБКА: Загрузка не удалась!
    pause
    exit /b 1
)

echo [3/3] Готово!
echo Открой браузер: http://168.222.140.21
echo ========================================
pause
