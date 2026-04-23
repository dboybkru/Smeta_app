@echo off
echo ========================================
echo    Обновление бэкенда smeta_app
echo ========================================

echo [1/3] Загрузка файлов на сервер...
scp D:\git\ChGPT\smeta_app\backend\app.py root@168.222.140.21:/var/www/smeta_app/backend/
scp D:\git\ChGPT\smeta_app\backend\crud.py root@168.222.140.21:/var/www/smeta_app/backend/
scp D:\git\ChGPT\smeta_app\backend\models.py root@168.222.140.21:/var/www/smeta_app/backend/
if errorlevel 1 (
    echo ОШИБКА: Загрузка не удалась!
    pause
    exit /b 1
)

echo [2/3] Перезапуск сервиса на сервере...
ssh root@168.222.140.21 "systemctl restart smeta"
if errorlevel 1 (
    echo ОШИБКА: Перезапуск не удался!
    pause
    exit /b 1
)

echo [3/3] Готово!
echo ========================================
pause
