@echo off
echo ========================================
echo    Обновление базы данных smeta_app
echo ========================================
echo ВНИМАНИЕ: База на сервере будет заменена!
echo Нажми Ctrl+C для отмены или любую клавишу для продолжения...
pause > nul

echo [1/2] Загрузка базы на сервер...
scp D:\git\ChGPT\smeta_app\backend\smeta.db root@168.222.140.21:/var/www/smeta_app/backend/
if errorlevel 1 (
    echo ОШИБКА: Загрузка не удалась!
    pause
    exit /b 1
)

echo [2/2] Готово!
echo ========================================
pause
