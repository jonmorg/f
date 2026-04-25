@echo off
setlocal

echo.
echo ================================================================
echo   Polymarket Value Betting Bot — Windows Setup
echo ================================================================
echo.

:: Check Python version
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Install Python 3.11+ from python.org
    pause
    exit /b 1
)

for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo Found Python %PYVER%

:: Create virtual environment
echo.
echo [1/4] Creating virtual environment...
python -m venv venv
if errorlevel 1 (
    echo ERROR: Could not create virtual environment.
    pause
    exit /b 1
)

:: Activate and install
echo [2/4] Activating venv and installing dependencies...
call venv\Scripts\activate.bat
pip install --upgrade pip --quiet
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: pip install failed. Check your internet connection.
    pause
    exit /b 1
)

:: Create .env if missing
echo [3/4] Checking .env file...
if not exist .env (
    copy .env.example .env >nul
    echo   Created .env from template — EDIT IT before running the bot!
) else (
    echo   .env already exists.
)

echo [4/4] Done.
echo.
echo ================================================================
echo   NEXT STEPS
echo ================================================================
echo.
echo  1. Edit .env and fill in:
echo       PRIVATE_KEY   — your Polygon wallet private key (0x...)
echo       POLYGON_RPC   — e.g. https://polygon-rpc.com
echo       TELEGRAM_TOKEN     — from @BotFather on Telegram
echo       TELEGRAM_CHAT_ID   — your Telegram chat/user ID
echo.
echo  2. Fund your wallet with USDC on Polygon (~$50).
echo     You also need a small amount of MATIC for gas (~$1 worth).
echo.
echo  3. Approve USDC for the Polymarket CLOB contract.
echo     Visit https://polymarket.com and deposit to your trading account.
echo.
echo  4. Start the bot:
echo       venv\Scripts\activate.bat
echo       python main.py
echo.
echo  Logs: polybot.log   Database: polybot.db
echo.
pause
