#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Постраничная выгрузка таблицы b_crm_timeline в CSV и пересылка по SCP.
Совместим с Python 3.6.

Зависимости:
  pip install mysql-connector-python==8.0.*  paramiko scp python-dotenv

Добавьте в .env (рядом со скриптом) свои параметры подключения:

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=myuser
MYSQL_PASS=mypass
MYSQL_DB=mydb

# SCP (SSH)
SCP_HOST=remote.example.com
SCP_PORT=22
SCP_USER=backupbot
SCP_PASS=ssh_password      # или оставьте пустым и используйте ключ
SCP_KEY=~/.ssh/id_rsa       # необязательно: путь к private key
SCP_REMOTE_DIR=/data/backups/crm

# Рабочая директория скрипта
WORKDIR=/var/backups/crm          # здесь создаётся last_id.txt и временные CSV
BATCH_SIZE=5000                   # размер страницы
"""

import csv
import datetime as dt
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import mysql.connector as mysql
import paramiko
from scp import SCPClient

###############################################################################
# Настройки

ENV_PATH = '.env'
load_dotenv(dotenv_path=ENV_PATH, verbose=True, override=True)

WORKDIR      = Path(os.getenv("WORKDIR", ".")).expanduser()
LAST_ID_FILE = WORKDIR / "last_id.txt"
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", 5000))

MYSQL_CFG = dict(
    unix_socket="/var/lib/mysql/mysql.sock",
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASS"),
    database=os.getenv("MYSQL_DB"),
    charset="utf8mb4",
)

SCP_CFG = dict(
    hostname = os.getenv("SCP_HOST"),
    port     = int(os.getenv("SCP_PORT", 22)),
    username = os.getenv("SCP_USER"),
    password = os.getenv("SCP_PASS") or None,
    key_path = os.getenv("SCP_KEY") or None,
    remote_dir = os.getenv("SCP_REMOTE_DIR", "/tmp"),
)

###############################################################################
# Вспомогательные функции

def read_last_id() -> int:
    """Берём последний выгруженный ID из файла."""
    try:
        return int(LAST_ID_FILE.read_text().strip())
    except Exception:
        return 0  # файла нет – стартуем с 0

def write_last_id(last_id: int):
    LAST_ID_FILE.write_text(str(last_id))

def mysql_connection():
    return mysql.connect(**MYSQL_CFG)

def scp_client():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if SCP_CFG["key_path"]:
        ssh.connect(SCP_CFG["hostname"],
                    port=SCP_CFG["port"],
                    username=SCP_CFG["username"],
                    key_filename=os.path.expanduser(SCP_CFG["key_path"]))
    else:
        ssh.connect(SCP_CFG["hostname"],
                    port=SCP_CFG["port"],
                    username=SCP_CFG["username"],
                    password=SCP_CFG["password"])
    return SCPClient(ssh.get_transport())

###############################################################################
# Основная логика

def export():
    WORKDIR.mkdir(parents=True, exist_ok=True)

    last_id = read_last_id()
    print(f"[INFO] Starting from ID > {last_id}")

    # Имя временного файла
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = WORKDIR / f"b_crm_timeline_{ts}.csv"

    total_exported = 0
    new_last_id = last_id

    with mysql_connection() as conn, conn.cursor(dictionary=True, buffered=False) as cur, \
         csv_path.open("w", newline='', encoding="utf-8") as csvfile:

        writer = None  # создадим после первой пачки
        while True:
            cur.execute(
                """
                SELECT *
                  FROM b_crm_timeline
                 WHERE ID > %s
              ORDER BY ID
                 LIMIT %s
                """,
                (last_id, BATCH_SIZE)
            )
            rows = cur.fetchall()
            if not rows:
                break

            if writer is None:
                # Заголовки один раз
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                writer.writeheader()

            writer.writerows(rows)
            conn.commit()  # не обязателен при SELECT, но держим соединение «живым»

            # Готовимся к следующей странице
            last_id = rows[-1]["ID"]
            new_last_id = last_id
            total_exported += len(rows)
            print(f"[INFO]   Exported {total_exported} rows (ID → {new_last_id})")

    if total_exported == 0:
        print("[INFO] Нет новых строк – файл не создавался.")
        return

    # Передаём CSV по SCP
    print(f"[INFO] Uploading {csv_path.name} to {SCP_CFG['hostname']}:{SCP_CFG['remote_dir']}")
    with scp_client() as scp:
        scp.put(str(csv_path), remote_path=SCP_CFG["remote_dir"])

    # Обновляем last_id
    write_last_id(new_last_id)
    print(f"[INFO] Done. Last ID saved: {new_last_id}")

###############################################################################
# Точка входа

if __name__ == "__main__":
    try:
        export()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
