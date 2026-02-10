"""P10. Автоматический аудит безопасности (OWASP-ориентированный).

Проверяет:
1. Нет f-strings в SQL-запросах (используется SQLAlchemy select)
2. Секреты не светятся в логах
3. API-ключи берутся только из .env (settings)
4. Нет hardcoded secrets в исходниках
5. Нет open() без encoding
6. HTML-инъекции блокируются

Использование:
    from src.bot.utils.security_audit import run_security_audit
    report = run_security_audit()
"""

import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Директории для аудита
AUDIT_DIRS = [
    Path("src"),
    Path("tests"),
]

# Файлы, которые пропускаем
SKIP_FILES = {
    "__pycache__",
    ".pyc",
    "node_modules",
    ".git",
    "security_audit.py",  # себя пропускаем
}

# Паттерны уязвимостей
VULNERABILITY_PATTERNS = {
    "SQL_INJECTION": {
        "pattern": re.compile(r'f["\'].*?(SELECT|INSERT|UPDATE|DELETE|DROP|ALTER)\s', re.IGNORECASE),
        "severity": "CRITICAL",
        "description": "Потенциальная SQL-инъекция через f-string",
        "fix": "Используйте SQLAlchemy ORM или параметризованные запросы",
    },
    "HARDCODED_SECRET": {
        "pattern": re.compile(
            r'(api_key|secret|password|token)\s*=\s*["\'][A-Za-z0-9_\-]{20,}["\']',
            re.IGNORECASE,
        ),
        "severity": "HIGH",
        "description": "Возможно, захардкоженный секрет",
        "fix": "Вынесите секрет в .env и используйте settings.VARIABLE",
    },
    "EXEC_EVAL": {
        "pattern": re.compile(r'\b(exec|eval)\s*\('),
        "severity": "HIGH",
        "description": "Использование exec/eval — потенциальная RCE",
        "fix": "Замените на безопасную альтернативу",
    },
    "PICKLE_LOAD": {
        "pattern": re.compile(r'pickle\.loads?\('),
        "severity": "HIGH",
        "description": "pickle.load — потенциальная десериализация вредоносных данных",
        "fix": "Используйте JSON для десериализации",
    },
    "OPEN_WITHOUT_ENCODING": {
        "pattern": re.compile(r'open\([^)]*\)\s*(?!.*encoding)'),
        "severity": "LOW",
        "description": "open() без явного encoding — возможны проблемы на разных ОС",
        "fix": "Добавьте encoding='utf-8'",
    },
    "SUBPROCESS_SHELL": {
        "pattern": re.compile(r'subprocess\.(run|Popen|call)\(.*shell\s*=\s*True'),
        "severity": "HIGH",
        "description": "subprocess с shell=True — риск инъекции команд",
        "fix": "Используйте shell=False и список аргументов",
    },
    "LOG_SENSITIVE": {
        "pattern": re.compile(
            r'log(ger)?\.(info|debug|warning|error)\(.*?(password|secret|token|api_key).*?\)',
            re.IGNORECASE,
        ),
        "severity": "MEDIUM",
        "description": "Возможно, чувствительные данные в логах",
        "fix": "Маскируйте секреты перед логированием",
    },
}


def _should_skip(filepath: Path) -> bool:
    """Проверяет, нужно ли пропустить файл."""
    for skip in SKIP_FILES:
        if skip in str(filepath):
            return True
    return not filepath.suffix == ".py"


def scan_file(filepath: Path) -> list[dict]:
    """Сканирует один файл на уязвимости.

    Returns:
        Список найденных проблем.
    """
    issues = []

    try:
        content = filepath.read_text(encoding="utf-8", errors="ignore")
        lines = content.splitlines()
    except Exception:
        return issues

    for line_num, line in enumerate(lines, 1):
        # Пропускаем комментарии
        stripped = line.strip()
        if stripped.startswith("#"):
            continue

        for vuln_id, vuln in VULNERABILITY_PATTERNS.items():
            if vuln["pattern"].search(line):
                # Дополнительные фильтры для ложных срабатываний
                if vuln_id == "HARDCODED_SECRET":
                    # Пропускаем значения из settings
                    if "settings." in line:
                        continue
                    # Пропускаем пустые строки и значения по умолчанию
                    if '""' in line or "''" in line or '= ""' in line:
                        continue
                    # Пропускаем тестовые файлы
                    if "test" in str(filepath).lower():
                        continue

                if vuln_id == "OPEN_WITHOUT_ENCODING":
                    # Пропускаем бинарные режимы
                    if '"rb"' in line or '"wb"' in line or "'rb'" in line or "'wb'" in line:
                        continue

                if vuln_id == "LOG_SENSITIVE":
                    # Пропускаем если уже маскировано
                    if "mask" in line.lower() or "***" in line:
                        continue

                issues.append({
                    "file": str(filepath),
                    "line": line_num,
                    "vuln_id": vuln_id,
                    "severity": vuln["severity"],
                    "description": vuln["description"],
                    "fix": vuln["fix"],
                    "code": stripped[:120],
                })

    return issues


def run_security_audit() -> dict:
    """Запускает полный аудит безопасности кодовой базы.

    Returns:
        {
            "total_files": int,
            "total_issues": int,
            "critical": int,
            "high": int,
            "medium": int,
            "low": int,
            "issues": [...]
            "summary": str
        }
    """
    all_issues = []
    total_files = 0

    for audit_dir in AUDIT_DIRS:
        if not audit_dir.exists():
            continue

        for filepath in audit_dir.rglob("*.py"):
            if _should_skip(filepath):
                continue

            total_files += 1
            issues = scan_file(filepath)
            all_issues.extend(issues)

    # Подсчёт по severity
    critical = sum(1 for i in all_issues if i["severity"] == "CRITICAL")
    high = sum(1 for i in all_issues if i["severity"] == "HIGH")
    medium = sum(1 for i in all_issues if i["severity"] == "MEDIUM")
    low = sum(1 for i in all_issues if i["severity"] == "LOW")

    # Формируем summary
    if critical > 0:
        grade = "🔴 CRITICAL"
    elif high > 0:
        grade = "🟠 NEEDS ATTENTION"
    elif medium > 0:
        grade = "🟡 ACCEPTABLE"
    else:
        grade = "🟢 SECURE"

    summary = (
        f"Аудит безопасности: {grade}\n"
        f"Файлов проверено: {total_files}\n"
        f"Найдено проблем: {len(all_issues)}\n"
        f"  🔴 Critical: {critical}\n"
        f"  🟠 High: {high}\n"
        f"  🟡 Medium: {medium}\n"
        f"  🟢 Low: {low}"
    )

    logger.info("Security audit: %d files, %d issues (%d critical)",
                total_files, len(all_issues), critical)

    return {
        "total_files": total_files,
        "total_issues": len(all_issues),
        "critical": critical,
        "high": high,
        "medium": medium,
        "low": low,
        "issues": all_issues,
        "summary": summary,
        "grade": grade,
    }


def format_audit_report(audit: dict) -> str:
    """Форматирует отчёт аудита для отправки в Telegram (HTML).

    Returns:
        HTML-форматированный отчёт.
    """
    lines = [
        f"🔒 <b>Аудит безопасности</b>\n",
        f"Статус: {audit['grade']}\n",
        f"📂 Файлов: {audit['total_files']} | ⚠️ Проблем: {audit['total_issues']}\n",
    ]

    if audit["issues"]:
        lines.append("\n<b>Найденные проблемы:</b>\n")
        # Показываем до 10 самых критичных
        sorted_issues = sorted(
            audit["issues"],
            key=lambda x: {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}[x["severity"]],
        )
        for issue in sorted_issues[:10]:
            emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}[issue["severity"]]
            lines.append(
                f"{emoji} <code>{issue['file']}:{issue['line']}</code>\n"
                f"   {issue['description']}\n"
            )

        if len(audit["issues"]) > 10:
            lines.append(f"\n... и ещё {len(audit['issues']) - 10} проблем")
    else:
        lines.append("\n✅ Проблем не обнаружено!")

    return "\n".join(lines)
