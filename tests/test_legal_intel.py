"""Тесты для плана «Юридический интеллект и Автоматизация производства».

L1: AI DocReview
L2: Smart Templates Wizard
L3: Legal Search Agent
L4: Conflict Check
L5: Multi-agent Brainstorm
L6: OSINT-lite (BIN Check)
L7: Ticket Manager
L8: Digital Case Storage (Mini App API)
L9: News Impact Analysis
L10: Deadline Assistant
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch


# ═══════════════════════════════════════════════════════════════════════════
#  L1: AI DocReview
# ═══════════════════════════════════════════════════════════════════════════


class TestDocReview:
    """L1: Анализ юридических рисков документов."""

    def test_import(self):
        from src.bot.utils.doc_review import (
            extract_text,
            analyze_legal_document,
            quick_doc_summary,
            extract_text_from_pdf,
            extract_text_from_docx,
        )

    @pytest.mark.asyncio
    async def test_extract_text_txt(self, tmp_path):
        """Извлечение текста из .txt файла."""
        from src.bot.utils.doc_review import extract_text

        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Договор аренды помещения\nСтороны:", encoding="utf-8")

        result = await extract_text(str(txt_file))
        assert "Договор" in result
        assert "Стороны" in result

    @pytest.mark.asyncio
    async def test_extract_text_unsupported(self):
        """Неподдерживаемый формат возвращает пустую строку."""
        from src.bot.utils.doc_review import extract_text
        result = await extract_text("/fake/file.xlsx")
        assert result == ""

    @pytest.mark.asyncio
    async def test_analyze_document_mock(self):
        """AI-анализ документа (mock AI)."""
        from src.bot.utils.doc_review import analyze_legal_document

        mock_response = (
            "🔴 <b>РИСК: Неограниченная ответственность</b>\n"
            "⚠️ Отсутствует пункт об ограничении ответственности"
        )

        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_ai:
            mock_ai.return_value.call_with_fallback = AsyncMock(return_value=mock_response)
            result = await analyze_legal_document("Текст договора...")
            assert "РИСК" in result or "риск" in result.lower() or mock_response in result

    @pytest.mark.asyncio
    async def test_analyze_with_question(self):
        """AI-анализ с дополнительным вопросом."""
        from src.bot.utils.doc_review import analyze_legal_document

        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_ai:
            mock_ai.return_value.call_with_fallback = AsyncMock(return_value="Анализ готов")
            result = await analyze_legal_document("Текст", user_question="Есть ли риски?")
            assert result == "Анализ готов"


# ═══════════════════════════════════════════════════════════════════════════
#  L2: Smart Templates Wizard
# ═══════════════════════════════════════════════════════════════════════════


class TestSmartWizard:
    """L2: Интерактивные конструкторы документов."""

    def test_wizard_templates_exist(self):
        from src.bot.handlers.legal_tools import WIZARD_TEMPLATES
        assert "nda_wizard" in WIZARD_TEMPLATES
        assert "employment_wizard" in WIZARD_TEMPLATES

    def test_wizard_questions(self):
        from src.bot.handlers.legal_tools import WIZARD_TEMPLATES
        nda = WIZARD_TEMPLATES["nda_wizard"]
        assert len(nda["questions"]) >= 4
        employment = WIZARD_TEMPLATES["employment_wizard"]
        assert len(employment["questions"]) >= 4

    def test_wizard_states_defined(self):
        from src.bot.handlers.legal_tools import LegalToolsStates
        assert LegalToolsStates.wizard_type
        assert LegalToolsStates.wizard_party_name


# ═══════════════════════════════════════════════════════════════════════════
#  L3: Legal Search Agent
# ═══════════════════════════════════════════════════════════════════════════


class TestLegalSearch:
    """L3: Поиск по законам РК."""

    def test_import(self):
        from src.bot.utils.legal_search import (
            find_relevant_laws,
            search_legal_context,
            KEY_LEGAL_ARTICLES,
        )

    def test_find_laws_employment(self):
        """Поиск по трудовому праву."""
        from src.bot.utils.legal_search import find_relevant_laws
        result = find_relevant_laws("Как правильно уволить сотрудника?")
        assert "ТК РК" in result
        assert "ст. 52" in result

    def test_find_laws_tax(self):
        """Поиск по налоговому праву."""
        from src.bot.utils.legal_search import find_relevant_laws
        result = find_relevant_laws("Какие налоги платить?")
        assert "НК РК" in result

    def test_find_laws_aifc(self):
        """Поиск по МФЦА."""
        from src.bot.utils.legal_search import find_relevant_laws
        result = find_relevant_laws("Как зарегистрировать компанию в МФЦА?")
        assert "AIFC" in result or "МФЦА" in result

    def test_find_laws_contract(self):
        """Поиск по договорному праву."""
        from src.bot.utils.legal_search import find_relevant_laws
        result = find_relevant_laws("Заключаем договор аренды")
        assert "ГК РК" in result

    def test_find_laws_no_match(self):
        """Нет совпадений — пустой результат."""
        from src.bot.utils.legal_search import find_relevant_laws
        result = find_relevant_laws("Погода сегодня")
        assert result == ""

    @pytest.mark.asyncio
    async def test_search_legal_context(self):
        """Полный поиск контекста (без Google)."""
        from src.bot.utils.legal_search import search_legal_context
        result = await search_legal_context("Увольнение работника")
        assert "ТК РК" in result


# ═══════════════════════════════════════════════════════════════════════════
#  L4: Conflict Check
# ═══════════════════════════════════════════════════════════════════════════


class TestConflictCheck:
    """L4: Проверка на конфликт интересов."""

    def test_import(self):
        from src.bot.utils.legal_search import check_conflicts

    @pytest.mark.asyncio
    async def test_no_conflicts_without_google(self):
        """Без Google Sheets — нет конфликтов."""
        from src.bot.utils.legal_search import check_conflicts
        result = await check_conflicts(name="Тест", google=None)
        assert result["has_conflict"] is False
        assert result["risk_level"] == "LOW"

    @pytest.mark.asyncio
    async def test_conflicts_found(self):
        """Обнаружение конфликтов в мокированных данных."""
        from src.bot.utils.legal_search import check_conflicts

        mock_google = AsyncMock()
        mock_google.get_recent_leads = AsyncMock(return_value=[
            {"name": "Иванов Иван", "email": "ivan@test.kz", "company": "ТОО Alpha"},
            {"name": "Петров Петр", "email": "petr@alpha.kz", "company": "ТОО Beta"},
        ])
        mock_google.get_consult_log = AsyncMock(return_value=[])

        result = await check_conflicts(name="Иванов", google=mock_google)
        assert result["has_conflict"] is True
        assert len(result["matches"]) > 0


# ═══════════════════════════════════════════════════════════════════════════
#  L5: Multi-agent Brainstorm
# ═══════════════════════════════════════════════════════════════════════════


class TestMultiAgentBrainstorm:
    """L5: Мульти-агентный консилиум."""

    def test_import(self):
        from src.bot.utils.multi_agent import (
            multi_agent_brainstorm,
            quick_brainstorm,
            AGENTS,
        )

    def test_agents_defined(self):
        from src.bot.utils.multi_agent import AGENTS
        assert "aifc_lawyer" in AGENTS
        assert "tax_consultant" in AGENTS
        assert "corporate_strategist" in AGENTS

    @pytest.mark.asyncio
    async def test_brainstorm_mock(self):
        """Мульти-агентный брейншторм (mock AI)."""
        from src.bot.utils.multi_agent import multi_agent_brainstorm

        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_ai:
            mock_ai.return_value.call_with_fallback = AsyncMock(
                return_value="Рекомендация юриста"
            )
            result = await multi_agent_brainstorm("Как открыть ТОО?")
            assert "Консилиум" in result or "Рекомендация" in result
            assert "ИТОГОВОЕ ЗАКЛЮЧЕНИЕ" in result


# ═══════════════════════════════════════════════════════════════════════════
#  L6: OSINT-lite (BIN Check)
# ═══════════════════════════════════════════════════════════════════════════


class TestBINCheck:
    """L6: Проверка контрагента по БИН."""

    def test_is_valid_bin(self):
        from src.bot.utils.legal_search import is_valid_bin
        assert is_valid_bin("123456789012") is True
        assert is_valid_bin("12345") is False
        assert is_valid_bin("") is False
        assert is_valid_bin("12345678901a") is False

    def test_format_bin_report_not_found(self):
        from src.bot.utils.legal_search import format_bin_report
        data = {"bin": "123456789012", "found": False, "error": "Не найден"}
        report = format_bin_report(data)
        assert "не найден" in report.lower() or "❌" in report

    def test_format_bin_report_found(self):
        from src.bot.utils.legal_search import format_bin_report
        data = {
            "bin": "123456789012",
            "found": True,
            "name": "ТОО Test Company",
            "status": "Активна",
            "registration_date": "2020-01-15",
            "activity": "IT",
            "address": "Алматы",
            "info": "",
        }
        report = format_bin_report(data)
        assert "ТОО Test Company" in report
        assert "Активна" in report

    @pytest.mark.asyncio
    async def test_check_invalid_bin(self):
        from src.bot.utils.legal_search import check_counterparty_by_bin
        result = await check_counterparty_by_bin("12345")
        assert result["found"] is False
        assert "error" in result


# ═══════════════════════════════════════════════════════════════════════════
#  L7: Ticket Manager
# ═══════════════════════════════════════════════════════════════════════════


class TestTicketManager:
    """L7: Система тикетов."""

    def test_import(self):
        from src.bot.utils.ticket_manager import (
            create_ticket,
            update_ticket_status,
            get_open_tickets,
            get_overdue_tickets,
            format_ticket,
        )

    @pytest.mark.asyncio
    async def test_create_ticket(self):
        from src.bot.utils.ticket_manager import create_ticket, get_open_tickets

        ticket = await create_ticket(
            title="Проверить договор",
            description="Срочная проверка NDA",
            assignee="Юрист1",
            priority="high",
            deadline_days=3,
        )

        assert ticket["title"] == "Проверить договор"
        assert ticket["status"] == "new"
        assert ticket["assignee"] == "Юрист1"

        open_tickets = get_open_tickets()
        assert len(open_tickets) > 0

    @pytest.mark.asyncio
    async def test_update_ticket_status(self):
        from src.bot.utils.ticket_manager import create_ticket, update_ticket_status, get_ticket

        ticket = await create_ticket(title="Задача 2")
        ok = update_ticket_status(ticket["id"], "in_progress", comment="Начал работу")
        assert ok is True

        t = get_ticket(ticket["id"])
        assert t["status"] == "in_progress"

    def test_format_ticket(self):
        from src.bot.utils.ticket_manager import format_ticket
        ticket = {
            "id": "T-0001",
            "title": "Тест",
            "description": "Описание",
            "assignee": "Юрист",
            "priority": "high",
            "status": "new",
            "user_id": 123,
            "deadline_display": "01.01.2026",
        }
        text = format_ticket(ticket)
        assert "T-0001" in text
        assert "Тест" in text


# ═══════════════════════════════════════════════════════════════════════════
#  L8: Digital Case Storage
# ═══════════════════════════════════════════════════════════════════════════


class TestDigitalCase:
    """L8: Цифровое дело в Mini App."""

    def test_webapp_endpoints_exist(self):
        """API endpoints для цифрового дела."""
        from src.bot.webapp.app import app

        routes = [r.path for r in app.routes]
        assert "/api/user/{user_id}/documents" in routes
        assert "/api/user/{user_id}/consultations" in routes
        assert "/api/user/{user_id}/profile" in routes
        assert "/api/tickets" in routes


# ═══════════════════════════════════════════════════════════════════════════
#  L9: News Impact Analysis
# ═══════════════════════════════════════════════════════════════════════════


class TestNewsImpact:
    """L9: AI-анализ влияния новостей."""

    def test_digest_has_impact_analysis(self):
        """Проверяем, что в digest.py есть анализ влияния."""
        import inspect
        from src.bot.handlers.digest import send_morning_digest
        source = inspect.getsource(send_morning_digest)
        assert "impact_analysis" in source
        assert "Для бизнеса это значит" in source


# ═══════════════════════════════════════════════════════════════════════════
#  L10: Deadline Assistant
# ═══════════════════════════════════════════════════════════════════════════


class TestDeadlineAssistant:
    """L10: Ассистент по дедлайнам."""

    def test_parse_deadline_days(self):
        from src.bot.utils.ticket_manager import parse_deadline_request
        result = parse_deadline_request("Напомни подать отчет через 7 дней")
        assert result is not None
        assert result["days"] == 7

    def test_parse_deadline_months(self):
        from src.bot.utils.ticket_manager import parse_deadline_request
        result = parse_deadline_request("Напомни через 2 месяца продлить лицензию")
        assert result is not None
        assert result["days"] == 60  # 2 * 30

    def test_parse_deadline_weeks(self):
        from src.bot.utils.ticket_manager import parse_deadline_request
        result = parse_deadline_request("Напомни через 3 недели")
        assert result is not None
        assert result["days"] == 21

    def test_parse_deadline_date(self):
        from src.bot.utils.ticket_manager import parse_deadline_request
        result = parse_deadline_request("Напомни 15.06.2027 оплатить налог")
        assert result is not None
        assert result["days"] > 0

    def test_parse_deadline_invalid(self):
        from src.bot.utils.ticket_manager import parse_deadline_request
        result = parse_deadline_request("Просто текст без даты")
        assert result is None

    @pytest.mark.asyncio
    async def test_schedule_reminder(self):
        """Планирование напоминания."""
        from src.bot.utils.ticket_manager import schedule_reminder

        mock_scheduler = MagicMock()
        mock_scheduler.add_job = MagicMock()
        mock_bot = AsyncMock()

        reminder = await schedule_reminder(
            scheduler=mock_scheduler,
            bot=mock_bot,
            user_id=12345,
            task="Подать отчёт в МФЦА",
            days=30,
        )

        assert reminder["task"] == "Подать отчёт в МФЦА"
        assert reminder["days"] == 30
        mock_scheduler.add_job.assert_called_once()

    def test_get_user_reminders(self):
        from src.bot.utils.ticket_manager import get_user_reminders
        # Должны быть reminder-ы от предыдущего теста
        reminders = get_user_reminders(12345)
        assert isinstance(reminders, list)


# ═══════════════════════════════════════════════════════════════════════════
#  INTEGRATION: Full Legal Flow Simulation
# ═══════════════════════════════════════════════════════════════════════════


class TestLegalIntelIntegration:
    """Интеграционный тест: полный юридический поток."""

    @pytest.mark.asyncio
    async def test_full_legal_consultation_flow(self):
        """Симуляция: поиск законов → консультация → тикет → напоминание."""

        # 1. Legal Search
        from src.bot.utils.legal_search import find_relevant_laws, search_legal_context
        laws = find_relevant_laws("увольнение работника по аттестации")
        assert "ст. 52 ТК РК" in laws

        context = await search_legal_context("увольнение работника")
        assert len(context) > 0

        # 2. Create ticket
        from src.bot.utils.ticket_manager import create_ticket, format_ticket
        ticket = await create_ticket(
            title="Консультация по увольнению",
            assignee="Оралбаев Ч.",
            priority="normal",
            deadline_days=5,
            user_id=999,
        )
        assert ticket["id"].startswith("T-")
        formatted = format_ticket(ticket)
        assert "Консультация" in formatted

        # 3. Schedule reminder
        from src.bot.utils.ticket_manager import schedule_reminder
        mock_scheduler = MagicMock()
        mock_scheduler.add_job = MagicMock()
        mock_bot = AsyncMock()

        reminder = await schedule_reminder(
            scheduler=mock_scheduler,
            bot=mock_bot,
            user_id=999,
            task="Проверить статус дела",
            days=5,
        )
        assert reminder["task"] == "Проверить статус дела"

    @pytest.mark.asyncio
    async def test_document_analysis_pipeline(self):
        """Симуляция: загрузка документа → анализ → brainstorm."""

        # 1. Текст документа
        doc_text = (
            "ДОГОВОР ОКАЗАНИЯ ЮРИДИЧЕСКИХ УСЛУГ\n"
            "1. Исполнитель оказывает Заказчику юридические услуги.\n"
            "2. Стоимость услуг: 500 000 тенге.\n"
            "3. Ответственность Исполнителя ограничена суммой договора.\n"
            "4. Заказчик несёт полную ответственность за все риски.\n"
            "5. Срок: 12 месяцев, автопролонгация.\n"
        )

        # 2. DocReview
        from src.bot.utils.doc_review import analyze_legal_document
        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_ai:
            mock_ai.return_value.call_with_fallback = AsyncMock(
                return_value="🔴 <b>РИСК: Неравные условия ответственности</b>"
            )
            review = await analyze_legal_document(doc_text)
            assert "РИСК" in review

        # 3. Multi-agent brainstorm
        from src.bot.utils.multi_agent import AGENTS
        assert len(AGENTS) == 3
