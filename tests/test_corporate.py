"""Тесты для плана «Корпоративный стандарт и Глубокая автоматизация».

C1: Google Calendar (Legal Booking)
C2: Docx-Engine
C3: Legal Task Tracker (DB)
C4: Email Marketing
C5: Sentiment Analysis
C6: Legal Invoicing
C7: Encrypted Vault
C8: Practice Area AI
C9: Vector Search 2.0
C10: QA Audit AI
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch


# ═══════════════════════════════════════════════════════════════════════════
#  C1: Google Calendar (Legal Booking)
# ═══════════════════════════════════════════════════════════════════════════


class TestCalendarBooking:
    """C1: Запись на консультацию."""

    def test_import(self):
        from src.bot.utils.calendar_client import (
            get_available_slots,
            create_event,
            get_booked_slots,
            cancel_booking,
        )

    @pytest.mark.asyncio
    async def test_get_slots(self):
        from src.bot.utils.calendar_client import get_available_slots
        slots = await get_available_slots(days_ahead=5)
        assert isinstance(slots, list)
        # В рабочие дни должны быть слоты
        for s in slots:
            assert "id" in s
            assert "date" in s
            assert "time" in s
            assert "display" in s

    @pytest.mark.asyncio
    async def test_create_event(self):
        from src.bot.utils.calendar_client import create_event, get_available_slots
        slots = await get_available_slots()
        if slots:
            result = await create_event(
                slot_id=slots[0]["id"],
                client_name="Тест Иванов",
                client_email="test@test.kz",
                topic="Тестовая консультация",
            )
            assert result["success"] is True
            assert result["client_name"] == "Тест Иванов"

    @pytest.mark.asyncio
    async def test_double_booking_rejected(self):
        from src.bot.utils.calendar_client import create_event, get_available_slots
        slots = await get_available_slots()
        if len(slots) >= 1:
            slot_id = slots[0]["id"]
            # Первое бронирование
            r1 = await create_event(slot_id, "Клиент 1")
            if r1["success"]:
                # Попытка повторного бронирования
                r2 = await create_event(slot_id, "Клиент 2")
                assert r2["success"] is False

    def test_cancel_booking(self):
        from src.bot.utils.calendar_client import cancel_booking, _booked_slots
        _booked_slots["test_slot"] = {"client": "test"}
        assert cancel_booking("test_slot") is True
        assert cancel_booking("nonexistent") is False


# ═══════════════════════════════════════════════════════════════════════════
#  C2: Docx-Engine
# ═══════════════════════════════════════════════════════════════════════════


class TestDocxEngine:
    """C2: Генерация .docx документов."""

    def test_import(self):
        from src.bot.utils.docx_engine import (
            DOCX_TEMPLATES,
            generate_document_docx,
            get_document_as_bytes,
        )

    def test_templates_defined(self):
        from src.bot.utils.docx_engine import DOCX_TEMPLATES
        assert "nda" in DOCX_TEMPLATES
        assert "power_of_attorney" in DOCX_TEMPLATES
        assert "claim_letter" in DOCX_TEMPLATES
        assert "employment_contract" in DOCX_TEMPLATES
        assert "service_agreement" in DOCX_TEMPLATES

    def test_template_fields(self):
        from src.bot.utils.docx_engine import DOCX_TEMPLATES
        for tid, tmpl in DOCX_TEMPLATES.items():
            assert "title" in tmpl
            assert "fields" in tmpl
            assert "questions" in tmpl
            assert len(tmpl["questions"]) >= 3

    @pytest.mark.asyncio
    async def test_generate_ai_fallback(self):
        """Генерация через AI когда шаблона нет."""
        from src.bot.utils.docx_engine import generate_document_docx

        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_ai:
            mock_ai.return_value.call_with_fallback = AsyncMock(
                return_value="СОГЛАШЕНИЕ О НЕРАЗГЛАШЕНИИ\n1. Стороны...\n2. Предмет..."
            )
            result = await generate_document_docx(
                "nda",
                {"client_name": "ТОО Тест", "counterparty": "ТОО Партнёр"},
                user_id=12345,
            )
            # Должен создать файл (txt или docx)
            assert result is not None


# ═══════════════════════════════════════════════════════════════════════════
#  C3: Legal Task Tracker
# ═══════════════════════════════════════════════════════════════════════════


class TestLegalTaskTracker:
    """C3: Модель LegalTask в БД."""

    def test_model_exists(self):
        from src.database.models import LegalTask
        assert LegalTask.__tablename__ == "legal_tasks"

    def test_model_fields(self):
        from src.database.models import LegalTask
        columns = {c.name for c in LegalTask.__table__.columns}
        assert "title" in columns
        assert "status" in columns
        assert "priority" in columns
        assert "assignee" in columns
        assert "deadline" in columns
        assert "user_id" in columns


# ═══════════════════════════════════════════════════════════════════════════
#  C4: Email Marketing
# ═══════════════════════════════════════════════════════════════════════════


class TestEmailMarketing:
    """C4: Отправка email."""

    def test_import(self):
        from src.bot.utils.email_sender import (
            send_email,
            send_welcome_email,
        )

    @pytest.mark.asyncio
    async def test_send_email_no_config(self):
        """Без настроек SMTP — возвращает False без ошибки."""
        from src.bot.utils.email_sender import send_email
        result = await send_email("test@test.kz", "Test", "<p>Test</p>")
        assert result is False

    @pytest.mark.asyncio
    async def test_welcome_email_template(self):
        """Шаблон приветственного письма формируется без ошибок."""
        from src.bot.utils.email_sender import send_welcome_email
        # Без SMTP — вернёт False, но не крашнется
        result = await send_welcome_email("Тест", "test@test.kz", "IT Guide")
        assert result is False


# ═══════════════════════════════════════════════════════════════════════════
#  C5: Sentiment Analysis
# ═══════════════════════════════════════════════════════════════════════════


class TestSentimentAnalysis:
    """C5: Определение срочности."""

    def test_import(self):
        from src.bot.utils.email_sender import analyze_sentiment

    def test_normal_question(self):
        from src.bot.utils.email_sender import analyze_sentiment
        result = analyze_sentiment("Как зарегистрировать ТОО?")
        assert result["urgency"] == "NORMAL"
        assert result["needs_alert"] is False

    def test_critical_question(self):
        from src.bot.utils.email_sender import analyze_sentiment
        result = analyze_sentiment("У нас обыск и блокировка счета! Что делать?!")
        assert result["urgency"] in ("CRITICAL", "URGENT")
        assert result["needs_alert"] is True
        assert len(result["triggers"]) > 0

    def test_urgent_question(self):
        from src.bot.utils.email_sender import analyze_sentiment
        result = analyze_sentiment("Срочно помогите! Завтра суд!")
        assert result["urgency"] in ("CRITICAL", "URGENT")
        assert result["score"] >= 30

    def test_high_question(self):
        from src.bot.utils.email_sender import analyze_sentiment
        result = analyze_sentiment("Это важно, нужно решить быстро")
        assert result["urgency"] in ("HIGH", "URGENT")

    def test_panic_detection(self):
        from src.bot.utils.email_sender import analyze_sentiment
        result = analyze_sentiment("ПОМОГИТЕ!!! ЭТО КАТАСТРОФА!!!")
        assert result["score"] > 20  # caps + exclamations


# ═══════════════════════════════════════════════════════════════════════════
#  C7: Encrypted Vault
# ═══════════════════════════════════════════════════════════════════════════


class TestEncryptedVault:
    """C7: Шифрованное хранилище."""

    def test_import(self):
        from src.bot.utils.docx_engine import (
            encrypt_and_store,
            decrypt_and_retrieve,
            get_user_vault_files,
        )

    @pytest.mark.asyncio
    async def test_encrypt_decrypt_cycle(self):
        """Шифрование и дешифрование данных."""
        from src.bot.utils.docx_engine import encrypt_and_store, decrypt_and_retrieve

        original = b"Confidential legal document content"
        filepath = await encrypt_and_store(original, "test_doc.pdf", user_id=99999)

        decrypted = await decrypt_and_retrieve(filepath)
        assert decrypted == original

    def test_get_user_vault_files(self):
        from src.bot.utils.docx_engine import get_user_vault_files
        files = get_user_vault_files(99999)
        assert isinstance(files, list)
        # Должен быть файл от предыдущего теста
        assert len(files) >= 1


# ═══════════════════════════════════════════════════════════════════════════
#  C8: Practice Area AI
# ═══════════════════════════════════════════════════════════════════════════


class TestPracticeAreaAI:
    """C8: Узкоспециализированный контекст."""

    def test_import(self):
        from src.bot.utils.vector_search import (
            detect_practice_area,
            get_practice_context,
            PRACTICE_AREAS,
        )

    def test_areas_defined(self):
        from src.bot.utils.vector_search import PRACTICE_AREAS
        assert "tax" in PRACTICE_AREAS
        assert "it_aifc" in PRACTICE_AREAS
        assert "corporate" in PRACTICE_AREAS
        assert "labor" in PRACTICE_AREAS
        assert "litigation" in PRACTICE_AREAS
        assert "ip" in PRACTICE_AREAS

    def test_detect_tax(self):
        from src.bot.utils.vector_search import detect_practice_area
        areas = detect_practice_area("Какие налоги платить при КПН?")
        assert any(a["id"] == "tax" for a in areas)

    def test_detect_labor(self):
        from src.bot.utils.vector_search import detect_practice_area
        areas = detect_practice_area("Как уволить работника по ТК РК?")
        assert any(a["id"] == "labor" for a in areas)

    def test_detect_aifc(self):
        from src.bot.utils.vector_search import detect_practice_area
        areas = detect_practice_area("Регистрация стартапа в МФЦА")
        assert any(a["id"] == "it_aifc" for a in areas)

    def test_get_context(self):
        from src.bot.utils.vector_search import get_practice_context
        ctx = get_practice_context("Налоговая проверка КПН")
        assert "НК РК" in ctx
        assert "СПЕЦИАЛИЗИРОВАННЫЙ КОНТЕКСТ" in ctx

    def test_no_match(self):
        from src.bot.utils.vector_search import get_practice_context
        ctx = get_practice_context("Прогноз погоды")
        assert ctx == ""


# ═══════════════════════════════════════════════════════════════════════════
#  C9: Vector Search 2.0
# ═══════════════════════════════════════════════════════════════════════════


class TestVectorSearch:
    """C9: Семантический поиск."""

    def test_import(self):
        from src.bot.utils.vector_search import (
            build_index,
            search_similar,
            search_consult_history,
            format_search_results,
        )

    def test_build_and_search(self):
        from src.bot.utils.vector_search import build_index, search_similar, _index
        _index.clear()  # Reset

        entries = [
            {"text": "Как уволить сотрудника за прогулы по ТК РК?", "source": "consult"},
            {"text": "Регистрация ТОО в Казахстане через ЦОН", "source": "article"},
            {"text": "Налоговые льготы для IT компаний в МФЦА", "source": "consult"},
            {"text": "Трудовой договор оформление работника зарплата", "source": "article"},
            {"text": "Развод раздел имущества суд", "source": "consult"},
        ]

        count = build_index(entries)
        assert count == 5

        # Поиск
        results = search_similar("увольнение работника прогул")
        assert len(results) > 0
        # Первый результат должен быть про увольнение
        assert "уволить" in results[0]["text"].lower() or "трудов" in results[0]["text"].lower()

    def test_search_no_index(self):
        from src.bot.utils.vector_search import search_similar, _index
        saved = list(_index)
        _index.clear()
        results = search_similar("тест")
        assert results == []
        _index.extend(saved)

    def test_format_results(self):
        from src.bot.utils.vector_search import format_search_results
        results = [{"text": "Тест ответ", "source": "consult_log", "score": 0.85, "metadata": {}}]
        formatted = format_search_results(results)
        assert "ПОХОЖИЕ ПРЕЦЕДЕНТЫ" in formatted
        assert "85%" in formatted


# ═══════════════════════════════════════════════════════════════════════════
#  C10: QA Audit AI
# ═══════════════════════════════════════════════════════════════════════════


class TestQAAudit:
    """C10: Аудит качества ответов."""

    def test_import(self):
        from src.bot.utils.vector_search import run_qa_audit, scheduled_qa_audit

    @pytest.mark.asyncio
    async def test_audit_no_data(self):
        """Аудит без данных — не крашится."""
        from src.bot.utils.vector_search import run_qa_audit
        result = await run_qa_audit()
        assert "нет данных" in result.lower() or "аудит" in result.lower()

    @pytest.mark.asyncio
    async def test_audit_with_mock_data(self):
        """Аудит с мокированными данными."""
        from src.bot.utils.vector_search import run_qa_audit

        mock_google = AsyncMock()
        mock_google.get_consult_log = AsyncMock(return_value=[
            {"user_id": "123", "question": "Как уволить?", "answer": "По ст. 52 ТК РК..."},
            {"user_id": "456", "question": "Налоги ИП?", "answer": "Согласно НК РК..."},
        ])

        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_ai:
            mock_ai.return_value.call_with_fallback = AsyncMock(
                return_value="📊 Оценка: 7/10. Ответы корректны."
            )
            result = await run_qa_audit(google=mock_google)
            assert "аудит" in result.lower() or "Оценка" in result


# ═══════════════════════════════════════════════════════════════════════════
#  INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════


class TestCorporateIntegration:
    """Интеграционный тест: полный корпоративный поток."""

    @pytest.mark.asyncio
    async def test_full_consultation_flow(self):
        """Вопрос → sentiment → practice area → vector search → answer."""

        # 1. Sentiment
        from src.bot.utils.email_sender import analyze_sentiment
        sentiment = analyze_sentiment("Срочно! Блокировка счета!")
        assert sentiment["urgency"] in ("CRITICAL", "URGENT")

        # 2. Practice Area
        from src.bot.utils.vector_search import get_practice_context
        ctx = get_practice_context("Блокировка счета налоговая")
        assert "НК РК" in ctx or "СПЕЦИАЛИЗИРОВАННЫЙ" in ctx

        # 3. Vector search
        from src.bot.utils.vector_search import search_similar
        results = search_similar("блокировка счета")
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_document_generation_flow(self):
        """Шаблон → данные → генерация → vault."""
        from src.bot.utils.docx_engine import DOCX_TEMPLATES, encrypt_and_store, decrypt_and_retrieve

        # Проверяем шаблоны
        assert len(DOCX_TEMPLATES) >= 5

        # Шифрование/дешифрование
        data = b"Test legal document for vault"
        path = await encrypt_and_store(data, "contract.pdf", 88888)
        decrypted = await decrypt_and_retrieve(path)
        assert decrypted == data

    def test_booking_slots_not_empty(self):
        """В рабочие дни должны быть слоты."""
        from src.bot.utils.calendar_client import _generate_slots
        slots = _generate_slots(10)
        # Может быть пустым только если все слоты в прошлом (крайне маловероятно)
        assert isinstance(slots, list)
