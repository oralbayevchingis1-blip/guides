"""Генерация юридических PDF на лету.

Типовые документы:
- NDA (Соглашение о неразглашении)
- Типовой договор оказания услуг
- Доверенность
- Устав ТОО

Использование:
    from src.bot.utils.pdf_generator import generate_nda_pdf, generate_contract_pdf
    pdf_path = await generate_nda_pdf(party1="SOLIS Partners", party2="ТОО Рога и Копыта")
"""

import io
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join("data", "generated_docs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def _safe_import_reportlab():
    """Импорт reportlab с fallback."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import cm
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        return True
    except ImportError:
        logger.warning("reportlab not installed. PDF generation disabled.")
        return False


async def generate_nda_pdf(
    party1: str,
    party2: str,
    city: str = "Алматы",
    purpose: str = "обсуждение возможного сотрудничества",
    user_name: str = "",
) -> str | None:
    """Генерирует NDA (Соглашение о неразглашении) в формате PDF.

    Returns:
        Путь к файлу или None при ошибке.
    """
    if not _safe_import_reportlab():
        return await _generate_nda_text(party1, party2, city, purpose, user_name)

    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

    date_str = datetime.now(timezone.utc).strftime("%d.%m.%Y")
    filename = f"NDA_{party2.replace(' ', '_')[:30]}_{date_str}.pdf"
    filepath = os.path.join(OUTPUT_DIR, filename)

    doc = SimpleDocTemplate(filepath, pagesize=A4,
                            leftMargin=2 * cm, rightMargin=2 * cm,
                            topMargin=2 * cm, bottomMargin=2 * cm)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("NDATitle", parent=styles["Heading1"],
                                  fontSize=16, alignment=1, spaceAfter=20)
    body_style = ParagraphStyle("NDABody", parent=styles["Normal"],
                                 fontSize=11, leading=15, spaceAfter=8)
    bold_style = ParagraphStyle("NDABold", parent=body_style, fontName="Helvetica-Bold")

    story = []

    story.append(Paragraph("СОГЛАШЕНИЕ О НЕРАЗГЛАШЕНИИ (NDA)", title_style))
    story.append(Paragraph(f"г. {city}                                     {date_str}", body_style))
    story.append(Spacer(1, 20))

    story.append(Paragraph(
        f"<b>{party1}</b> (далее — «Раскрывающая сторона») и "
        f"<b>{party2}</b> (далее — «Получающая сторона»), "
        f"совместно именуемые «Стороны», заключили настоящее Соглашение о нижеследующем:",
        body_style,
    ))
    story.append(Spacer(1, 12))

    sections = [
        ("1. ПРЕДМЕТ СОГЛАШЕНИЯ",
         f"1.1. Стороны обязуются не разглашать конфиденциальную информацию, "
         f"полученную в ходе {purpose}.<br/>"
         f"1.2. Под конфиденциальной информацией понимается любая информация, "
         f"переданная одной Стороной другой в устной, письменной или электронной форме, "
         f"включая, но не ограничиваясь: коммерческую тайну, ноу-хау, финансовые данные, "
         f"стратегические планы и клиентские базы."),
        ("2. ОБЯЗАТЕЛЬСТВА СТОРОН",
         "2.1. Получающая сторона обязуется:<br/>"
         "— не раскрывать конфиденциальную информацию третьим лицам;<br/>"
         "— использовать информацию исключительно в целях, указанных в п. 1.1;<br/>"
         "— обеспечить защиту информации не хуже, чем собственной."),
        ("3. СРОК ДЕЙСТВИЯ",
         "3.1. Настоящее Соглашение вступает в силу с даты подписания "
         "и действует в течение 3 (трёх) лет."),
        ("4. ОТВЕТСТВЕННОСТЬ",
         "4.1. За нарушение условий настоящего Соглашения виновная Сторона "
         "возмещает причинённые убытки в полном объёме в соответствии "
         "с законодательством Республики Казахстан."),
        ("5. ПРИМЕНИМОЕ ПРАВО",
         "5.1. Настоящее Соглашение регулируется законодательством "
         "Республики Казахстан. Споры разрешаются в судебном порядке "
         "по месту нахождения истца."),
    ]

    for title, content in sections:
        story.append(Paragraph(title, bold_style))
        story.append(Paragraph(content, body_style))
        story.append(Spacer(1, 8))

    story.append(Spacer(1, 30))
    story.append(Paragraph("РЕКВИЗИТЫ И ПОДПИСИ СТОРОН", bold_style))
    story.append(Spacer(1, 12))
    story.append(Paragraph(
        f"<b>Раскрывающая сторона:</b><br/>{party1}<br/><br/>"
        f"___________________ / Подпись /<br/><br/>"
        f"<b>Получающая сторона:</b><br/>{party2}<br/><br/>"
        f"___________________ / Подпись /",
        body_style,
    ))

    story.append(Spacer(1, 40))
    story.append(Paragraph(
        "<i>Документ сгенерирован AI-ассистентом SOLIS Partners.<br/>"
        "Носит ознакомительный характер. Перед подписанием рекомендуется "
        "проверка юристом.</i>",
        ParagraphStyle("Disclaimer", parent=body_style, fontSize=8, textColor="gray"),
    ))

    doc.build(story)
    logger.info("NDA PDF generated: %s", filepath)
    return filepath


async def _generate_nda_text(
    party1: str, party2: str, city: str, purpose: str, user_name: str,
) -> str | None:
    """Fallback: генерирует NDA как текстовый файл если reportlab не установлен."""
    date_str = datetime.now(timezone.utc).strftime("%d.%m.%Y")
    filename = f"NDA_{party2.replace(' ', '_')[:30]}_{date_str}.txt"
    filepath = os.path.join(OUTPUT_DIR, filename)

    content = f"""СОГЛАШЕНИЕ О НЕРАЗГЛАШЕНИИ (NDA)
г. {city}                                     {date_str}

{party1} (Раскрывающая сторона) и {party2} (Получающая сторона)
заключили настоящее Соглашение:

1. ПРЕДМЕТ СОГЛАШЕНИЯ
Стороны обязуются не разглашать конфиденциальную информацию,
полученную в ходе {purpose}.

2. ОБЯЗАТЕЛЬСТВА
Получающая сторона обязуется не раскрывать информацию третьим лицам.

3. СРОК: 3 года с даты подписания.

4. ОТВЕТСТВЕННОСТЬ: в соответствии с законодательством РК.

Подписи:
{party1}: ___________________
{party2}: ___________________

---
Сгенерировано AI-ассистентом SOLIS Partners.
Носит ознакомительный характер.
"""
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


async def generate_contract_pdf(
    service_name: str,
    client_name: str,
    client_company: str = "",
    amount: str = "",
    duration: str = "12 месяцев",
) -> str | None:
    """Генерирует типовой договор оказания юридических услуг."""
    date_str = datetime.now(timezone.utc).strftime("%d.%m.%Y")
    filename = f"Contract_{client_name.replace(' ', '_')[:30]}_{date_str}.txt"
    filepath = os.path.join(OUTPUT_DIR, filename)

    company = client_company or client_name

    content = f"""ДОГОВОР ОКАЗАНИЯ ЮРИДИЧЕСКИХ УСЛУГ

г. Алматы                                     {date_str}

ТОО «SOLIS Partners» (Исполнитель)
и {company} (Заказчик)

1. ПРЕДМЕТ ДОГОВОРА
1.1. Исполнитель обязуется оказать следующие юридические услуги:
    {service_name}

2. СТОИМОСТЬ
2.1. Стоимость услуг: {amount or 'по согласованию Сторон'}.

3. СРОК
3.1. Срок действия: {duration}.

4. ОБЯЗАННОСТИ СТОРОН
4.1. Исполнитель обязуется оказать услуги качественно и в срок.
4.2. Заказчик обязуется предоставить необходимые документы и информацию.

5. ПРИМЕНИМОЕ ПРАВО
5.1. Законодательство Республики Казахстан.

Реквизиты:
Исполнитель: ТОО «SOLIS Partners» ___________________
Заказчик: {company} ___________________

---
Сгенерировано AI-ассистентом SOLIS Partners.
Требует проверки юристом перед подписанием.
"""
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


# ── Список доступных шаблонов ────────────────────────────────────────────

DOCUMENT_TEMPLATES = {
    "nda": {
        "title": "📄 NDA (Соглашение о неразглашении)",
        "fields": ["party1", "party2", "city", "purpose"],
        "generator": generate_nda_pdf,
    },
    "contract": {
        "title": "📋 Договор оказания юридических услуг",
        "fields": ["service_name", "client_name", "client_company", "amount"],
        "generator": generate_contract_pdf,
    },
}
