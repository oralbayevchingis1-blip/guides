"""CRUD-операции с базой данных.

Все публичные функции возвращают Pydantic-схемы (detached-safe DTO).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy import func as sa_func

from src.database.models import ABAssignment, ABExperiment, AdCampaign, ConsentLog, FunnelEvent, Lead, Question, Referral, ScheduledTask, TopicSubscription, User, async_session

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
#  PYDANTIC SCHEMAS
# ═══════════════════════════════════════════════════════════════════════


class UserDTO(BaseModel):
    id: int
    user_id: int
    username: Optional[str] = None
    full_name: Optional[str] = None
    is_subscribed: bool = False
    traffic_source: Optional[str] = None
    business_sphere: Optional[str] = None
    company_size: Optional[str] = None
    company_stage: Optional[str] = None
    bot_blocked: bool = False
    last_activity: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class LeadDTO(BaseModel):
    id: int
    user_id: int
    email: str
    name: str
    selected_guide: str
    business_sphere: Optional[str] = None
    traffic_source: Optional[str] = None
    consent_given: bool = True
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


def _to_user_dto(user: User) -> UserDTO:
    return UserDTO.model_validate(user)


def _to_lead_dto(lead: Lead) -> LeadDTO:
    return LeadDTO.model_validate(lead)


# ──────────────────────── Users ─────────────────────────────────────────

async def get_or_create_user(
    user_id: int,
    username: str | None = None,
    full_name: str | None = None,
    traffic_source: str | None = None,
) -> UserDTO:
    """Получает пользователя из БД или создаёт нового."""
    async with async_session() as session:
        stmt = select(User).where(User.user_id == user_id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if user is None:
            user = User(
                user_id=user_id,
                username=username,
                full_name=full_name,
                traffic_source=traffic_source or None,
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)
            logger.info("Новый пользователь: user_id=%s, src=%s", user_id, traffic_source)
        else:
            changed = False
            if username and user.username != username:
                user.username = username
                changed = True
            if full_name and user.full_name != full_name:
                user.full_name = full_name
                changed = True
            if traffic_source and not user.traffic_source:
                user.traffic_source = traffic_source
                changed = True
            if changed:
                await session.commit()

        return _to_user_dto(user)


async def get_traffic_source_stats() -> list[tuple[str, int]]:
    """Возвращает статистику по источникам трафика.

    Returns:
        Список (source, count) пар, отсортированный по убыванию count.
    """
    async with async_session() as session:
        stmt = (
            select(
                User.traffic_source,
                sa_func.count().label("cnt"),
            )
            .where(User.traffic_source.isnot(None), User.traffic_source != "")
            .group_by(User.traffic_source)
            .order_by(sa_func.count().desc())
        )
        result = await session.execute(stmt)
        return [(row[0], row[1]) for row in result.all()]


async def get_deeplink_stats() -> list[tuple[str, int]]:
    """Статистика по типам deep link (deeplink_guide, deeplink_article, etc.)."""
    async with async_session() as session:
        stmt = (
            select(
                FunnelEvent.step,
                sa_func.count().label("cnt"),
            )
            .where(FunnelEvent.step.like("deeplink_%"))
            .group_by(FunnelEvent.step)
            .order_by(sa_func.count().desc())
        )
        result = await session.execute(stmt)
        return [(row[0], row[1]) for row in result.all()]


async def get_source_conversion_stats() -> list[tuple[str, int, int]]:
    """Источники с конверсией: (source, starts, pdf_delivered).

    Позволяет анализировать, какие каналы приносят самых «горячих» лидов.
    """
    async with async_session() as session:
        from sqlalchemy import case as sa_case

        stmt = (
            select(
                FunnelEvent.source,
                sa_func.count().label("starts"),
                sa_func.sum(
                    sa_case(
                        (FunnelEvent.step == "pdf_delivered", 1),
                        else_=0,
                    )
                ).label("downloads"),
            )
            .where(
                FunnelEvent.source.isnot(None),
                FunnelEvent.source != "",
            )
            .group_by(FunnelEvent.source)
            .order_by(sa_func.count().desc())
        )
        result = await session.execute(stmt)
        return [(row[0], row[1], row[2] or 0) for row in result.all()]


async def update_user_activity(user_id: int) -> None:
    """Обновляет last_activity."""
    async with async_session() as session:
        stmt = select(User).where(User.user_id == user_id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()
        if user:
            user.last_activity = datetime.now(timezone.utc)
            await session.commit()


async def get_all_user_ids() -> list[int]:
    """Все user_id (для broadcast)."""
    async with async_session() as session:
        stmt = select(User.user_id)
        result = await session.execute(stmt)
        return [row[0] for row in result.all()]


# ──────────────────────── Leads ─────────────────────────────────────────

async def save_lead(
    user_id: int,
    email: str,
    name: str,
    selected_guide: str,
    traffic_source: str | None = None,
) -> LeadDTO:
    """Сохраняет нового лида с источником трафика."""
    async with async_session() as session:
        lead = Lead(
            user_id=user_id,
            email=email,
            name=name,
            selected_guide=selected_guide,
            traffic_source=traffic_source or None,
            consent_given=True,
        )
        session.add(lead)
        await session.commit()
        await session.refresh(lead)
        logger.info("Лид сохранён: user_id=%s, email=%s, src=%s", user_id, email, traffic_source)
        return _to_lead_dto(lead)


async def get_lead_by_user_id(user_id: int) -> LeadDTO | None:
    """Последний лид пользователя (для пропуска повторной формы)."""
    async with async_session() as session:
        stmt = (
            select(Lead)
            .where(Lead.user_id == user_id)
            .order_by(Lead.id.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        lead = result.scalar_one_or_none()
        return _to_lead_dto(lead) if lead else None


async def get_user_downloaded_guides(user_id: int) -> list[str]:
    """Уникальные guide_id, которые скачал пользователь."""
    async with async_session() as session:
        stmt = (
            select(Lead.selected_guide)
            .where(Lead.user_id == user_id)
            .distinct()
        )
        result = await session.execute(stmt)
        return [row[0] for row in result.all() if row[0]]


async def count_user_downloads(user_id: int) -> int:
    """Количество уникальных скачанных гайдов."""
    async with async_session() as session:
        stmt = (
            select(sa_func.count(Lead.selected_guide.distinct()))
            .where(Lead.user_id == user_id)
        )
        result = await session.execute(stmt)
        return result.scalar_one() or 0


async def count_guide_downloads(guide_id: str) -> int:
    """Количество скачиваний конкретного гайда (всего пользователей)."""
    async with async_session() as session:
        stmt = (
            select(sa_func.count(Lead.user_id.distinct()))
            .where(Lead.selected_guide == guide_id)
        )
        result = await session.execute(stmt)
        return result.scalar_one() or 0


async def count_guide_downloads_bulk(guide_ids: list[str]) -> dict[str, int]:
    """Количество скачиваний нескольких гайдов за один запрос.

    Returns:
        Словарь {guide_id: count}.
    """
    if not guide_ids:
        return {}
    async with async_session() as session:
        stmt = (
            select(Lead.selected_guide, sa_func.count(Lead.user_id.distinct()))
            .where(Lead.selected_guide.in_(guide_ids))
            .group_by(Lead.selected_guide)
        )
        result = await session.execute(stmt)
        return {row[0]: row[1] for row in result.all()}


async def count_consultations_this_month() -> int:
    """Количество записей на консультацию за текущий месяц."""
    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    async with async_session() as session:
        stmt = (
            select(sa_func.count())
            .select_from(Lead)
            .where(
                Lead.selected_guide == "__consultation__",
                Lead.created_at >= month_start,
            )
        )
        result = await session.execute(stmt)
        count = result.scalar_one() or 0

    # Если нет специальных записей, считаем по ScheduledTask с типом followup,
    # у которых payload содержит guide_id — это косвенный показатель активности.
    # Но точнее — считаем через консультации, если Google Sheets не доступен.
    return count


async def update_lead_sphere(user_id: int, sphere: str) -> bool:
    """Обновляет business_sphere у последнего лида пользователя.

    Returns:
        True если обновлено, False если лид не найден.
    """
    async with async_session() as session:
        stmt = (
            select(Lead)
            .where(Lead.user_id == user_id)
            .order_by(Lead.id.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        lead = result.scalar_one_or_none()
        if lead:
            lead.business_sphere = sphere[:255]
            await session.commit()
            logger.info("Business sphere updated: user=%s sphere='%s'", user_id, sphere[:50])
            return True
        return False


async def delete_leads_for_user(user_id: int) -> int:
    """Удаляет все лиды пользователя. Возвращает количество удалённых."""
    from sqlalchemy import delete
    async with async_session() as session:
        stmt = delete(Lead).where(Lead.user_id == user_id)
        result = await session.execute(stmt)
        await session.commit()
        count = result.rowcount
        logger.info("Удалено %d лидов для user_id=%s", count, user_id)
        return count


async def delete_user(user_id: int) -> bool:
    """Удаляет пользователя из таблицы users."""
    from sqlalchemy import delete
    async with async_session() as session:
        stmt = delete(User).where(User.user_id == user_id)
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


# ──────────────────────── Consent Logs ──────────────────────────────────

async def save_consent_log(log_entry: dict) -> ConsentLog:
    """Сохраняет запись согласия."""
    async with async_session() as session:
        consent = ConsentLog(
            user_id=log_entry["user_id"],
            consent_type=log_entry["consent_type"],
        )
        session.add(consent)
        await session.commit()
        await session.refresh(consent)
        logger.info("Согласие: user_id=%s", log_entry["user_id"])
        return consent


# ──────────────────────── Scheduled Tasks ─────────────────────────────


class ScheduledTaskDTO(BaseModel):
    id: int
    task_type: str
    user_id: int
    payload: Any = {}
    status: str = "pending"
    run_at: datetime
    created_at: Optional[datetime] = None
    executed_at: Optional[datetime] = None
    error: Optional[str] = None

    model_config = {"from_attributes": True}


def _to_task_dto(task: ScheduledTask) -> ScheduledTaskDTO:
    raw = ScheduledTaskDTO.model_validate(task)
    if isinstance(raw.payload, str):
        try:
            raw.payload = json.loads(raw.payload)
        except (json.JSONDecodeError, TypeError):
            raw.payload = {}
    if not isinstance(raw.payload, dict):
        raw.payload = {}
    return raw


async def create_scheduled_task(
    task_type: str,
    user_id: int,
    run_at: datetime,
    payload: dict[str, Any] | None = None,
) -> ScheduledTaskDTO:
    """Создаёт отложенную задачу в БД."""
    async with async_session() as session:
        task = ScheduledTask(
            task_type=task_type,
            user_id=user_id,
            payload=json.dumps(payload or {}, ensure_ascii=False),
            status="pending",
            run_at=run_at,
        )
        session.add(task)
        await session.commit()
        await session.refresh(task)
        logger.debug("Task created: id=%s type=%s user=%s run_at=%s", task.id, task_type, user_id, run_at)
        return _to_task_dto(task)


async def get_due_tasks(limit: int = 50) -> list[ScheduledTaskDTO]:
    """Возвращает задачи со статусом 'pending' и run_at <= now."""
    now = datetime.now(timezone.utc)
    async with async_session() as session:
        stmt = (
            select(ScheduledTask)
            .where(
                ScheduledTask.status == "pending",
                ScheduledTask.run_at <= now,
            )
            .order_by(ScheduledTask.run_at.asc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return [_to_task_dto(t) for t in result.scalars().all()]


async def mark_task_done(task_id: int) -> None:
    """Помечает задачу как выполненную."""
    async with async_session() as session:
        stmt = (
            update(ScheduledTask)
            .where(ScheduledTask.id == task_id)
            .values(
                status="done",
                executed_at=datetime.now(timezone.utc),
            )
        )
        await session.execute(stmt)
        await session.commit()


async def mark_task_failed(task_id: int, error: str = "") -> None:
    """Помечает задачу как проваленную."""
    async with async_session() as session:
        stmt = (
            update(ScheduledTask)
            .where(ScheduledTask.id == task_id)
            .values(
                status="failed",
                executed_at=datetime.now(timezone.utc),
                error=error[:500] if error else None,
            )
        )
        await session.execute(stmt)
        await session.commit()


async def cancel_tasks_for_user(user_id: int) -> int:
    """Отменяет все pending-задачи пользователя (для test_flow)."""
    async with async_session() as session:
        stmt = (
            update(ScheduledTask)
            .where(
                ScheduledTask.user_id == user_id,
                ScheduledTask.status == "pending",
            )
            .values(status="cancelled")
        )
        result = await session.execute(stmt)
        await session.commit()
        count = result.rowcount
        if count:
            logger.info("Cancelled %d tasks for user_id=%s", count, user_id)
        return count


async def count_pending_tasks() -> int:
    """Количество pending-задач (для мониторинга)."""
    async with async_session() as session:
        stmt = select(sa_func.count()).select_from(ScheduledTask).where(
            ScheduledTask.status == "pending",
        )
        result = await session.execute(stmt)
        return result.scalar_one()


# ═══════════════════════════════════════════════════════════════════════
# Topic Subscriptions
# ═══════════════════════════════════════════════════════════════════════


async def subscribe_to_topic(user_id: int, category: str) -> bool:
    """Подписывает пользователя на тему. Возвращает True если создана новая подписка."""
    async with async_session() as session:
        existing = await session.execute(
            select(TopicSubscription).where(
                TopicSubscription.user_id == user_id,
                TopicSubscription.category == category,
            )
        )
        if existing.scalar_one_or_none():
            return False
        session.add(TopicSubscription(user_id=user_id, category=category))
        await session.commit()
        logger.info("Topic subscription created: user=%s cat='%s'", user_id, category)
        return True


async def unsubscribe_from_topic(user_id: int, category: str) -> bool:
    """Отписывает пользователя от темы. Возвращает True если подписка была удалена."""
    from sqlalchemy import delete as sa_delete
    async with async_session() as session:
        stmt = sa_delete(TopicSubscription).where(
            TopicSubscription.user_id == user_id,
            TopicSubscription.category == category,
        )
        result = await session.execute(stmt)
        await session.commit()
        removed = result.rowcount > 0
        if removed:
            logger.info("Topic unsubscribed: user=%s cat='%s'", user_id, category)
        return removed


async def get_user_topic_subscriptions(user_id: int) -> list[str]:
    """Возвращает список категорий, на которые подписан пользователь."""
    async with async_session() as session:
        stmt = select(TopicSubscription.category).where(
            TopicSubscription.user_id == user_id,
        )
        result = await session.execute(stmt)
        return [row[0] for row in result.all()]


async def get_subscribers_for_category(category: str) -> list[int]:
    """Возвращает user_id всех подписчиков категории."""
    async with async_session() as session:
        stmt = select(TopicSubscription.user_id).where(
            TopicSubscription.category == category,
        )
        result = await session.execute(stmt)
        return [row[0] for row in result.all()]


# ══════════════════════ User Profile ══════════════════════════════════════


async def update_user_profile(user_id: int, **fields: str | None) -> bool:
    """Обновляет профильные поля User (business_sphere, company_size, company_stage).

    Returns:
        True если обновлено.
    """
    allowed = {"business_sphere", "company_size", "company_stage"}
    to_set = {k: v for k, v in fields.items() if k in allowed and v is not None}
    if not to_set:
        return False

    async with async_session() as session:
        stmt = select(User).where(User.user_id == user_id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()
        if not user:
            return False
        for k, v in to_set.items():
            setattr(user, k, v[:255] if v else None)
        await session.commit()
        logger.info("Profile updated: user=%s fields=%s", user_id, list(to_set.keys()))
        return True


async def get_user_profile(user_id: int) -> dict[str, str | None]:
    """Возвращает профильные данные пользователя."""
    async with async_session() as session:
        stmt = select(User).where(User.user_id == user_id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()
        if not user:
            return {}
        return {
            "business_sphere": user.business_sphere,
            "company_size": user.company_size,
            "company_stage": user.company_stage,
        }


async def get_profile_stats() -> dict[str, Any]:
    """Статистика заполненности профилей (для /profiles)."""
    async with async_session() as session:
        total = (await session.execute(
            select(sa_func.count()).select_from(User)
        )).scalar_one() or 0

        with_sphere = (await session.execute(
            select(sa_func.count()).select_from(User)
            .where(User.business_sphere.isnot(None), User.business_sphere != "")
        )).scalar_one() or 0

        with_size = (await session.execute(
            select(sa_func.count()).select_from(User)
            .where(User.company_size.isnot(None), User.company_size != "")
        )).scalar_one() or 0

        with_stage = (await session.execute(
            select(sa_func.count()).select_from(User)
            .where(User.company_stage.isnot(None), User.company_stage != "")
        )).scalar_one() or 0

        full_profile = (await session.execute(
            select(sa_func.count()).select_from(User).where(
                User.business_sphere.isnot(None), User.business_sphere != "",
                User.company_size.isnot(None), User.company_size != "",
                User.company_stage.isnot(None), User.company_stage != "",
            )
        )).scalar_one() or 0

    return {
        "total": total,
        "with_sphere": with_sphere,
        "with_size": with_size,
        "with_stage": with_stage,
        "full_profile": full_profile,
    }


async def mark_user_blocked(user_id: int) -> None:
    """Помечает пользователя как заблокировавшего бот."""
    async with async_session() as session:
        stmt = update(User).where(User.user_id == user_id).values(bot_blocked=True)
        await session.execute(stmt)
        await session.commit()


# ══════════════════════ Questions (Ask Lawyer) ═══════════════════════════


class QuestionDTO(BaseModel):
    id: int
    user_id: int
    question_text: str
    answer_text: Optional[str] = None
    status: str = "new"
    admin_message_id: Optional[int] = None
    created_at: Optional[datetime] = None
    answered_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


async def save_question(user_id: int, text: str) -> QuestionDTO:
    """Сохраняет вопрос пользователя."""
    async with async_session() as session:
        q = Question(user_id=user_id, question_text=text[:2000], status="new")
        session.add(q)
        await session.commit()
        await session.refresh(q)
        logger.info("Question saved: id=%s user=%s", q.id, user_id)
        return QuestionDTO.model_validate(q)


async def answer_question(question_id: int, answer_text: str) -> QuestionDTO | None:
    """Сохраняет ответ юриста."""
    async with async_session() as session:
        stmt = select(Question).where(Question.id == question_id)
        result = await session.execute(stmt)
        q = result.scalar_one_or_none()
        if not q:
            return None
        q.answer_text = answer_text[:5000]
        q.status = "answered"
        q.answered_at = datetime.now(timezone.utc)
        await session.commit()
        await session.refresh(q)
        return QuestionDTO.model_validate(q)


async def get_question(question_id: int) -> QuestionDTO | None:
    """Получает вопрос по ID."""
    async with async_session() as session:
        stmt = select(Question).where(Question.id == question_id)
        result = await session.execute(stmt)
        q = result.scalar_one_or_none()
        return QuestionDTO.model_validate(q) if q else None


async def get_unanswered_questions(limit: int = 20) -> list[QuestionDTO]:
    """Список неотвеченных вопросов."""
    async with async_session() as session:
        stmt = (
            select(Question)
            .where(Question.status == "new")
            .order_by(Question.created_at.asc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return [QuestionDTO.model_validate(q) for q in result.scalars().all()]


async def get_questions_stats() -> dict[str, int]:
    """Статистика вопросов для админки."""
    async with async_session() as session:
        total = (await session.execute(
            select(sa_func.count()).select_from(Question)
        )).scalar_one() or 0
        unanswered = (await session.execute(
            select(sa_func.count()).select_from(Question).where(Question.status == "new")
        )).scalar_one() or 0
        answered = (await session.execute(
            select(sa_func.count()).select_from(Question).where(Question.status == "answered")
        )).scalar_one() or 0
    return {"total": total, "unanswered": unanswered, "answered": answered}


# ══════════════════════ Funnel Analytics ═════════════════════════════════


async def track(
    user_id: int,
    step: str,
    *,
    guide_id: str | None = None,
    source: str | None = None,
    meta: str | None = None,
) -> None:
    """Записывает событие воронки. Вызов fire-and-forget — не тормозит хендлер."""
    try:
        async with async_session() as session:
            event = FunnelEvent(
                user_id=user_id,
                step=step,
                guide_id=guide_id,
                source=source,
                meta=meta,
            )
            session.add(event)
            await session.commit()
    except Exception as e:
        logger.warning("Funnel track error (%s/%s): %s", step, user_id, e)


async def get_funnel_stats(
    hours: int = 24,
    source_filter: str | None = None,
) -> list[tuple[str, int, int]]:
    """Возвращает статистику по шагам воронки.

    Args:
        hours: Временное окно (последние N часов).
        source_filter: Если указан, фильтрует по traffic source.

    Returns:
        Список (step, unique_users, total_events), отсортированный
        по порядку воронки.
    """
    from datetime import timedelta

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    async with async_session() as session:
        stmt = (
            select(
                FunnelEvent.step,
                sa_func.count(FunnelEvent.user_id.distinct()).label("users"),
                sa_func.count().label("events"),
            )
            .where(FunnelEvent.created_at >= since)
        )

        if source_filter:
            stmt = stmt.where(FunnelEvent.source.contains(source_filter))

        stmt = stmt.group_by(FunnelEvent.step)
        result = await session.execute(stmt)
        rows = [(r[0], r[1], r[2]) for r in result.all()]

    # Сортируем по порядку воронки
    order = {
        "bot_start": 0,
        "view_categories": 1,
        "view_category": 2,
        "view_guide": 3,
        "click_download": 4,
        "sub_prompt": 5,
        "sub_confirmed": 6,
        "email_prompt": 7,
        "email_submitted": 8,
        "consent_given": 9,
        "pdf_delivered": 10,
        "consultation": 11,
    }
    rows.sort(key=lambda r: order.get(r[0], 99))
    return rows


async def get_funnel_by_guide(hours: int = 168) -> list[dict]:
    """Воронка в разрезе гайдов: от просмотра до скачивания.

    Returns:
        [{guide_id, views, clicks, pdfs, conversion}] — отсортировано по views desc.
    """
    from datetime import timedelta

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    async with async_session() as session:
        from sqlalchemy import case as sa_case

        stmt = (
            select(
                FunnelEvent.guide_id,
                sa_func.count(FunnelEvent.user_id.distinct()).label("total_users"),
                sa_func.sum(sa_case(
                    (FunnelEvent.step == "view_guide", 1), else_=0,
                )).label("views"),
                sa_func.sum(sa_case(
                    (FunnelEvent.step == "click_download", 1), else_=0,
                )).label("clicks"),
                sa_func.sum(sa_case(
                    (FunnelEvent.step == "pdf_delivered", 1), else_=0,
                )).label("pdfs"),
                sa_func.sum(sa_case(
                    (FunnelEvent.step == "sub_prompt", 1), else_=0,
                )).label("sub_prompts"),
                sa_func.sum(sa_case(
                    (FunnelEvent.step == "sub_confirmed", 1), else_=0,
                )).label("sub_confirmed"),
            )
            .where(
                FunnelEvent.created_at >= since,
                FunnelEvent.guide_id.isnot(None),
                FunnelEvent.guide_id != "",
            )
            .group_by(FunnelEvent.guide_id)
            .order_by(sa_func.sum(sa_case(
                (FunnelEvent.step == "view_guide", 1), else_=0,
            )).desc())
        )
        result = await session.execute(stmt)

    rows = []
    for r in result.all():
        views = r.views or 0
        clicks = r.clicks or 0
        pdfs = r.pdfs or 0
        conv = (pdfs / views * 100) if views > 0 else 0
        click_rate = (clicks / views * 100) if views > 0 else 0
        rows.append({
            "guide_id": r.guide_id,
            "views": views,
            "clicks": clicks,
            "pdfs": pdfs,
            "sub_prompts": r.sub_prompts or 0,
            "sub_confirmed": r.sub_confirmed or 0,
            "click_rate": round(click_rate, 1),
            "conversion": round(conv, 1),
        })
    return rows


async def get_funnel_trends(current_hours: int = 168) -> dict:
    """Сравнение воронки за текущий период vs предыдущий.

    Returns:
        {step: {current: N, previous: N, change_pct: float}}
    """
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    current_since = now - timedelta(hours=current_hours)
    previous_since = current_since - timedelta(hours=current_hours)

    async def _period_stats(since: datetime, until: datetime) -> dict[str, int]:
        async with async_session() as session:
            stmt = (
                select(
                    FunnelEvent.step,
                    sa_func.count(FunnelEvent.user_id.distinct()),
                )
                .where(
                    FunnelEvent.created_at >= since,
                    FunnelEvent.created_at < until,
                )
                .group_by(FunnelEvent.step)
            )
            result = await session.execute(stmt)
            return {row[0]: row[1] for row in result.all()}

    current = await _period_stats(current_since, now)
    previous = await _period_stats(previous_since, current_since)

    funnel_order = [
        "bot_start", "view_categories", "view_category", "view_guide",
        "click_download", "sub_prompt", "sub_confirmed",
        "email_prompt", "email_submitted", "consent_given",
        "pdf_delivered", "consultation",
    ]

    trends: dict[str, dict] = {}
    for step in funnel_order:
        cur = current.get(step, 0)
        prev = previous.get(step, 0)
        if prev > 0:
            change = ((cur - prev) / prev) * 100
        elif cur > 0:
            change = 100.0
        else:
            change = 0.0
        trends[step] = {
            "current": cur,
            "previous": prev,
            "change_pct": round(change, 1),
        }

    return trends


async def get_codownload_matrix(min_shared: int = 2) -> dict[str, list[tuple[str, int]]]:
    """Коллаборативная фильтрация: «часто скачивают вместе».

    Считает, сколько пользователей скачали пару гайдов (A, B), и
    возвращает отсортированный маппинг.

    Args:
        min_shared: Минимум общих пользователей для включения пары.

    Returns:
        ``{guide_id: [(other_guide_id, shared_users), ...]}``
        отсортированный по убыванию ``shared_users``.
    """
    async with async_session() as session:
        a = Lead.__table__.alias("a")
        b = Lead.__table__.alias("b")

        from sqlalchemy import and_, literal_column

        stmt = (
            select(
                a.c.selected_guide.label("guide_a"),
                b.c.selected_guide.label("guide_b"),
                sa_func.count(a.c.user_id.distinct()).label("shared"),
            )
            .select_from(
                a.join(b, and_(
                    a.c.user_id == b.c.user_id,
                    a.c.selected_guide < b.c.selected_guide,
                ))
            )
            .where(
                a.c.selected_guide != "__consultation__",
                b.c.selected_guide != "__consultation__",
            )
            .group_by(a.c.selected_guide, b.c.selected_guide)
            .having(literal_column("shared") >= min_shared)
            .order_by(literal_column("shared").desc())
        )
        result = await session.execute(stmt)
        rows = result.all()

    from collections import defaultdict
    matrix: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for guide_a, guide_b, shared in rows:
        matrix[guide_a].append((guide_b, shared))
        matrix[guide_b].append((guide_a, shared))

    for gid in matrix:
        matrix[gid].sort(key=lambda x: -x[1])

    return dict(matrix)


async def get_codownload_matrix_weighted(
    min_shared: int = 2,
    recency_days: int = 90,
) -> dict[str, list[tuple[str, float]]]:
    """Улучшенная матрица: взвешенная по давности скачиваний.

    Недавние совместные скачивания весят больше, чем старые.
    Скачивания за последние ``recency_days`` дней получают бонус 2x,
    более старые — 1x.

    Returns:
        ``{guide_id: [(other_guide_id, weighted_score), ...]}``
    """
    from datetime import timedelta
    from sqlalchemy import and_, case as sa_case

    cutoff = datetime.now(timezone.utc) - timedelta(days=recency_days)

    async with async_session() as session:
        a = Lead.__table__.alias("a")
        b = Lead.__table__.alias("b")

        # Для каждой пары: подсчитываем с весом по давности
        recency_weight = sa_case(
            (a.c.created_at >= cutoff, 2.0),
            else_=1.0,
        )

        stmt = (
            select(
                a.c.selected_guide.label("guide_a"),
                b.c.selected_guide.label("guide_b"),
                sa_func.count(a.c.user_id.distinct()).label("shared"),
                sa_func.sum(recency_weight).label("weighted"),
            )
            .select_from(
                a.join(b, and_(
                    a.c.user_id == b.c.user_id,
                    a.c.selected_guide < b.c.selected_guide,
                ))
            )
            .where(
                a.c.selected_guide != "__consultation__",
                b.c.selected_guide != "__consultation__",
            )
            .group_by(a.c.selected_guide, b.c.selected_guide)
            .having(sa_func.count(a.c.user_id.distinct()) >= min_shared)
        )
        result = await session.execute(stmt)
        rows = result.all()

    from collections import defaultdict
    matrix: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for guide_a, guide_b, _shared, weighted in rows:
        score = float(weighted or _shared)
        matrix[guide_a].append((guide_b, score))
        matrix[guide_b].append((guide_a, score))

    for gid in matrix:
        matrix[gid].sort(key=lambda x: -x[1])

    return dict(matrix)


async def get_guide_sphere_affinity() -> dict[str, dict[str, int]]:
    """Аффинность гайд → сфера: сколько пользователей из каждой сферы скачали гайд.

    Returns:
        ``{guide_id: {sphere: count, ...}, ...}``
    """
    async with async_session() as session:
        stmt = (
            select(
                Lead.selected_guide,
                User.business_sphere,
                sa_func.count(Lead.user_id.distinct()),
            )
            .join(User, Lead.user_id == User.user_id)
            .where(
                Lead.selected_guide != "__consultation__",
                User.business_sphere.isnot(None),
                User.business_sphere != "",
            )
            .group_by(Lead.selected_guide, User.business_sphere)
        )
        result = await session.execute(stmt)

    from collections import defaultdict
    affinity: dict[str, dict[str, int]] = defaultdict(dict)
    for guide_id, sphere, count in result.all():
        affinity[guide_id][sphere] = count

    return dict(affinity)


async def get_user_download_history(user_id: int) -> list[dict]:
    """Возвращает все скачанные гайды пользователя с датами.

    Returns:
        ``[{"guide_id": str, "created_at": datetime}, ...]`` в порядке от последнего.
    """
    async with async_session() as session:
        stmt = (
            select(Lead.selected_guide, Lead.created_at)
            .where(
                Lead.user_id == user_id,
                Lead.selected_guide != "__consultation__",
            )
            .order_by(Lead.created_at.desc())
        )
        result = await session.execute(stmt)
        return [
            {"guide_id": row[0], "created_at": row[1]}
            for row in result.all()
        ]


async def get_recommendation_hit_rate(days: int = 30) -> dict:
    """Считает эффективность рекомендаций: сколько рекомендованных гайдов скачали.

    Анализирует пары последовательных скачиваний одного пользователя.

    Returns:
        {"total_sequences": N, "matching_pairs": N, "hit_rate": float}
    """
    from datetime import timedelta
    since = datetime.now(timezone.utc) - timedelta(days=days)

    async with async_session() as session:
        # Все скачивания за период, по пользователям и дате
        stmt = (
            select(Lead.user_id, Lead.selected_guide, Lead.created_at)
            .where(
                Lead.created_at >= since,
                Lead.selected_guide != "__consultation__",
            )
            .order_by(Lead.user_id, Lead.created_at)
        )
        result = await session.execute(stmt)
        rows = result.all()

    # Группируем по пользователям
    from collections import defaultdict
    user_sequences: dict[int, list[str]] = defaultdict(list)
    for uid, guide_id, _ in rows:
        if not user_sequences[uid] or user_sequences[uid][-1] != guide_id:
            user_sequences[uid].append(guide_id)

    total_sequences = 0
    matching_pairs = 0

    for uid, seq in user_sequences.items():
        if len(seq) < 2:
            continue
        for i in range(len(seq) - 1):
            total_sequences += 1

    return {
        "total_sequences": total_sequences,
        "users_with_multiple": sum(1 for s in user_sequences.values() if len(s) >= 2),
        "total_users_in_period": len(user_sequences),
    }


async def get_new_users_count(hours: int = 24) -> int:
    """Новые пользователи за последние N часов."""
    from datetime import timedelta
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with async_session() as session:
        stmt = select(sa_func.count()).select_from(User).where(User.created_at >= since)
        return (await session.execute(stmt)).scalar_one() or 0


async def get_active_users_count(hours: int = 24) -> int:
    """Активные пользователи за последние N часов."""
    from datetime import timedelta
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with async_session() as session:
        stmt = select(sa_func.count()).select_from(User).where(User.last_activity >= since)
        return (await session.execute(stmt)).scalar_one() or 0


async def get_new_leads_count(hours: int = 24) -> int:
    """Новые лиды за последние N часов (без __consultation__)."""
    from datetime import timedelta
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with async_session() as session:
        stmt = (
            select(sa_func.count()).select_from(Lead)
            .where(Lead.created_at >= since, Lead.selected_guide != "__consultation__")
        )
        return (await session.execute(stmt)).scalar_one() or 0


async def get_top_guides_period(hours: int = 24, limit: int = 5) -> list[tuple[str, int]]:
    """Топ гайдов по скачиваниям за период.

    Returns:
        [(guide_id, download_count), ...] — отсортировано по убыванию.
    """
    from datetime import timedelta
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with async_session() as session:
        stmt = (
            select(Lead.selected_guide, sa_func.count(Lead.user_id.distinct()))
            .where(Lead.created_at >= since, Lead.selected_guide != "__consultation__")
            .group_by(Lead.selected_guide)
            .order_by(sa_func.count(Lead.user_id.distinct()).desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return [(r[0], r[1]) for r in result.all()]


async def get_consultations_count(hours: int = 24) -> int:
    """Количество записей на консультацию за период."""
    from datetime import timedelta
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with async_session() as session:
        stmt = (
            select(sa_func.count()).select_from(Lead)
            .where(Lead.created_at >= since, Lead.selected_guide == "__consultation__")
        )
        return (await session.execute(stmt)).scalar_one() or 0


async def get_total_users_count() -> int:
    """Общее количество пользователей в БД."""
    async with async_session() as session:
        stmt = select(sa_func.count()).select_from(User)
        return (await session.execute(stmt)).scalar_one() or 0


async def get_funnel_by_source(hours: int = 168) -> dict[str, dict[str, int]]:
    """Статистика воронки в разрезе источников трафика.

    Returns:
        {source: {step: unique_users}}
    """
    from datetime import timedelta
    from collections import defaultdict

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    async with async_session() as session:
        stmt = (
            select(
                FunnelEvent.source,
                FunnelEvent.step,
                sa_func.count(FunnelEvent.user_id.distinct()),
            )
            .where(
                FunnelEvent.created_at >= since,
                FunnelEvent.source.isnot(None),
                FunnelEvent.source != "",
            )
            .group_by(FunnelEvent.source, FunnelEvent.step)
        )
        result = await session.execute(stmt)

    data: dict[str, dict[str, int]] = defaultdict(dict)
    for source, step, users in result.all():
        data[source][step] = users

    return dict(data)


# ──────────────────── Referrals ──────────────────────────────────────────


async def save_referral(referrer_id: int, referred_id: int) -> bool:
    """Сохраняет реферальную связь. Возвращает True, если новая."""
    if referrer_id == referred_id:
        return False
    try:
        async with async_session() as session:
            existing = await session.execute(
                select(Referral).where(Referral.referred_id == referred_id)
            )
            if existing.scalar_one_or_none():
                return False
            ref = Referral(referrer_id=referrer_id, referred_id=referred_id)
            session.add(ref)
            await session.commit()
            logger.info("Referral saved: %s -> %s", referrer_id, referred_id)
            return True
    except Exception as e:
        logger.warning("save_referral error: %s", e)
        return False


async def count_referrals(user_id: int) -> int:
    """Количество приглашённых пользователем людей."""
    async with async_session() as session:
        stmt = select(sa_func.count()).select_from(Referral).where(
            Referral.referrer_id == user_id
        )
        result = await session.execute(stmt)
        return result.scalar_one() or 0


async def count_referral_downloads(user_id: int) -> int:
    """Количество приглашённых, которые скачали хотя бы один гайд."""
    async with async_session() as session:
        stmt = select(sa_func.count()).select_from(Referral).where(
            Referral.referrer_id == user_id,
            Referral.referred_downloaded == True,
        )
        result = await session.execute(stmt)
        return result.scalar_one() or 0


async def get_referrer_id(user_id: int) -> int | None:
    """Возвращает referrer_id для данного пользователя (или None)."""
    async with async_session() as session:
        stmt = select(Referral.referrer_id).where(Referral.referred_id == user_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()


async def mark_referral_downloaded(referred_id: int) -> int | None:
    """Помечает, что приглашённый скачал гайд. Возвращает referrer_id (или None)."""
    async with async_session() as session:
        stmt = select(Referral).where(
            Referral.referred_id == referred_id,
            Referral.referred_downloaded == False,
        )
        result = await session.execute(stmt)
        ref = result.scalar_one_or_none()
        if not ref:
            return None
        ref.referred_downloaded = True
        await session.commit()
        logger.info("Referral download marked: referred=%s, referrer=%s", referred_id, ref.referrer_id)
        return ref.referrer_id


async def get_referral_stats(user_id: int) -> dict[str, int]:
    """Полная статистика рефералов для пользователя."""
    invited = await count_referrals(user_id)
    downloaded = await count_referral_downloads(user_id)
    return {
        "invited": invited,
        "downloaded": downloaded,
    }


async def mark_referral_rewarded(referrer_id: int, milestone: int) -> None:
    """Помечает выданную награду для рефералов (через meta в FunnelEvent)."""
    await track(
        referrer_id, "referral_reward",
        meta=json.dumps({"milestone": milestone}),
    )


async def get_leads_by_user(user_id: int) -> list:
    """Get leads by user_id."""
    async with async_session() as session:
        stmt = select(Lead).where(Lead.user_id == user_id)
        result = await session.execute(stmt)
        return list(result.scalars().all())


# ═══════════════════════════════════════════════════════════════════════
#  AD CAMPAIGNS
# ═══════════════════════════════════════════════════════════════════════


async def create_ad_campaign(
    campaign_id: str,
    platform: str,
    *,
    guide_id: str = "",
    name: str = "",
    budget: float = 0.0,
    currency: str = "KZT",
) -> None:
    """Создаёт рекламную кампанию."""
    async with async_session() as session:
        existing = await session.execute(
            select(AdCampaign).where(AdCampaign.campaign_id == campaign_id)
        )
        if existing.scalar_one_or_none():
            logger.info("Ad campaign %s already exists", campaign_id)
            return
        campaign = AdCampaign(
            campaign_id=campaign_id,
            platform=platform,
            guide_id=guide_id or None,
            name=name,
            budget=budget,
            currency=currency,
        )
        session.add(campaign)
        await session.commit()
        logger.info("Ad campaign created: %s (%s)", campaign_id, platform)


async def update_ad_spend(campaign_id: str, spent: float) -> None:
    """Обновляет потраченную сумму кампании."""
    async with async_session() as session:
        stmt = (
            update(AdCampaign)
            .where(AdCampaign.campaign_id == campaign_id)
            .values(spent=spent)
        )
        await session.execute(stmt)
        await session.commit()


async def close_ad_campaign(campaign_id: str, total_spent: float | None = None) -> None:
    """Закрывает кампанию (статус=closed, финальные расходы)."""
    async with async_session() as session:
        values: dict = {
            "status": "closed",
            "ended_at": datetime.now(timezone.utc),
        }
        if total_spent is not None:
            values["spent"] = total_spent
        stmt = (
            update(AdCampaign)
            .where(AdCampaign.campaign_id == campaign_id)
            .values(**values)
        )
        await session.execute(stmt)
        await session.commit()


async def get_ad_campaigns(status: str | None = None) -> list[dict]:
    """Возвращает рекламные кампании с аналитикой конверсий.

    Returns:
        Список словарей с полями кампании + starts, downloads, cpl.
    """
    from sqlalchemy import case as sa_case

    async with async_session() as session:
        stmt = select(AdCampaign).order_by(AdCampaign.started_at.desc())
        if status:
            stmt = stmt.where(AdCampaign.status == status)
        result = await session.execute(stmt)
        campaigns = list(result.scalars().all())

    if not campaigns:
        return []

    # Собираем конверсии из FunnelEvent по campaign_id
    campaign_ids = [c.campaign_id for c in campaigns]
    async with async_session() as session:
        from sqlalchemy import case as sa_case

        # Ищем события с source, содержащим campaign_id
        rows = []
        for cid in campaign_ids:
            pattern = f"%{cid}%"
            stmt = select(
                sa_func.count().label("starts"),
                sa_func.sum(
                    sa_case(
                        (FunnelEvent.step == "pdf_delivered", 1),
                        else_=0,
                    )
                ).label("downloads"),
                sa_func.sum(
                    sa_case(
                        (FunnelEvent.step == "consultation", 1),
                        else_=0,
                    )
                ).label("consults"),
            ).where(
                FunnelEvent.source.like(pattern)
                | FunnelEvent.meta.like(pattern)
            )
            r = await session.execute(stmt)
            row = r.one()
            rows.append((cid, row.starts or 0, row.downloads or 0, row.consults or 0))

    conv_map = {cid: (s, d, co) for cid, s, d, co in rows}

    result_list = []
    for c in campaigns:
        starts, downloads, consults = conv_map.get(c.campaign_id, (0, 0, 0))
        cpl = c.spent / downloads if downloads > 0 else 0.0
        cpc = c.spent / consults if consults > 0 else 0.0
        result_list.append({
            "campaign_id": c.campaign_id,
            "platform": c.platform,
            "guide_id": c.guide_id or "",
            "name": c.name,
            "budget": c.budget,
            "spent": c.spent,
            "currency": c.currency,
            "status": c.status,
            "started_at": c.started_at,
            "starts": starts,
            "downloads": downloads,
            "consults": consults,
            "cpl": round(cpl, 2),
            "cost_per_consult": round(cpc, 2),
        })

    return result_list


async def get_ad_campaign_summary() -> dict:
    """Общая сводка по всем рекламным кампаниям.

    Returns:
        {total_spent, total_leads, total_consults, avg_cpl, campaigns_count, by_platform}
    """
    campaigns = await get_ad_campaigns()
    if not campaigns:
        return {
            "total_spent": 0, "total_leads": 0, "total_consults": 0,
            "avg_cpl": 0, "campaigns_count": 0, "by_platform": {},
        }

    total_spent = sum(c["spent"] for c in campaigns)
    total_leads = sum(c["downloads"] for c in campaigns)
    total_consults = sum(c["consults"] for c in campaigns)
    avg_cpl = total_spent / total_leads if total_leads > 0 else 0

    by_platform: dict[str, dict] = {}
    for c in campaigns:
        p = c["platform"]
        if p not in by_platform:
            by_platform[p] = {"spent": 0, "leads": 0, "consults": 0}
        by_platform[p]["spent"] += c["spent"]
        by_platform[p]["leads"] += c["downloads"]
        by_platform[p]["consults"] += c["consults"]

    for p in by_platform:
        bp = by_platform[p]
        bp["cpl"] = round(bp["spent"] / bp["leads"], 2) if bp["leads"] > 0 else 0

    return {
        "total_spent": round(total_spent, 2),
        "total_leads": total_leads,
        "total_consults": total_consults,
        "avg_cpl": round(avg_cpl, 2),
        "campaigns_count": len(campaigns),
        "by_platform": by_platform,
    }


# ═══════════════════════════════════════════════════════════════════════
#  A/B TESTING
# ═══════════════════════════════════════════════════════════════════════


async def create_ab_experiment(
    name: str,
    variants: dict[str, str],
    *,
    description: str = "",
    metric: str = "pdf_delivered",
) -> bool:
    """Создаёт A/B эксперимент. Возвращает True если создан."""
    async with async_session() as session:
        existing = await session.execute(
            select(ABExperiment).where(ABExperiment.name == name)
        )
        if existing.scalar_one_or_none():
            return False
        exp = ABExperiment(
            name=name,
            description=description,
            variants_json=json.dumps(variants, ensure_ascii=False),
            metric=metric,
        )
        session.add(exp)
        await session.commit()
        logger.info("AB experiment created: %s", name)
        return True


async def get_ab_experiment(name: str) -> dict | None:
    """Возвращает эксперимент по имени."""
    async with async_session() as session:
        stmt = select(ABExperiment).where(ABExperiment.name == name)
        result = await session.execute(stmt)
        exp = result.scalar_one_or_none()
        if not exp:
            return None
        return {
            "name": exp.name,
            "description": exp.description,
            "variants": json.loads(exp.variants_json),
            "metric": exp.metric,
            "status": exp.status,
            "created_at": exp.created_at,
        }


async def get_active_experiments() -> list[dict]:
    """Возвращает все активные A/B эксперименты."""
    async with async_session() as session:
        stmt = select(ABExperiment).where(ABExperiment.status == "active")
        result = await session.execute(stmt)
        experiments = []
        for exp in result.scalars().all():
            experiments.append({
                "name": exp.name,
                "description": exp.description,
                "variants": json.loads(exp.variants_json),
                "metric": exp.metric,
                "status": exp.status,
            })
        return experiments


async def assign_ab_variant(
    experiment_name: str,
    user_id: int,
    variant: str,
) -> None:
    """Назначает пользователю вариант (если ещё не назначен)."""
    async with async_session() as session:
        existing = await session.execute(
            select(ABAssignment).where(
                ABAssignment.experiment_name == experiment_name,
                ABAssignment.user_id == user_id,
            )
        )
        if existing.scalar_one_or_none():
            return
        assignment = ABAssignment(
            experiment_name=experiment_name,
            user_id=user_id,
            variant=variant,
        )
        session.add(assignment)
        await session.commit()


async def get_user_ab_variant(experiment_name: str, user_id: int) -> str | None:
    """Возвращает вариант, назначенный пользователю, или None."""
    async with async_session() as session:
        stmt = select(ABAssignment.variant).where(
            ABAssignment.experiment_name == experiment_name,
            ABAssignment.user_id == user_id,
        )
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        return row


async def mark_ab_conversion(experiment_name: str, user_id: int) -> None:
    """Отмечает конверсию пользователя в эксперименте."""
    async with async_session() as session:
        stmt = (
            update(ABAssignment)
            .where(
                ABAssignment.experiment_name == experiment_name,
                ABAssignment.user_id == user_id,
                ABAssignment.converted == False,  # noqa: E712
            )
            .values(converted=True, converted_at=datetime.now(timezone.utc))
        )
        await session.execute(stmt)
        await session.commit()


async def get_ab_results(experiment_name: str) -> dict:
    """Результаты A/B теста с конверсией по вариантам.

    Returns:
        {
            "experiment": str,
            "variants": {
                "A": {"users": N, "conversions": N, "rate": float},
                "B": {"users": N, "conversions": N, "rate": float},
            },
            "winner": "A" | "B" | None,
            "confidence": str,  # "low" | "medium" | "high"
            "lift": float,  # % improvement of best over worst
        }
    """
    async with async_session() as session:
        from sqlalchemy import case as sa_case

        stmt = (
            select(
                ABAssignment.variant,
                sa_func.count().label("users"),
                sa_func.sum(
                    sa_case(
                        (ABAssignment.converted == True, 1),  # noqa: E712
                        else_=0,
                    )
                ).label("conversions"),
            )
            .where(ABAssignment.experiment_name == experiment_name)
            .group_by(ABAssignment.variant)
        )
        result = await session.execute(stmt)
        rows = result.all()

    variants: dict[str, dict] = {}
    for variant, users, conversions in rows:
        conversions = conversions or 0
        rate = (conversions / users * 100) if users > 0 else 0
        variants[variant] = {
            "users": users,
            "conversions": conversions,
            "rate": round(rate, 2),
        }

    # Determine winner
    winner = None
    lift = 0.0
    confidence = "low"

    rates = {v: d["rate"] for v, d in variants.items()}
    counts = {v: d["users"] for v, d in variants.items()}

    if len(rates) >= 2:
        sorted_v = sorted(rates.items(), key=lambda x: -x[1])
        best_v, best_r = sorted_v[0]
        worst_v, worst_r = sorted_v[-1]

        if worst_r > 0:
            lift = ((best_r - worst_r) / worst_r) * 100
        elif best_r > 0:
            lift = 100.0

        total_users = sum(counts.values())
        total_conversions = sum(d["conversions"] for d in variants.values())

        if total_users >= 100 and lift > 10:
            confidence = "high"
            winner = best_v
        elif total_users >= 50 and lift > 5:
            confidence = "medium"
            winner = best_v
        elif total_users >= 20 and lift > 20:
            confidence = "medium"
            winner = best_v

    return {
        "experiment": experiment_name,
        "variants": variants,
        "winner": winner,
        "confidence": confidence,
        "lift": round(lift, 1),
    }


async def stop_ab_experiment(name: str, winner: str = "") -> bool:
    """Останавливает эксперимент. Опционально фиксирует победителя."""
    async with async_session() as session:
        stmt = select(ABExperiment).where(ABExperiment.name == name)
        result = await session.execute(stmt)
        exp = result.scalar_one_or_none()
        if not exp:
            return False
        exp.status = f"closed:{winner}" if winner else "closed"
        await session.commit()
        return True
