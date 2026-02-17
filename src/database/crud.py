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

from src.database.models import ConsentLog, FunnelEvent, Lead, Question, Referral, ScheduledTask, TopicSubscription, User, async_session

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
        # Self-join: для каждого пользователя находим все пары скачанных гайдов
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

    # Сортируем каждый список по убыванию
    for gid in matrix:
        matrix[gid].sort(key=lambda x: -x[1])

    return dict(matrix)


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
