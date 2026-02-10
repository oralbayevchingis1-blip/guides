"""Загрузка PDF-файлов из Google Drive с локальным кешем."""

import logging
import os

import aiohttp

logger = logging.getLogger(__name__)

# URL для прямого скачивания файлов, расшаренных с «Anyone with link»
DOWNLOAD_URL = "https://drive.google.com/uc?id={file_id}&export=download"

# Локальная директория для кеширования скачанных PDF
CACHE_DIR = os.path.join("data", "cache")


async def download_guide_pdf(file_id: str) -> str | None:
    """Скачивает PDF из Google Drive и кеширует локально.

    Args:
        file_id: ID файла из URL Google Drive
                 (``https://drive.google.com/file/d/{ID}/view``).

    Returns:
        Путь к локальному файлу или ``None`` при ошибке.
    """
    local_path = os.path.join(CACHE_DIR, f"{file_id}.pdf")

    # Если файл уже скачан — возвращаем из кеша
    if os.path.isfile(local_path):
        logger.debug("PDF '%s' найден в кеше", file_id)
        return local_path

    # Скачиваем из Drive
    url = DOWNLOAD_URL.format(file_id=file_id)
    logger.info("Скачиваю PDF из Google Drive: %s", file_id)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, allow_redirects=True) as resp:
                if resp.status != 200:
                    logger.error(
                        "Ошибка скачивания PDF (status=%s): %s",
                        resp.status,
                        file_id,
                    )
                    return None

                content = await resp.read()

                # Проверяем что получили PDF, а не HTML-страницу ошибки
                if len(content) < 100 or content[:5] == b"<!DOC":
                    logger.error(
                        "Получен не-PDF контент для file_id=%s "
                        "(возможно, файл не расшарен с 'Anyone with link')",
                        file_id,
                    )
                    return None

        # Сохраняем в кеш
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(content)

        logger.info("PDF сохранён: %s (%d bytes)", local_path, len(content))
        return local_path

    except Exception as e:
        logger.error("Ошибка скачивания PDF '%s': %s", file_id, e)
        return None


def clear_pdf_cache() -> int:
    """Удаляет все закешированные PDF.

    Returns:
        Количество удалённых файлов.
    """
    if not os.path.isdir(CACHE_DIR):
        return 0

    count = 0
    for filename in os.listdir(CACHE_DIR):
        if filename.endswith(".pdf"):
            os.remove(os.path.join(CACHE_DIR, filename))
            count += 1

    logger.info("Очищен PDF-кеш: %d файлов удалено", count)
    return count
