"""Тесты HTML-санитайзера."""

import pytest

from src.bot.utils.html_sanitizer import (
    sanitize_article_html,
    fix_broken_tags,
    sanitize_and_fix,
    slugify,
    unique_slug,
)


class TestSanitizeHtml:
    """Тесты очистки HTML."""

    def test_allowed_tags_preserved(self):
        """Допустимые теги сохраняются."""
        html = "<h2>Title</h2><p>Text <strong>bold</strong></p>"
        result = sanitize_article_html(html)
        assert "<h2>" in result
        assert "<p>" in result
        assert "<strong>" in result

    def test_script_tags_removed(self):
        """Script теги удаляются."""
        html = '<p>OK</p><script>alert("xss")</script>'
        result = sanitize_article_html(html)
        assert "<script>" not in result
        assert "</script>" not in result

    def test_style_tags_removed(self):
        """Style теги удаляются."""
        html = '<style>body{color:red}</style><p>Text</p>'
        result = sanitize_article_html(html)
        assert "<style>" not in result

    def test_empty_input(self):
        """Пустая строка возвращает пустую строку."""
        assert sanitize_article_html("") == ""
        assert sanitize_article_html(None) == ""

    def test_blockquote_preserved(self):
        """Blockquote сохраняется."""
        html = "<blockquote>Цитата из закона</blockquote>"
        result = sanitize_article_html(html)
        assert "<blockquote>" in result


class TestFixBrokenTags:
    """Тесты исправления незакрытых тегов."""

    def test_unclosed_tag_fixed(self):
        """Незакрытый тег исправляется."""
        html = "<p>Text without closing tag"
        result = fix_broken_tags(html)
        assert "</p>" in result

    def test_nested_unclosed(self):
        """Вложенные незакрытые теги исправляются."""
        html = "<h2>Title<p>Paragraph"
        result = fix_broken_tags(html)
        assert "</h2>" in result or "</p>" in result

    def test_empty_input(self):
        assert fix_broken_tags("") == ""


class TestSlugify:
    """Тесты генерации slug."""

    def test_cyrillic_transliteration(self):
        """Кириллица транслитерируется."""
        result = slugify("Увольнение без рисков")
        assert "uvol" in result
        assert " " not in result

    def test_latin_pass_through(self):
        """Латиница проходит без изменений."""
        result = slugify("Hello World")
        assert result == "hello-world"

    def test_special_chars_removed(self):
        """Спецсимволы удаляются."""
        result = slugify("Статья №1: тест!")
        assert "№" not in result
        assert "!" not in result

    def test_max_length(self):
        """Slug ограничен 80 символами."""
        long_title = "А" * 200
        result = slugify(long_title)
        assert len(result) <= 80

    def test_empty_input(self):
        """Пустой ввод возвращает 'article'."""
        result = slugify("")
        assert result == "article"


class TestUniqueSlug:
    """Тесты уникальных slug."""

    @pytest.mark.asyncio
    async def test_no_conflict(self):
        """Без конфликта возвращается базовый slug."""
        result = await unique_slug("Test Title", [])
        assert result == "test-title"

    @pytest.mark.asyncio
    async def test_with_conflict(self):
        """При конфликте добавляется суффикс."""
        result = await unique_slug("Test Title", ["test-title"])
        assert result == "test-title-2"

    @pytest.mark.asyncio
    async def test_multiple_conflicts(self):
        """При нескольких конфликтах суффикс увеличивается."""
        result = await unique_slug(
            "Test Title",
            ["test-title", "test-title-2", "test-title-3"],
        )
        assert result == "test-title-4"
