"""
ExtBot que registra el texto que el bot envía al usuario (para /exportar_chat).
Intercepta send_message, edit_message_text y send_document (caption o nombre de archivo).
"""

from __future__ import annotations

from typing import Any

from telegram.ext import ExtBot

import chat_export_sqlite


def _cid_ok(chat_id: Any) -> int | None:
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return None


class LoggingExtBot(ExtBot):
    async def send_message(self, chat_id: Any, text: str, *args: Any, **kwargs: Any):  # type: ignore[override,no-untyped-def]
        msg = await super().send_message(chat_id, text, *args, **kwargs)
        cid = _cid_ok(chat_id)
        if cid is not None and text and text.strip():
            try:
                chat_export_sqlite.append_bot_line(cid, text.strip())
            except Exception:
                pass
        return msg

    async def edit_message_text(self, text: str, chat_id: Any = None, message_id: int | None = None, inline_message_id: str | None = None, **kwargs: Any):  # type: ignore[override,no-untyped-def]
        result = await super().edit_message_text(
            text,
            chat_id=chat_id,
            message_id=message_id,
            inline_message_id=inline_message_id,
            **kwargs,
        )
        cid = _cid_ok(chat_id)
        if cid is not None and text and text.strip():
            try:
                chat_export_sqlite.append_bot_line(
                    cid, text.strip(), note="mensaje editado"
                )
            except Exception:
                pass
        return result

    async def send_document(self, chat_id: Any, document: Any, caption: str | None = None, **kwargs: Any):  # type: ignore[override,no-untyped-def]
        msg = await super().send_document(
            chat_id, document, caption=caption, **kwargs
        )
        cid = _cid_ok(chat_id)
        if cid is None:
            return msg
        try:
            cap = (caption or "").strip()
            if cap:
                chat_export_sqlite.append_bot_line(cid, cap)
            else:
                fname = "archivo"
                if msg and getattr(msg, "document", None):
                    fname = msg.document.file_name or fname
                elif kwargs.get("filename"):
                    fname = str(kwargs["filename"])
                chat_export_sqlite.append_bot_line(
                    cid, f"[Archivo enviado: {fname}]", note="adjunto"
                )
        except Exception:
            pass
        return msg
