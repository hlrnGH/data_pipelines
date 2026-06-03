"""
Fonctions de validation des événements d'écoute.
"""
from datetime import datetime, timezone


def is_valid_listening_event(event: dict) -> bool:
    """
    Valide un événement d'écoute.
    Retourne False si :
    - user_id manquant
    - timestamp dans le futur
    - duration_ms < 5000 (pattern bot)
    """
    # user_id obligatoire
    if not event.get("user_id"):
        return False

    # timestamp ne doit pas être dans le futur
    timestamp_str = event.get("timestamp", "")
    try:
        ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        if ts > datetime.now(timezone.utc):
            return False
    except (ValueError, AttributeError):
        return False

    # durée trop courte = pattern bot
    duration = event.get("duration_ms", 0)
    if duration < 5000:
        return False

    return True
