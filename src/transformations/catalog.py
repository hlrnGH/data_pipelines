"""
Fonctions de transformation du catalogue musical.
"""
from typing import Optional


def normalize_artist_name(name: Optional[str]) -> Optional[str]:
    """
    Normalise le nom d'un artiste :
    - Supprime les espaces en début/fin
    - Met en title case (première lettre de chaque mot en majuscule)
    - Retourne None si l'entrée est None
    """
    if name is None:
        return None
    return name.strip().title()


def validate_track_schema(track: dict) -> list:
    """
    Valide le schéma d'un track.
    Retourne une liste d'erreurs (vide si tout est OK).

    Règles :
    - 'title' obligatoire
    - 'duration_ms' doit être > 0 et <= 36_000_000 (10h max)
    """
    errors = []

    if not track.get("title"):
        errors.append("title manquant ou vide")

    duration = track.get("duration_ms")
    if duration is None:
        errors.append("duration_ms manquant")
    elif duration <= 0:
        errors.append(f"duration_ms invalide : {duration} (doit être > 0)")
    elif duration > 36_000_000:
        errors.append(f"duration_ms trop long : {duration} (max 10h = 36 000 000 ms)")

    return errors


def deduplicate_artists(artists: list) -> list:
    """
    Supprime les doublons d'artistes.
    Un doublon = même nom (insensible à la casse) ET même label.
    Conserve la première occurrence.
    """
    seen = set()
    result = []

    for artist in artists:
        key = (artist.get("name", "").strip().lower(), artist.get("label", "").strip().lower())
        if key not in seen:
            seen.add(key)
            result.append(artist)

    return result
