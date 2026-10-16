# -*- coding: utf-8 -*-
"""
Stockage partagé des QCM : un fichier JSON par module, dans le dossier data/.
Permet à plusieurs personnes de travailler chacune sur son (ses) module(s)
sans conflits Git, puisque chacun ne modifie que le(s) fichier(s) concerné(s).

Format de chaque fichier data/<slug_du_module>.json :
{
  "module": "Nom du module tel que saisi",
  "lessons": <arbre de profondeur libre>
}

L'arbre "lessons" peut avoir n'importe quelle profondeur : chaque niveau
est soit un dict (un sous-dossier à parcourir), soit une liste de questions
(une feuille). Exemples valides :

  # 1 niveau (ancien format) :
  "lessons": { "cirrhose": [ {question...}, ... ] }

  # 3 niveaux (chapitre -> sous-thème -> année) :
  "lessons": {
    "généralités": {
      "boiterie": {
        "2025": [ {question...}, ... ]
      }
    }
  }

bot.py parcourt cet arbre dynamiquement (un niveau de menu par dict
traversé) jusqu'à tomber sur une liste, qui devient le quiz.

Ce fichier est importé par bot.py et add_qcm_web.py — il doit rester dans
le même dossier qu'eux.
"""

import json
import logging
import os
import re

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def slugify_module(name: str) -> str:
    """Transforme un nom de module en nom de fichier sûr (garde les accents)."""
    name = (name or "").strip().lower()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^\w\-]", "", name, flags=re.UNICODE)
    return name or "module"


def _module_path(module_name: str) -> str:
    return os.path.join(DATA_DIR, f"{slugify_module(module_name)}.json")


def list_module_files() -> list[str]:
    if not os.path.isdir(DATA_DIR):
        return []
    return sorted(f for f in os.listdir(DATA_DIR) if f.endswith(".json"))


def _normalize_tree(node):
    """Normalise récursivement les clés (strip + lowercase) à tous les niveaux
    de l'arbre, sans toucher aux listes de questions (les feuilles)."""
    if isinstance(node, list):
        return node
    if isinstance(node, dict):
        return {
            (key or "").strip().lower(): _normalize_tree(value)
            for key, value in node.items()
        }
    return node


def _merge_tree(a: dict, b: dict, context: str = "") -> None:
    """Fusionne l'arbre b dans a, récursivement (en place)."""
    for key, value in b.items():
        if key not in a:
            a[key] = value
        elif isinstance(a[key], list) and isinstance(value, list):
            a[key].extend(value)
        elif isinstance(a[key], dict) and isinstance(value, dict):
            _merge_tree(a[key], value, context=f"{context}/{key}")
        else:
            logger.warning(
                "⚠️  Conflit de structure lors de la fusion sur \"%s/%s\" — la version déjà chargée est conservée",
                context, key,
            )


def load_qcm_data() -> dict:
    """Charge tous les fichiers data/*.json et renvoie
    { module: <arbre lessons>, ... }"""
    result: dict = {}
    for filename in list_module_files():
        path = os.path.join(DATA_DIR, filename)
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = json.load(f)
        except json.JSONDecodeError as e:
            logger.warning("⚠️  data/%s : JSON invalide, fichier ignoré (%s)", filename, e)
            continue
        except OSError as e:
            logger.warning("⚠️  data/%s : impossible de lire le fichier (%s)", filename, e)
            continue

        if not isinstance(content, dict) or "module" not in content or "lessons" not in content:
            logger.warning(
                "⚠️  data/%s : structure invalide — attendu {\"module\": ..., \"lessons\": {...}}, fichier ignoré",
                filename,
            )
            continue

        module_name = (content.get("module") or "").strip().lower()
        lessons = content.get("lessons", {})
        if not module_name:
            logger.warning("⚠️  data/%s : champ \"module\" vide, fichier ignoré", filename)
            continue

        lessons = _normalize_tree(lessons)
        if not isinstance(lessons, dict):
            logger.warning("⚠️  data/%s : \"lessons\" doit être un objet, fichier ignoré", filename)
            continue

        if module_name in result:
            logger.warning(
                "⚠️  data/%s : le module \"%s\" existe déjà dans un autre fichier — fusion",
                filename, module_name,
            )
            _merge_tree(result[module_name], lessons, context=module_name)
            continue

        result[module_name] = lessons
    return result


def save_module(module_name: str, lessons: dict) -> None:
    """Sauvegarde uniquement le fichier du module concerné (les autres
    fichiers ne sont pas touchés)."""
    module_name = (module_name or "").strip().lower()
    lessons = _normalize_tree(lessons)
    os.makedirs(DATA_DIR, exist_ok=True)
    path = _module_path(module_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"module": module_name, "lessons": lessons}, f, ensure_ascii=False, indent=2)


def delete_module_file(module_name: str) -> None:
    path = _module_path(module_name)
    if os.path.exists(path):
        os.remove(path)


# ============ NAVIGATION / ÉDITION DANS L'ARBRE (profondeur libre) ============

def get_node(root: dict, path: list[str]):
    """Renvoie le nœud à cet endroit de l'arbre (dict ou liste), ou None si le
    chemin n'existe pas."""
    node = root
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node


def ensure_leaf_list(root: dict, path: list[str]) -> list:
    """Crée (si besoin) tous les dicts intermédiaires du chemin, puis renvoie
    la liste de questions à la feuille (la crée vide si elle n'existe pas).
    Lève ValueError si un élément du chemin existe déjà mais n'est pas du bon type."""
    if not path:
        raise ValueError("le chemin ne peut pas être vide")
    node = root
    for key in path[:-1]:
        child = node.setdefault(key, {})
        if not isinstance(child, dict):
            raise ValueError(f"\"{key}\" est déjà une liste de questions, pas un dossier")
        node = child
    last = path[-1]
    leaf = node.setdefault(last, [])
    if not isinstance(leaf, list):
        raise ValueError(f"\"{last}\" est déjà un dossier, pas une liste de questions")
    return leaf


def delete_node(root: dict, path: list[str]) -> bool:
    """Supprime le nœud (dict ou liste) à ce chemin. Renvoie True si supprimé."""
    if not path:
        return False
    node = root
    for key in path[:-1]:
        if not isinstance(node, dict) or key not in node:
            return False
        node = node[key]
    last = path[-1]
    if isinstance(node, dict) and last in node:
        del node[last]
        return True
    return False


def prune_empty_dicts(root: dict, path: list[str]) -> None:
    """Après une suppression, retire les dossiers parents devenus vides,
    en remontant depuis la feuille jusqu'à la racine."""
    for depth in range(len(path) - 1, 0, -1):
        parent = get_node(root, path[:depth])
        if isinstance(parent, dict) and not parent:
            delete_node(root, path[:depth])
        else:
            break