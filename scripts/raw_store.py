import hashlib
import time
from pathlib import Path

ILLEGAL_CHARS = '<>:"/\\|?*'


def _sanitize(value):
    cleaned = "".join("_" if ch in ILLEGAL_CHARS else ch for ch in value)
    cleaned = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in cleaned)
    return cleaned.strip("._")


def _params_to_string(params):
    if not params:
        return ""
    if isinstance(params, str):
        return params
    if isinstance(params, dict):
        parts = [f"{k}={v}" for k, v in sorted(params.items())]
        return "&".join(parts)
    return str(params)


def save_raw_xml(base_dir, season, league_key, endpoint, params, xml_bytes, counter):
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    endpoint_slug = _sanitize(endpoint.strip("/").replace("/", "_"))
    params_str = _params_to_string(params)
    params_slug = _sanitize(params_str)

    name_parts = [f"{counter:06d}", endpoint_slug]
    if params_slug:
        if len(params_slug) > 80:
            params_hash = hashlib.md5(params_str.encode("utf-8")).hexdigest()
            name_parts.append(params_hash)
        else:
            name_parts.append(params_slug)
    name_parts.append(timestamp)

    file_name = "__".join(part for part in name_parts if part) + ".xml"
    raw_dir = Path(base_dir) / "data" / "raw" / str(season) / str(league_key or "unknown")
    raw_dir.mkdir(parents=True, exist_ok=True)

    file_path = raw_dir / file_name
    if len(file_name) > 150 or len(str(file_path)) > 240:
        name_hash = hashlib.md5(file_name.encode("utf-8")).hexdigest()
        file_name = f"{counter:06d}__{endpoint_slug}__{name_hash}__{timestamp}.xml"
        file_path = raw_dir / file_name
    if len(str(file_path)) > 240:
        base_hash = hashlib.md5(f"{endpoint}|{params_str}".encode("utf-8")).hexdigest()
        file_name = f"{counter:06d}__{base_hash}__{timestamp}.xml"
        file_path = raw_dir / file_name
    file_path.write_bytes(xml_bytes)
    return str(file_path)
