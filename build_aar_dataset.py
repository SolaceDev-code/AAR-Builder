#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import re
import zipfile
from pathlib import Path

from bs4 import BeautifulSoup

METADATA_FIELDS = [
    "source_site_name", "source_base_url", "source_type", "source_telnet_port",
    "schema_name", "schema_version", "archive_filename",
    "wiki_archived_date_display", "wiki_archived_date_iso", "wiki_archived_date_timestamp",
    "archive_build_date_display", "archive_build_date_iso", "archive_build_date_timestamp",
    "json_build_date_display", "json_build_date_iso", "json_build_date_timestamp",
    "character_count", "mech_count", "colossi_count",
]

CHARACTER_FIELDS = [
    "character_name", "full_name", "callsign", "source_character_url", "mech_name",
    "rank", "position", "department", "home_colony",
]

MECH_FIELDS = [
    "mech_slug", "mech_name", "pilot_name", "source_mech_url", "mech_class", "color_scheme", "voice_actor",
    "mech_description", "right_arm_weapon", "left_arm_weapon", "torso_weapon",
    "shoulder_weapon", "sensor_suite", "enhanced_melee", "advanced_targeting_array",
    "improved_agility", "explosive_ammo", "jump_jets", "improved_toughness",
]

COLOSSI_FIELDS = [
    "source_page_title", "source_page_url", "colossi_category", "colossi_category_slug",
    "colossus_name", "colossus_slug", "colossus_description",
]

COLOSSUS_OPTION_FIELDS = [
    "source_combat_page_title", "source_combat_page_url", "class_options", "armor_options",
    "armor_special_options", "hit_chart_options", "weapon_options",
    "weapon_special_options", "hit_locations_by_chart",
]

DEFAULT_CONFIG = {
    "source_site_name": "Aegis Company MUSH",
    "source_base_url": "https://delphi.aresmush.com/",
    "source_type": "AresMUSH wiki export",
    "source_telnet_port": "4201",
    "schema_name": "after_action_report_archive",
    "schema_version": "0.0.1",
}

CANONICAL_OPTIONS = {
    "class_options": "Class One, Class Two, Class Three, Class Four",
    "armor_options": "Agile, General, Ram, Tank",
    "armor_special_options": "Thick Hide, Thick Hide 1, Thick Hide 2",
    "hit_chart_options": "Humanoidcolossus, Quadruped, Avian, Draconic, Crustacean, Insect, Swarm, Hexapod, Centaur, Serpentine, Aquatic, Repair",
    "weapon_options": "Claw, Bite, Spines, Acid, Club, Mandibles, Talons, Tangle",
    "weapon_special_options": "enormous, small",
}

HIT_LOCATIONS_BY_CHART = {
    "Humanoidcolossus": "Abdomen, Chest, Head, Left Arm, Left Hand, Left Leg, Neck, Right Arm, Right Hand, Right Leg",
    "Quadruped": "Body, Head, Left Front Leg, Left Rear Leg, Neck, Right Front Leg, Right Rear Leg, Underbelly",
    "Avian": "Body, Head, Left Wing, Neck, Right Wing, Underbelly",
    "Draconic": "Body, Head, Left Front Leg, Left Rear Leg, Left Wing, Neck, Right Front Leg, Right Rear Leg, Right Wing, Tail, Underbelly",
    "Crustacean": "Body, Head, Left Claw, Left Legs, Right Claw, Right Legs, Tail, Underbelly",
    "Insect": "Abdomen, Head, Left Legs, Left Wing, Right Legs, Right Wing, Thorax",
    "Swarm": "Part of the swarm, Swarm member, Within the swarm",
    "Hexapod": "Body, Head, Left Center Leg, Left Front Leg, Left Rear Leg, Neck, Right Center Leg, Right Front Leg, Right Rear Leg, Underbelly",
    "Centaur": "Abdomen, Body, Head, Left Arm, Left Front Leg, Left Rear Leg, Neck, Right Arm, Right Front Leg, Right Rear Leg, Torso",
    "Serpentine": "Body, Head, Tail",
    "Aquatic": "Abdomen, Body, Dorsal Fin, Head, Left Fin, Right Fin, Tail",
    "Repair": "Derrick, Engine, Gantry, Power Cell, Scaffolding, Tool Arms, Tracks",
    "Mech": "Abdomen, Chest, Cockpit, Gyro, Left Arm, Left Leg, Right Arm, Right Leg",
    "Mech Pilot": "Abdomen, Chest, Groin, Head, Left Arm, Left Hand, Left Leg, Neck, Right Arm, Right Hand, Right Leg",
}


def clean_text(value):
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value if value else ""


def make_slug(value):
    value = clean_text(value).lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def fill_record(record, field_names):
    out = {}
    for field in field_names:
        val = record.get(field, "")
        out[field] = val if isinstance(val, dict) else clean_text(val)
    return out


def format_display_date(value):
    return value.strftime("%d %b %Y")


def utc_now_naive():
    return dt.datetime.now(dt.UTC).replace(tzinfo=None, microsecond=0)


def load_config(path):
    cfg = dict(DEFAULT_CONFIG)
    if path:
        with open(path, "r", encoding="utf-8") as handle:
            user_cfg = json.load(handle)
        cfg.update({k: v for k, v in user_cfg.items() if v is not None})
    return cfg


def get_zip_timestamp(zf):
    infos = zf.infolist()
    if not infos:
        return utc_now_naive()
    return max(dt.datetime(*zi.date_time) for zi in infos)


def page_title_from_soup(soup):
    h1 = soup.find("h1")
    if h1:
        return clean_text(h1.get_text(" ", strip=True))
    title = soup.title.string if soup.title and soup.title.string else ""
    title = clean_text(title)
    return re.sub(r"^Aegis Company Archive -\s*", "", title)


def parse_label_rows(soup):
    result = {}
    for row in soup.select("div.profile-box div.row"):
        cols = row.find_all("div", recursive=False)
        if len(cols) >= 2:
            label = clean_text(cols[0].get_text(" ", strip=True)).rstrip(":")
            value = clean_text(cols[1].get_text(" ", strip=True))
            if label:
                result[label] = value
    return result


def parse_mech_section(soup, mech_name=""):
    result = {field: "" for field in MECH_FIELDS}
    mech_tab = soup.select_one("#mech")
    if not mech_tab:
        return fill_record(result, MECH_FIELDS)

    if not mech_name:
        title_el = mech_tab.select_one(".box-title h2") or mech_tab.select_one(".box-title .h2")
        mech_name = clean_text(title_el.get_text(" ", strip=True)) if title_el else ""

    box_type = mech_tab.select_one(".box-type")
    result["mech_class"] = clean_text(box_type.get_text(" ", strip=True)) if box_type else ""

    profile_map = {}
    for item in mech_tab.select(".profile-item"):
        name_el = item.select_one(".profile-item-name")
        value_el = item.select_one(".profile-item-value")
        if not name_el or not value_el:
            continue
        label = clean_text(name_el.get_text(" ", strip=True))
        label = re.sub(r"^[^A-Za-z]+", "", label)
        value = clean_text(value_el.get_text(" ", strip=True))
        if label and label not in profile_map:
            profile_map[label] = value

    description_header = None
    for heading in mech_tab.find_all(["h5", "h4"]):
        if clean_text(heading.get_text(" ", strip=True)).lower() == "description":
            description_header = heading
            break
    if description_header:
        desc_parts = []
        for sibling in description_header.next_siblings:
            if getattr(sibling, "name", None) in {"h4", "h5"}:
                break
            if getattr(sibling, "get_text", None):
                text = clean_text(sibling.get_text(" ", strip=True))
                if text:
                    desc_parts.append(text)
        result["mech_description"] = clean_text(" ".join(desc_parts))

    field_map = {
        "Color Scheme": "color_scheme",
        "Voice Actor": "voice_actor",
        "Right Arm Weapon": "right_arm_weapon",
        "Left Arm Weapon": "left_arm_weapon",
        "Torso Weapon": "torso_weapon",
        "Shoulder Weapon": "shoulder_weapon",
        "Sensor Suite": "sensor_suite",
        "Enhanced Melee": "enhanced_melee",
        "Advanced Targeting Array": "advanced_targeting_array",
        "Improved Agility": "improved_agility",
        "Explosive Ammo": "explosive_ammo",
        "Jump Jets": "jump_jets",
        "Improved Toughness": "improved_toughness",
    }
    for label, dest in field_map.items():
        result[dest] = clean_text(profile_map.get(label, ""))

    if not result["mech_class"]:
        page_text = mech_tab.get_text("\n", strip=True)
        match = re.search(r"\b(Light|Medium|Heavy|Assault)-class Mech\b", page_text, re.I)
        if match:
            result["mech_class"] = clean_text(match.group(0))

    return fill_record(result, MECH_FIELDS)


def parse_character_page(html, source_base_url):
    soup = BeautifulSoup(html, "html.parser")
    labels = parse_label_rows(soup)
    character_name = page_title_from_soup(soup)

    character = {
        "character_name": character_name,
        "full_name": labels.get("Full Name", ""),
        "callsign": labels.get("Callsign", ""),
        "source_character_url": source_base_url.rstrip("/") + "/char/" + character_name,
        "mech_name": labels.get("Mech Name", ""),
        "rank": labels.get("Rank", ""),
        "position": labels.get("Position", ""),
        "department": labels.get("Department", ""),
        "home_colony": labels.get("Home Colony", ""),
    }
    character = fill_record(character, CHARACTER_FIELDS)

    mech = parse_mech_section(soup, character["mech_name"])
    mech["mech_slug"] = character["mech_name"]
    mech["mech_name"] = character["mech_name"]
    mech["pilot_name"] = character["character_name"]
    mech["source_mech_url"] = character["source_character_url"]
    mech = fill_record(mech, MECH_FIELDS)
    return character, mech


def extract_character_files(zf):
    if "characters.html" not in zf.namelist():
        return []
    soup = BeautifulSoup(zf.read("characters.html").decode("utf-8", errors="ignore"), "html.parser")
    files = []
    skip_exact = {"index.html", "characters.html", "scenes.html", "locations.html", "wiki.html"}
    skip_prefixes = ("gov_", "setting_", "science_", "inc_", "combat", "home", "template_", "mech_", "gm", "new_", "generator-")
    for anchor in soup.select("a[href]"):
        href = clean_text(anchor.get("href"))
        if not href or not href.endswith(".html"):
            continue
        if href in skip_exact or href.startswith(skip_prefixes):
            continue
        if href in zf.namelist():
            files.append(href)

    seen = set()
    ordered = []
    for name in files:
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    return ordered


def parse_colossi_page(zf, source_base_url):
    filename = "setting_colossi.html"
    if filename not in zf.namelist():
        return []
    soup = BeautifulSoup(zf.read(filename).decode("utf-8", errors="ignore"), "html.parser")
    title = page_title_from_soup(soup)
    page_url = source_base_url.rstrip("/") + "/wiki/setting:colossi"
    records = []

    for pane in soup.select("div.tab-content > div.tab-pane"):
        category = ""
        pane_id = clean_text(pane.get("id"))
        if pane_id:
            nav_link = soup.select_one(f'ul.nav-tabs a[href="#{pane_id}"]')
            if nav_link:
                category = clean_text(nav_link.get_text(" ", strip=True))
        if not category:
            category = pane_id.replace("-", " ").title() if pane_id else ""

        for heading in pane.find_all("h4", recursive=False):
            name = clean_text(heading.get_text(" ", strip=True))
            if not name:
                continue
            desc_parts = []
            for sibling in heading.next_siblings:
                if getattr(sibling, "name", None) in {"h1", "h2", "h3", "h4"}:
                    break
                if getattr(sibling, "get_text", None):
                    text = clean_text(sibling.get_text(" ", strip=True))
                    if text:
                        desc_parts.append(text)
            records.append(fill_record({
                "source_page_title": title,
                "source_page_url": page_url,
                "colossi_category": category,
                "colossi_category_slug": make_slug(category),
                "colossus_name": name,
                "colossus_slug": make_slug(name),
                "colossus_description": clean_text(" ".join(desc_parts)),
            }, COLOSSI_FIELDS))
    return records


def parse_gm_colossi(zf, source_base_url):
    filename = "gmcolossi.html"
    if filename not in zf.namelist():
        return []
    soup = BeautifulSoup(zf.read(filename).decode("utf-8", errors="ignore"), "html.parser")
    title = page_title_from_soup(soup)
    page_url = source_base_url.rstrip("/") + "/wiki/gmcolossi"
    record = fill_record({
        "source_combat_page_title": title,
        "source_combat_page_url": page_url,
        **CANONICAL_OPTIONS,
        "hit_locations_by_chart": HIT_LOCATIONS_BY_CHART,
    }, COLOSSUS_OPTION_FIELDS)
    return [record]


def build_dataset(zip_path, config):
    with zipfile.ZipFile(zip_path) as zf:
        archive_ts = get_zip_timestamp(zf)
        build_ts = utc_now_naive()
        metadata = {k: "" for k in METADATA_FIELDS}
        metadata.update({
            "source_site_name": config.get("source_site_name", ""),
            "source_base_url": config.get("source_base_url", ""),
            "source_type": config.get("source_type", ""),
            "source_telnet_port": clean_text(config.get("source_telnet_port", "")),
            "schema_name": config.get("schema_name", ""),
            "schema_version": config.get("schema_version", ""),
            "archive_filename": Path(zip_path).name,
            "wiki_archived_date_display": format_display_date(archive_ts),
            "wiki_archived_date_iso": archive_ts.date().isoformat(),
            "wiki_archived_date_timestamp": archive_ts.isoformat() + "Z",
            "archive_build_date_display": format_display_date(archive_ts),
            "archive_build_date_iso": archive_ts.date().isoformat(),
            "archive_build_date_timestamp": archive_ts.isoformat() + "Z",
            "json_build_date_display": format_display_date(build_ts),
            "json_build_date_iso": build_ts.date().isoformat(),
            "json_build_date_timestamp": build_ts.isoformat() + "Z",
        })

        characters = []
        mechs = []
        for fname in extract_character_files(zf):
            html = zf.read(fname).decode("utf-8", errors="ignore")
            character, mech = parse_character_page(html, metadata["source_base_url"])
            characters.append(character)
            if character.get("mech_name", ""):
                mechs.append(mech)

        colossi = parse_colossi_page(zf, metadata["source_base_url"])
        colossusdata = parse_gm_colossi(zf, metadata["source_base_url"])

        metadata["character_count"] = str(len(characters))
        metadata["mech_count"] = str(len(mechs))
        metadata["colossi_count"] = str(len(colossi))
        metadata = fill_record(metadata, METADATA_FIELDS)
        return {
            "metadata": metadata,
            "characterdata": [fill_record(row, CHARACTER_FIELDS) for row in characters],
            "mechdata": [fill_record(row, MECH_FIELDS) for row in mechs],
            "colossidata": [fill_record(row, COLOSSI_FIELDS) for row in colossi],
            "colossusdata": colossusdata,
        }


def write_outputs(dataset, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    easy_path = output_dir / "aar-data.json"

    date_iso = clean_text(dataset.get("metadata", {}).get("json_build_date_iso", "")) or utc_now_naive().date().isoformat()
    version = clean_text(dataset.get("metadata", {}).get("schema_version", "")) or "0.0.0"
    version_slug = "v" + version.replace(".", "-")
    archive_path = output_dir / f"{date_iso}_{version_slug}_aar-data-archive.json"

    for path in (easy_path, archive_path):
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(dataset, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
    return easy_path, archive_path


def main():
    parser = argparse.ArgumentParser(description="Convert an AresMUSH wiki export zip into AAR JSON.")
    parser.add_argument("zip_path", help="Path to wiki export zip")
    parser.add_argument("--output-dir", default=".", help="Directory for JSON output files")
    parser.add_argument("--config-json", default="", help="Optional JSON config override file")
    args = parser.parse_args()

    config = load_config(args.config_json) if args.config_json else dict(DEFAULT_CONFIG)
    dataset = build_dataset(args.zip_path, config)
    easy_path, archive_path = write_outputs(dataset, args.output_dir)
    print(f"Wrote {easy_path}")
    print(f"Wrote {archive_path}")


if __name__ == "__main__":
    main()
