"""
Export tootukassa_jobs.json to CSV with all fields and English translations.
"""

import csv
import json
import sys

def flatten_vacancy(v):
    tka = v.get("tookohaAndmed", {}) or {}
    nkk = v.get("noudedKandidaadile", {}) or {}
    rek = v.get("rekvisiidid", {}) or {}
    kon = v.get("avalikKontaktisik", {}) or {}
    pak = v.get("toopakkuja", {}) or {}

    aadressid = v.get("aadressid", [])
    aadress = aadressid[0].get("aadressTekst", "") if aadressid else ""
    aadress_tapsustus = aadressid[0].get("aadressTapsustus", "") if aadressid else ""

    juhiload = nkk.get("juhiload", [])
    keeleoskused = nkk.get("keeleoskused", [])
    kandideerimise_dok = nkk.get("kandideerimiseDokumendid", [])

    keeled = "; ".join(
        f"{k.get('keel','')} ({k.get('taseKirjas','')})"
        for k in keeleoskused
    )

    tolked = {t["tolgeKood"]: t["vaartusEn"] for t in v.get("tolked", [])}

    return {
        "id": v.get("id"),
        "nimetus": v.get("nimetus"),
        "ametinimetusTapsustus": v.get("ametinimetusTapsustus"),
        "staatusKood": v.get("staatusKood"),
        "kandideerimineKp": v.get("kandideerimineKp"),
        "url": v.get("url"),

        "toopakkuja.nimi": pak.get("nimi"),
        "toopakkuja.registrikood": pak.get("registrikood"),

        "aadressTekst": aadress,
        "aadressTapsustus": aadress_tapsustus,

        "avalikKontaktisik.nimi": kon.get("nimi"),
        "avalikKontaktisik.email": kon.get("email"),
        "avalikKontaktisik.telefon": kon.get("telefon"),
        "avalikKontaktisik.ametikoht": kon.get("ametikoht"),

        "kohtadeArv": tka.get("kohtadeArv"),
        "toosuhteKestusKood": tka.get("toosuhteKestusKood"),
        "onTaiskohaga": tka.get("onTaiskohaga"),
        "onOsakohaga": tka.get("onOsakohaga"),
        "onVahetustega": tka.get("onVahetustega"),
        "onOositi": tka.get("onOositi"),
        "onKodusTootamine": tka.get("onKodusTootamine"),
        "tooaegTapsustus": tka.get("tooaegTapsustus"),
        "tooleAsuminePaev": tka.get("tooleAsuminePaev"),
        "tooleAsumineKuu": tka.get("tooleAsumineKuu"),
        "tooleAsumineAasta": tka.get("tooleAsumineAasta"),
        "onPalkAvalik": tka.get("onPalkAvalik"),
        "tootasuAlates": tka.get("tootasuAlates"),
        "tootasuKuni": tka.get("tootasuKuni"),
        "tootasuTapsustus": tka.get("tootasuTapsustus"),
        "tooylesanded": tka.get("tooylesanded"),
        "omaltPooltPakume": tka.get("omaltPooltPakume"),

        "haridusTase": nkk.get("haridusTase"),
        "nouded": nkk.get("nouded"),
        "lisainfoKandideerijale": nkk.get("lisainfoKandideerijale"),
        "juhiload": "; ".join(juhiload),
        "keeleoskused": keeled,
        "kandideerimiseDokumendid": "; ".join(kandideerimise_dok),
        "linkKeskkonda": nkk.get("linkKeskkonda"),

        "lisamiseKp": rek.get("lisamiseKp"),
        "kinnitamiseKp": rek.get("kinnitamiseKp"),

        # English translations
        "nimetus_en": tolked.get("AMETINIMETUS"),
        "ametinimetusTapsustus_en": tolked.get("AMETINIMETUS_TAPSUSTUS"),
        "tooaegTapsustus_en": tolked.get("TOOAEG_TAPSUSTUS"),
        "tootasuTapsustus_en": tolked.get("TOOTASU_TAPSUSTUS"),
        "tooylesanded_en": tolked.get("TOOYLESANDED"),
        "omaltPooltPakume_en": tolked.get("OMALT_POOLT_PAKUME"),
        "nouded_en": tolked.get("MUUD_NOUDED"),
        "lisainfoKandideerijale_en": tolked.get("LISAINFO_KANDIDEERIJALE"),
    }


def main():
    input_file = sys.argv[1] if len(sys.argv) > 1 else "vacancies.json"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "vacancies.csv"

    with open(input_file, encoding="utf-8") as f:
        data = json.load(f)

    rows = [flatten_vacancy(v) for v in data]

    if not rows:
        print("No data found.")
        return

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done! {len(rows)} vacancies saved to {output_file}")


if __name__ == "__main__":
    main()