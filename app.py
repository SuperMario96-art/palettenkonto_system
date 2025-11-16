from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, send_file
)
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, date, timedelta
from config import DATABASE_URI, SECRET_KEY

import io
from openpyxl import Workbook
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import simpleSplit
import re

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URI
app.config["SECRET_KEY"] = SECRET_KEY
db = SQLAlchemy(app)


# ---------- JINJA-Filter für Datum/Zeit ----------

@app.template_filter("dt")
def format_datetime(value):
    if not value:
        return ""
    return value.strftime("%d.%m.%Y %H:%M")


# -------------------- Modelle --------------------

class Partner(db.Model):
    __tablename__ = "partner"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, nullable=False)

    accounts = db.relationship("Account", back_populates="partner")
    month_closures = db.relationship("MonthClosure", back_populates="partner")


class Account(db.Model):
    __tablename__ = "account"
    id = db.Column(db.Integer, primary_key=True)
    partner_id = db.Column(db.Integer, db.ForeignKey("partner.id"))

    partner = db.relationship("Partner", back_populates="accounts")
    entries = db.relationship("Entry", back_populates="account")


class Entry(db.Model):
    __tablename__ = "entry"
    id = db.Column(db.Integer, primary_key=True)

    belegnummer = db.Column(db.String(20))
    datum = db.Column(db.DateTime)          # Buchungsdatum und -zeit
    richtung = db.Column(db.String(20))     # 'Eingang' / 'Ausgang' / 'Korrektur'

    menge_eup = db.Column(db.Integer)
    menge_gb = db.Column(db.Integer)
    menge_tmb1 = db.Column(db.Integer)
    menge_tmb2 = db.Column(db.Integer)

    kommentar = db.Column(db.Text)
    konto_seq = db.Column(db.Integer, default=0)

    erfasst_von = db.Column(db.Text)

    account_id = db.Column(db.Integer, db.ForeignKey("account.id"))
    account = db.relationship("Account", back_populates="entries")


class MonthClosure(db.Model):
    """
    Monatsabschluss: harte Sperre –
    nach dem Monatsabschluss können im jeweiligen Monat
    keine Buchungen hinzugefügt, geändert oder korrigiert werden.
    """
    __tablename__ = "month_closure"

    id = db.Column(db.Integer, primary_key=True)
    partner_id = db.Column(db.Integer, db.ForeignKey("partner.id"), nullable=False)

    year = db.Column(db.Integer, nullable=False)
    month = db.Column(db.Integer, nullable=False)

    saldo_eup = db.Column(db.Numeric, default=0)
    saldo_gb = db.Column(db.Numeric, default=0)
    saldo_tmb1 = db.Column(db.Numeric, default=0)
    saldo_tmb2 = db.Column(db.Numeric, default=0)

    period_end = db.Column(db.DateTime, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    partner = db.relationship("Partner", back_populates="month_closures")


# -------------------- Hilfsfunktionen --------------------

def month_range(dt_date: date):
    """Monatsanfang und Monatsende für ein gegebenes Datum."""
    start = datetime(dt_date.year, dt_date.month, 1)
    if dt_date.month == 12:
        next_m = datetime(dt_date.year + 1, 1, 1)
    else:
        next_m = datetime(dt_date.year, dt_date.month + 1, 1)
    end = next_m - timedelta(seconds=1)
    return start, end


def parse_date_or_none(s):
    """Parst YYYY-MM-DD oder gibt None zurück."""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


def get_last_closure_before(partner_id, dt: datetime):
    """Letzten Monatsabschluss vor dem angegebenen Datum ermitteln."""
    return (
        MonthClosure.query
        .filter_by(partner_id=partner_id)
        .filter(MonthClosure.period_end < dt)
        .order_by(MonthClosure.period_end.desc())
        .first()
    )


def collect_partner_entries(partner: Partner):
    """Alle Buchungen über alle Konten eines Partners sammeln."""
    all_entries = []
    for acc in partner.accounts:
        all_entries.extend(acc.entries)
    return all_entries


def calculate_saldo_and_sums(partner_id, start_date: datetime, end_date: datetime):
    """
    Berechnet:
      entries         – Buchungen im Zeitraum
      saldo_start     – Anfangssaldo
      movement        – Bewegung im Zeitraum
      saldo_end       – Endsaldo
      sums_eingang    – Summen 'Eingang' nach Lademittel-Arten
      sums_ausgang    – Summen 'Ausgang' nach Lademittel-Arten
    unter Berücksichtigung des letzten Monatsabschlusses.
    """
    partner = Partner.query.get(partner_id)
    if not partner:
        return None

    all_entries = collect_partner_entries(partner)

    # Basiswert aus letztem Monatsabschluss
    last_closure = get_last_closure_before(partner_id, start_date)
    if last_closure:
        saldo_start = {
            "eup": float(last_closure.saldo_eup or 0),
            "gb": float(last_closure.saldo_gb or 0),
            "tmb1": float(last_closure.saldo_tmb1 or 0),
            "tmb2": float(last_closure.saldo_tmb2 or 0),
        }
        base_date = last_closure.period_end
    else:
        saldo_start = {"eup": 0, "gb": 0, "tmb1": 0, "tmb2": 0}
        base_date = datetime.min

    movement = {"eup": 0, "gb": 0, "tmb1": 0, "tmb2": 0}
    sums_eingang = {"eup": 0, "gb": 0, "tmb1": 0, "tmb2": 0}
    sums_ausgang = {"eup": 0, "gb": 0, "tmb1": 0, "tmb2": 0}
    entries_in_period = []

    for e in all_entries:
        if not e.datum:
            continue

        # alles bis base_date ist bereits im letzten Monatsabschluss enthalten
        if e.datum <= base_date:
            continue

        if e.richtung == "Eingang":
            mult = 1
        elif e.richtung == "Ausgang":
            mult = -1
        elif e.richtung == "Korrektur":
            # Korrekturen: Vorzeichen steckt in der Menge selbst
            mult = 1
        else:
            mult = 1

        me_eup = float(e.menge_eup or 0)
        me_gb = float(e.menge_gb or 0)
        me_tmb1 = float(e.menge_tmb1 or 0)
        me_tmb2 = float(e.menge_tmb2 or 0)

        # Aufholen vom Monatsabschluss bis zum Beginn des Zeitraums
        if base_date < e.datum < start_date:
            saldo_start["eup"] += me_eup * mult
            saldo_start["gb"] += me_gb * mult
            saldo_start["tmb1"] += me_tmb1 * mult
            saldo_start["tmb2"] += me_tmb2 * mult

        # innerhalb des betrachteten Zeitraums
        elif start_date <= e.datum <= end_date:
            entries_in_period.append(e)

            movement["eup"] += me_eup * mult
            movement["gb"] += me_gb * mult
            movement["tmb1"] += me_tmb1 * mult
            movement["tmb2"] += me_tmb2 * mult

            if e.richtung == "Eingang":
                sums_eingang["eup"] += me_eup
                sums_eingang["gb"] += me_gb
                sums_eingang["tmb1"] += me_tmb1
                sums_eingang["tmb2"] += me_tmb2
            elif e.richtung == "Ausgang":
                sums_ausgang["eup"] += me_eup
                sums_ausgang["gb"] += me_gb
                sums_ausgang["tmb1"] += me_tmb1
                sums_ausgang["tmb2"] += me_tmb2
            # Korrektur wird nur in Bewegung/Saldo berücksichtigt, nicht in sums_*

    # neueste Buchungen oben
    entries_in_period.sort(key=lambda x: x.datum or datetime.min, reverse=True)

    saldo_end = {
        "eup": saldo_start["eup"] + movement["eup"],
        "gb": saldo_start["gb"] + movement["gb"],
        "tmb1": saldo_start["tmb1"] + movement["tmb1"],
        "tmb2": saldo_start["tmb2"] + movement["tmb2"],
    }

    return {
        "entries": entries_in_period,
        "saldo_start": saldo_start,
        "movement": movement,
        "saldo_end": saldo_end,
        "sums_eingang": sums_eingang,
        "sums_ausgang": sums_ausgang,
    }


# -------------------- Hauptbildschirm --------------------

@app.route("/")
def index():
    q = (request.args.get("q") or "").strip().lower()
    partners_query = Partner.query
    if q:
        partners_query = partners_query.filter(Partner.name.ilike(f"%{q}%"))

    partners = partners_query.all()
    data = []

    for p in partners:
        eup = gb = tmb1 = tmb2 = 0
        for acc in p.accounts:
            for e in acc.entries:
                if e.richtung == "Eingang":
                    mult = 1
                elif e.richtung == "Ausgang":
                    mult = -1
                elif e.richtung == "Korrektur":
                    mult = 1
                else:
                    mult = 1
                eup += float(e.menge_eup or 0) * mult
                gb += float(e.menge_gb or 0) * mult
                tmb1 += float(e.menge_tmb1 or 0) * mult
                tmb2 += float(e.menge_tmb2 or 0) * mult

        data.append(
            {
                "id": p.id,
                "name": p.name,
                "saldo_eup": round(eup, 2),
                "saldo_gb": round(gb, 2),
                "saldo_tmb1": round(tmb1, 2),
                "saldo_tmb2": round(tmb2, 2),
            }
        )

    return render_template("index.html", partners=data, q=q)


# -------------------- PALLETTENKONTO --------------------

@app.route("/partner/<int:partner_id>")
def partner_detail(partner_id):
    partner = Partner.query.get_or_404(partner_id)

    today = date.today()
    default_start, default_end = month_range(today)

    # --------- Datum + Filterlogik ----------
    start_date_param = parse_date_or_none(request.args.get("start_date"))
    end_date_param   = parse_date_or_none(request.args.get("end_date"))

    year_str = (request.args.get("year") or "").strip()
    month_str = (request.args.get("month") or "").strip()
    richtung_filter = request.args.get("richtung") or "ALLE"

    # 1️⃣ Wenn Zeitraum manuell ausgewählt → diesen verwenden
    if start_date_param and end_date_param:
        start_date = start_date_param
        end_date   = end_date_param
        used_year  = start_date.year
        used_month = start_date.month

    # 2️⃣ Wenn Jahr/Monat gewählt → Monatsansicht
    elif year_str or month_str:
        try:
            used_year = int(year_str) if year_str else today.year
        except:
            used_year = today.year

        try:
            used_month = int(month_str) if month_str else 1
        except:
            used_month = 1

        start_date, end_date = month_range(date(used_year, used_month, 1))

    # 3️⃣ Standard → aktueller Monat
    else:
        start_date = default_start
        end_date = default_end
        used_year = start_date.year
        used_month = start_date.month

    # ----- Berechnungen -----
    result = calculate_saldo_and_sums(partner_id, start_date, end_date)
    if not result:
        flash("Partner nicht gefunden", "error")
        return redirect(url_for("index"))

    entries = result["entries"]

    # Richtung-Filter
    if richtung_filter in ("Eingang", "Ausgang", "Korrektur"):
        entries = [e for e in entries if e.richtung == richtung_filter]

    # Summen für die Tabelle (wie Excel)
    totals = {"eup": 0, "gb": 0, "tmb1": 0, "tmb2": 0}
    for e in entries:
        totals["eup"]  += float(e.menge_eup or 0)
        totals["gb"]   += float(e.menge_gb or 0)
        totals["tmb1"] += float(e.menge_tmb1 or 0)
        totals["tmb2"] += float(e.menge_tmb2 or 0)

    for k in totals:
        totals[k] = round(totals[k], 2)

    # Liste aller Jahre für Drop-down
    all_entries = collect_partner_entries(partner)
    years_list = sorted({e.datum.year for e in all_entries if e.datum})

    # Kann der Monat geschlossen werden?
    ms, me = month_range(start_date.date())
    is_full_month = (start_date == ms and end_date == me)

    first_of_current = date(today.year, today.month, 1)
    existing_closure = MonthClosure.query.filter_by(
        partner_id=partner_id, year=start_date.year, month=start_date.month
    ).first()

    can_close_month = (
        is_full_month
        and end_date.date() < first_of_current
        and existing_closure is None
    )

    selected_month_closed = existing_closure is not None

    # Schneller Zugriff: Vormonat
    prev_ref = (ms - timedelta(days=1)).date()
    prev_month_start, prev_month_end = month_range(prev_ref)

    year_start = datetime(today.year, 1, 1)
    year_end   = datetime(today.year, 12, 31, 23, 59, 59)

    return render_template(
        "partner_detail.html",
        partner=partner,
        entries=entries,
        start_saldo=result["saldo_start"],
        movement=result["movement"],
        end_saldo=result["saldo_end"],
        sums_eingang=result["sums_eingang"],
        sums_ausgang=result["sums_ausgang"],
        totals=totals,
        start_date=start_date,
        end_date=end_date,
        years_list=years_list,
        selected_year=used_year,
        selected_month=used_month,
        richtung_filter=richtung_filter,
        current_year=today.year,
        current_month=today.month,
        prev_year=prev_month_start.year,
        prev_month=prev_month_start.month,
        year_start_str=year_start.strftime("%Y-%m-%d"),
        year_end_str=year_end.strftime("%Y-%m-%d"),
        can_close_month=can_close_month,
        close_year=start_date.year,
        close_month=start_date.month,
        selected_month_closed=selected_month_closed,
    )


# -------------------- MONATSABSCHLUSS --------------------

@app.route("/partner/<int:partner_id>/close_month", methods=["POST"])
def close_month(partner_id):
    """Führt den Monatsabschluss für einen Partner aus."""
    year = int(request.form.get("year"))
    month = int(request.form.get("month"))

    partner = Partner.query.get_or_404(partner_id)

    existing = MonthClosure.query.filter_by(
        partner_id=partner_id, year=year, month=month
    ).first()
    if existing:
        flash("Monat ist bereits abgeschlossen.", "info")
        return redirect(url_for("partner_detail", partner_id=partner_id))

    period_ref = date(year, month, 1)
    period_start, period_end = month_range(period_ref)

    # Salden bis einschließlich Monatsende berechnen
    result = calculate_saldo_and_sums(partner_id, datetime.min, period_end)
    saldo = result["saldo_end"]

    closure = MonthClosure(
        partner_id=partner_id,
        year=year,
        month=month,
        saldo_eup=saldo["eup"],
        saldo_gb=saldo["gb"],
        saldo_tmb1=saldo["tmb1"],
        saldo_tmb2=saldo["tmb2"],
        period_end=period_end,
        created_at=datetime.utcnow(),
    )

    db.session.add(closure)
    db.session.commit()

    flash("Monatsabschluss erfolgreich durchgeführt.", "success")

    return redirect(
        url_for(
            "partner_detail",
            partner_id=partner_id,
            year=year,
            month=month,
        )
    )


# -------------------- NEUE BUCHUNG --------------------

@app.route("/partner/<int:partner_id>/new_entry", methods=["GET", "POST"])
def new_entry(partner_id):
    """Erfassung einer neuen Buchung für einen Partner."""
    partner = Partner.query.get_or_404(partner_id)
    if not partner.accounts:
        flash("Kein Konto für diesen Partner vorhanden.", "error")
        return redirect(url_for("partner_detail", partner_id=partner.id))

    account = partner.accounts[0]

    def render_form(state=None):
        """Formular mit aktuellem Zustand rendern."""
        if state is None:
            state = {
                "richtung": "AUS",
                "typ": "EUP",
                "menge": "",
                "kommentar": "",
                "datum": datetime.now().strftime("%Y-%m-%d"),
            }
        return render_template(
            "new_entry.html",
            partner=partner,
            form_state=state,
        )

    if request.method == "GET":
        return render_form()

    richtung_raw = request.form.get("richtung")
    typ = request.form.get("typ")
    menge_str = (request.form.get("menge") or "").strip()
    kommentar = (request.form.get("kommentar") or "").strip()
    datum_str = (request.form.get("datum") or "").strip()

    state = {
        "richtung": richtung_raw,
        "typ": typ,
        "menge": menge_str,
        "kommentar": kommentar,
        "datum": datum_str or datetime.now().strftime("%Y-%m-%d"),
    }

    # Datum: vom Nutzer eingegebenes Datum + aktuelle Zeit
    try:
        user_date = datetime.strptime(state["datum"], "%Y-%m-%d").date()
        current_time = datetime.now().time()
        entry_date = datetime.combine(user_date, current_time)
    except ValueError:
        flash("Fehler! Datum ist ungültig.", "error")
        return render_form(state)

    # Nicht in einen bereits abgeschlossenen Monat buchen
    last_close = (
        MonthClosure.query.filter_by(partner_id=partner.id)
        .order_by(MonthClosure.period_end.desc())
        .first()
    )
    if last_close and entry_date <= last_close.period_end:
        flash(
            "Fehler! Monat ist bereits abgeschlossen. Buchungen nur im aktuellen Zeitraum erlaubt.",
            "error",
        )
        return render_form(state)

    # Menge
    try:
        menge = int(menge_str)
    except ValueError:
        flash("Fehler! Menge ist ungültig.", "error")
        return render_form(state)

    # Richtung: nur Eingang / Ausgang
    mapping = {"EIN": "Eingang", "AUS": "Ausgang"}
    richtung_db = mapping.get((richtung_raw or "").upper())
    if richtung_db not in ("Eingang", "Ausgang"):
        flash("Fehler! Ungültige Richtung.", "error")
        return render_form(state)

    # Ausgang → im Kommentar muss eine Nummer (≥ 4 Ziffern) stehen
    if richtung_db == "Ausgang":
        if not re.search(r"\d{4,}", kommentar or ""):
            flash(
                "Fehler! Bei Ausgang muss im Kommentar eine Nummer mit mindestens 4 Ziffern stehen.",
                "error",
            )
            return render_form(state)

    # Verteilung der Menge auf Lademitteltypen
    menge_eup = menge_gb = menge_tmb1 = menge_tmb2 = 0
    if typ == "EUP":
        menge_eup = menge
    elif typ == "GB":
        menge_gb = menge
    elif typ == "TMB1":
        menge_tmb1 = menge
    elif typ == "TMB2":
        menge_tmb2 = menge

    # Belegnummer / Konto-Nr (für neue Standardbuchung immer KontoSeq = 0)
    today_str = datetime.now().strftime("%Y%m%d")
    count_today = (
        Entry.query.filter(Entry.belegnummer.like(f"{today_str}%")).count() + 1
    )
    belegnummer = f"{today_str}{count_today:02d}"
    konto_seq = 0

    new_e = Entry(
        belegnummer=belegnummer,
        datum=entry_date,
        richtung=richtung_db,
        menge_eup=menge_eup,
        menge_gb=menge_gb,
        menge_tmb1=menge_tmb1,
        menge_tmb2=menge_tmb2,
        kommentar=kommentar,
        konto_seq=konto_seq,
        erfasst_von="Taach",
        account_id=account.id,
    )

    db.session.add(new_e)
    db.session.commit()

    return redirect(url_for("partner_detail", partner_id=partner.id))


# -------------------- KORREKTURBUCHUNG --------------------

@app.route("/partner/<int:partner_id>/correction_entry", methods=["GET", "POST"])
def correction_entry(partner_id):
    """Erfassung einer Korrekturbuchung zu einer vorhandenen Belegnummer."""
    partner = Partner.query.get_or_404(partner_id)
    if not partner.accounts:
        flash("Kein Konto für diesen Partner vorhanden.", "error")
        return redirect(url_for("partner_detail", partner_id=partner.id))

    account = partner.accounts[0]

    def render_form(state=None):
        """Formular für Korrekturbuchung rendern."""
        if state is None:
            state = {
                "belegnummer": "",
                "datum": datetime.now().strftime("%Y-%m-%d"),
                "typ": "EUP",
                "menge": "",
                "kommentar": "",
            }
        return render_template(
            "correction_entry.html",
            partner=partner,
            form_state=state,
        )

    if request.method == "GET":
        return render_form()

    belegnummer = (request.form.get("belegnummer") or "").strip()
    datum_str = (request.form.get("datum") or "").strip()
    typ = request.form.get("typ")
    menge_str = (request.form.get("menge") or "").strip()
    kommentar = (request.form.get("kommentar") or "").strip()

    state = {
        "belegnummer": belegnummer,
        "datum": datum_str or datetime.now().strftime("%Y-%m-%d"),
        "typ": typ,
        "menge": menge_str,
        "kommentar": kommentar,
    }

    if not belegnummer:
        flash("Fehler! Belegnummer fehlt.", "error")
        return render_form(state)

    if not kommentar:
        flash("Fehler! Kommentar fehlt.", "error")
        return render_form(state)

    # Datum
    try:
        user_date = datetime.strptime(state["datum"], "%Y-%m-%d").date()
        current_time = datetime.now().time()
        entry_date = datetime.combine(user_date, current_time)
    except ValueError:
        flash("Fehler! Datum ist ungültig.", "error")
        return render_form(state)

    # Ursprüngliche Buchung (letzte Eingang/Ausgang mit dieser Belegnummer)
    original = (
        Entry.query.join(Account, Entry.account_id == Account.id)
        .filter(Account.partner_id == partner.id)
        .filter(Entry.belegnummer == belegnummer)
        .filter(Entry.richtung.in_(["Eingang", "Ausgang"]))
        .order_by(Entry.konto_seq.desc(), Entry.id.desc())
        .first()
    )

    if not original:
        flash(f"Fehler! Belegnummer {belegnummer} nicht gefunden.", "error")
        return render_form(state)

    # Keine Korrektur für Buchungen in einem bereits abgeschlossenen Monat
    last_close = (
        MonthClosure.query.filter_by(partner_id=partner.id)
        .order_by(MonthClosure.period_end.desc())
        .first()
    )
    if last_close and original.datum <= last_close.period_end:
        flash(
            "Fehler! Monat der Originalbuchung ist abgeschlossen. Korrektur nicht möglich.",
            "error",
        )
        return render_form(state)

    # Menge
    try:
        menge = int(menge_str)
    except ValueError:
        flash("Fehler! Menge ist ungültig.", "error")
        return render_form(state)

    # Vorzeichen für Korrektur: entgegengesetzt zur ursprünglichen Buchung
    if original.richtung == "Ausgang":
        korr_menge = menge      # Plus
    else:  # Eingang
        korr_menge = -menge     # Minus

    menge_eup = menge_gb = menge_tmb1 = menge_tmb2 = 0
    if typ == "EUP":
        menge_eup = korr_menge
    elif typ == "GB":
        menge_gb = korr_menge
    elif typ == "TMB1":
        menge_tmb1 = korr_menge
    elif typ == "TMB2":
        menge_tmb2 = korr_menge

    # Neuer Konto-Seq = letzter für diese Belegnummer + 1
    last_for_beleg = (
        Entry.query.join(Account, Entry.account_id == Account.id)
        .filter(Account.partner_id == partner.id)
        .filter(Entry.belegnummer == belegnummer)
        .order_by(Entry.konto_seq.desc())
        .first()
    )
    next_seq = (last_for_beleg.konto_seq or 0) + 1 if last_for_beleg else 1

    new_e = Entry(
        belegnummer=belegnummer,
        datum=entry_date,
        richtung="Korrektur",
        menge_eup=menge_eup,
        menge_gb=menge_gb,
        menge_tmb1=menge_tmb1,
        menge_tmb2=menge_tmb2,
        kommentar=kommentar,
        konto_seq=next_seq,
        erfasst_von="Taach",
        account_id=account.id,
    )

    db.session.add(new_e)
    db.session.commit()

    return redirect(url_for("partner_detail", partner_id=partner.id))


# -------------------- EXPORT EXCEL --------------------

@app.route("/partner/<int:partner_id>/export_excel")
def export_excel(partner_id):
    """Export der Buchungen in Excel für den gewählten Zeitraum."""
    start_date = parse_date_or_none(request.args.get("start_date"))
    end_date = parse_date_or_none(request.args.get("end_date"))
    richtung_filter = request.args.get("richtung") or "ALLE"

    if not start_date or not end_date:
        flash("Ungültiger Zeitraum für Export.", "error")
        return redirect(url_for("partner_detail", partner_id=partner_id))

    partner = Partner.query.get_or_404(partner_id)
    result = calculate_saldo_and_sums(partner_id, start_date, end_date)

    entries = result["entries"]

    if richtung_filter in ("Eingang", "Ausgang", "Korrektur"):
        entries = [e for e in entries if e.richtung == richtung_filter]

    wb = Workbook()
    ws = wb.active
    ws.title = "Buchungen"

    ws.append(
        [
            "Datum",
            "Belegnummer",
            "Konto Nr",
            "Richtung",
            "EUP",
            "GB",
            "TMB1",
            "TMB2",
            "Kommentar",
            "Erfasst von",
        ]
    )

    for e in entries:
        ws.append(
            [
                e.datum.strftime("%d.%m.%Y %H:%M") if e.datum else "",
                e.belegnummer,
                e.konto_seq,
                e.richtung,
                float(e.menge_eup or 0),
                float(e.menge_gb or 0),
                float(e.menge_tmb1 or 0),
                float(e.menge_tmb2 or 0),
                e.kommentar or "",
                e.erfasst_von or "",
            ]
        )

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"Palettenkonto_{partner.name}_{start_date.date()}_{end_date.date()}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# -------------------- PALETTENKONTO-AUSZUG (Deckblatt + Anlage) --------------------

@app.route("/partner/<int:partner_id>/auszug_pdf")
def export_auszug_pdf(partner_id):
    """PDF-Auszug (Deckblatt + Anlagen-Tabelle) für einen Partner."""
    start_date = parse_date_or_none(request.args.get("start_date"))
    end_date = parse_date_or_none(request.args.get("end_date"))

    if not start_date or not end_date:
        flash("Ungültiger Zeitraum für Export.", "error")
        return redirect(url_for("partner_detail", partner_id=partner_id))

    partner = Partner.query.get_or_404(partner_id)
    result = calculate_saldo_and_sums(partner_id, start_date, end_date)

    # Alle relevanten Buchungen (Eingang, Ausgang, Korrektur)
    entries = [
        e for e in result["entries"]
        if e.richtung in ("Eingang", "Ausgang", "Korrektur")
    ]
    entries.sort(key=lambda e: e.datum or datetime.min)

    # Salden gesamt (Summe aus allen Lademitteln)
    def sum_dict(d):
        return (
            float(d.get("eup", 0) or 0) +
            float(d.get("gb", 0) or 0) +
            float(d.get("tmb1", 0) or 0) +
            float(d.get("tmb2", 0) or 0)
        )

    saldo_start_total = sum_dict(result["saldo_start"])
    saldo_end_total   = sum_dict(result["saldo_end"])
    sum_eing_total    = sum_dict(result["sums_eingang"])
    sum_ausg_total    = sum_dict(result["sums_ausgang"])

    # Zeitraum-Text
    if start_date.date() == end_date.date():
        period_str = start_date.strftime("%d.%m.%Y")
    else:
        period_str = f"{start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}"

    # PDF erstellen
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    x_margin = 40
    y = height - 40

    # ======= DECKBLATT =======
    pdf.setFont("Helvetica", 9)
    pdf.drawString(x_margin, y, "expert Warenvertrieb · Postfach 1680 · 30837 Langenhagen")
    y -= 35

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(x_margin, y, partner.name or "")

    pdf.setFont("Helvetica", 10)
    pdf.drawRightString(width - x_margin, y, f"Auszugsdatum: {date.today().strftime('%d.%m.%Y')}")
    y -= 25

    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(x_margin, y, "PACKMITTEL-KONTO")
    y -= 30

    pdf.setFont("Helvetica", 10)
    pdf.drawString(x_margin, y, "Hiermit bestätigen wir Ihnen den unten anstehenden Saldo Ihres")
    y -= 14
    pdf.drawString(x_margin, y, f"Packmittelkontos per {period_str}.")
    y -= 30

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(x_margin, y, "Saldovortrag Anfang:")
    pdf.drawRightString(width - x_margin, y, f"{saldo_start_total:.2f}")
    y -= 18

    pdf.drawString(x_margin, y, "Summe Eingang:")
    pdf.drawRightString(width - x_margin, y, f"{sum_eing_total:.2f}")
    y -= 18

    pdf.drawString(x_margin, y, "Summe Ausgang:")
    pdf.drawRightString(width - x_margin, y, f"{sum_ausg_total:.2f}")
    y -= 18

    pdf.drawString(x_margin, y, "Saldo Ende:")
    pdf.drawRightString(width - x_margin, y, f"{saldo_end_total:.2f}")
    y -= 40

    pdf.setFont("Helvetica", 10)
    pdf.drawString(x_margin, y, "Unterschrift Partner:")
    pdf.line(x_margin, y - 5, x_margin + 200, y - 5)

    pdf.drawString(width / 2 + 20, y, "Unterschrift Mitarbeiter:")
    pdf.line(width / 2 + 20, y - 5, width / 2 + 220, y - 5)

    pdf.showPage()

    # ======= ANLAGE – Tabelle =======
    table_x = 40
    table_y = height - 60
    base_row_h = 15
    table_width = width - 2 * table_x

    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(table_x, table_y, "Anlage – Buchungsübersicht")
    table_y -= 20

    # Spaltenkoordinaten
    col_date    = table_x + 4
    col_beleg   = table_x + 80
    col_konto   = table_x + 150
    col_richt   = table_x + 200
    col_eup     = table_x + 260
    col_gb      = table_x + 305
    col_tmb1    = table_x + 350
    col_tmb2    = table_x + 395
    col_comment = table_x + 440

    def draw_header(y_pos):
        """Tabellenkopf zeichnen."""
        pdf.setFillColorRGB(0.15, 0.32, 0.70)
        pdf.rect(table_x, y_pos, table_width, base_row_h, fill=1, stroke=1)

        pdf.setFillColorRGB(1, 1, 1)
        pdf.setFont("Helvetica-Bold", 9)

        pdf.drawString(col_date,    y_pos + 4, "Datum")
        pdf.drawString(col_beleg,   y_pos + 4, "Belegnummer")
        pdf.drawString(col_konto,   y_pos + 4, "Konto Nr")
        pdf.drawString(col_richt,   y_pos + 4, "Richtung")
        pdf.drawString(col_eup,     y_pos + 4, "EUP")
        pdf.drawString(col_gb,      y_pos + 4, "GB")
        pdf.drawString(col_tmb1,    y_pos + 4, "TMB1")
        pdf.drawString(col_tmb2,    y_pos + 4, "TMB2")
        pdf.drawString(col_comment, y_pos + 4, "Kommentar")

        pdf.setFillColorRGB(0, 0, 0)

    # Kopf einmal zeichnen
    draw_header(table_y)
    table_y -= base_row_h
    pdf.setFont("Helvetica", 8)

    # Tabellenzeilen
    for idx, e in enumerate(entries):
        full_comment = (e.kommentar or "").strip()

        # Kommentar-Wrap (max. 3 Zeilen)
        comment_width = (table_x + table_width) - col_comment - 5
        wrapped_comment = simpleSplit(full_comment, "Helvetica", 8, comment_width)
        wrapped_comment = wrapped_comment[:3] if wrapped_comment else [""]

        needed_height = base_row_h * len(wrapped_comment)

        # Neue Seite bei Platzmangel
        if table_y - needed_height < 50:
            pdf.showPage()
            table_y = height - 60
            pdf.setFont("Helvetica-Bold", 12)
            pdf.drawString(table_x, table_y, "Anlage – Buchungsübersicht (Fortsetzung)")
            table_y -= 20
            draw_header(table_y)
            table_y -= base_row_h
            pdf.setFont("Helvetica", 8)

        # Zebra-Hintergrund
        if idx % 2 == 0:
            pdf.setFillColorRGB(0.95, 0.97, 1.0)
        else:
            pdf.setFillColorRGB(1, 1, 1)

        pdf.rect(
            table_x,
            table_y - (needed_height - base_row_h),
            table_width,
            needed_height,
            fill=1,
            stroke=1
        )

        pdf.setFillColorRGB(0, 0, 0)

        datum = e.datum.strftime("%d.%m.%Y %H:%M") if e.datum else ""

        pdf.drawString(col_date,  table_y + 3, datum)
        pdf.drawString(col_beleg, table_y + 3, e.belegnummer or "")
        pdf.drawString(col_konto, table_y + 3, str(e.konto_seq or 0))
        pdf.drawString(col_richt, table_y + 3, e.richtung or "")

        pdf.drawRightString(col_eup + 30,  table_y + 3, f"{float(e.menge_eup or 0):.2f}")
        pdf.drawRightString(col_gb + 30,   table_y + 3, f"{float(e.menge_gb or 0):.2f}")
        pdf.drawRightString(col_tmb1 + 30, table_y + 3, f"{float(e.menge_tmb1 or 0):.2f}")
        pdf.drawRightString(col_tmb2 + 30, table_y + 3, f"{float(e.menge_tmb2 or 0):.2f}")

        comment_y = table_y + 3
        for line in wrapped_comment:
            pdf.drawString(col_comment, comment_y, line)
            comment_y -= base_row_h

        table_y -= needed_height

    pdf.setStrokeColorRGB(0, 0, 0)
    pdf.line(table_x, table_y, table_x + table_width, table_y)

    pdf.showPage()
    pdf.save()
    buffer.seek(0)

    filename = f"Palettenkonto_Auszug_{partner.name}_{start_date.date()}_{end_date.date()}.pdf"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/pdf",
    )


# -------------------- PALETTENSCHEIN FÜR EINE BUCHUNG --------------------

@app.route("/entry/<int:entry_id>/palettenschein")
def palettenschein(entry_id):
    """Erzeugt einen Palettenschein (PDF) für eine einzelne Buchung."""
    entry = Entry.query.get_or_404(entry_id)
    partner = entry.account.partner if entry.account else None

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    x, y = 40, height - 40

    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(x, y, "Palettenschein")
    y -= 30

    pdf.setFont("Helvetica", 11)
    if partner:
        pdf.drawString(x, y, f"Partner: {partner.name}")
        y -= 18

    pdf.drawString(x, y, f"Belegnummer: {entry.belegnummer or ''}")
    y -= 18

    pdf.drawString(
        x,
        y,
        f"Buchungsdatum: {entry.datum.strftime('%d.%m.%Y %H:%M') if entry.datum else ''}",
    )
    y -= 18

    pdf.drawString(x, y, f"Richtung: {entry.richtung or ''}")
    y -= 25

    # Lademittel + Mengen
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(x, y, "Lademittel:")
    y -= 16

    pdf.setFont("Helvetica", 11)
    lademittel = []
    if entry.menge_eup:
        lademittel.append(("EUP", float(entry.menge_eup)))
    if entry.menge_gb:
        lademittel.append(("GB", float(entry.menge_gb)))
    if entry.menge_tmb1:
        lademittel.append(("TMB1", float(entry.menge_tmb1)))
    if entry.menge_tmb2:
        lademittel.append(("TMB2", float(entry.menge_tmb2)))

    if not lademittel:
        pdf.drawString(x, y, "keine Mengen erfasst")
        y -= 20
    else:
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(x, y, "Art")
        pdf.drawString(x + 80, y, "Menge")
        y -= 14
        pdf.setFont("Helvetica", 10)
        for name, menge in lademittel:
            pdf.drawString(x, y, name)
            pdf.drawString(x + 80, y, f"{menge:.2f}")
            y -= 14
        y -= 10

    if entry.kommentar:
        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(x, y, "Kommentar:")
        y -= 14
        pdf.setFont("Helvetica", 10)
        pdf.drawString(x, y, entry.kommentar[:90])
        y -= 20

    # Ausstellungsdatum
    pdf.setFont("Helvetica", 11)
    pdf.drawString(
        x,
        y,
        f"Ausstellungsdatum: {date.today().strftime('%d.%m.%Y')}",
    )
    y -= 40

    # Unterschriften
    pdf.setFont("Helvetica", 10)
    pdf.drawString(x, y, "Unterschrift Partner:")
    pdf.line(x, y - 5, x + 200, y - 5)

    pdf.drawString(x + 260, y, "Unterschrift Mitarbeiter:")
    pdf.line(x + 260, y - 5, x + 460, y - 5)

    pdf.showPage()
    pdf.save()
    buffer.seek(0)

    filename = f"Palettenschein_{entry.belegnummer or entry.id}.pdf"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/pdf",
    )


# -------------------- NEUER PARTNER --------------------

@app.route("/partner/new", methods=["GET", "POST"])
def new_partner():
    """
    Anlage eines neuen Partners + automatisches Anlegen eines leeren Kontos.
    """
    if request.method == "GET":
        return render_template("new_partner.html")

    name = (request.form.get("name") or "").strip()

    if not name:
        flash("Fehler! Partnername fehlt.", "error")
        return render_template("new_partner.html", name=name)

    # Partner anlegen
    partner = Partner(name=name)
    db.session.add(partner)
    db.session.flush()  # damit partner.id verfügbar ist

    # Mindestens ein Konto für diesen Partner anlegen
    account = Account(partner_id=partner.id)
    db.session.add(account)

    db.session.commit()

    flash("Partner wurde angelegt.", "success")
    return redirect(url_for("partner_detail", partner_id=partner.id))


# -------------------- PARTNER LÖSCHEN --------------------

@app.route("/partner/<int:partner_id>/delete", methods=["POST"])
def delete_partner(partner_id):
    """
    Löscht einen Partner inklusive Konten, Buchungen und Monatsabschlüssen.
    """
    partner = Partner.query.get_or_404(partner_id)

    # Alle Monatsabschlüsse löschen
    MonthClosure.query.filter_by(partner_id=partner.id).delete()

    # Alle Buchungen der Konten dieses Partners löschen
    for acc in partner.accounts:
        Entry.query.filter_by(account_id=acc.id).delete()

    # Alle Konten des Partners löschen
    Account.query.filter_by(partner_id=partner.id).delete()

    # Partner selbst löschen
    db.session.delete(partner)
    db.session.commit()

    flash("Partner wurde gelöscht.", "success")
    return redirect(url_for("index"))


# -------------------- START --------------------

if __name__ == "__main__":
    # Für die erste Initialisierung kann man temporär aktivieren:
    # with app.app_context():
    #     db.create_all()
    app.run(debug=True)


