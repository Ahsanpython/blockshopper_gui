import re
import time
import random
import threading
from datetime import datetime
from urllib.parse import urljoin
from queue import Queue, Empty
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ============= Constants & UI Lists =============
BASE = "https://blockshopper.com"
CITY_BASE = f"{BASE}/ca/contra-costa-county/cities"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/119.0.0.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_CITIES = [
    "lafayette", "moraga", "orinda", "walnut creek",
    "danville", "san ramon", "pleasanton", "alamo"
]

# ============= Common helpers (unchanged behavior) =============
def slugify_city(city: str) -> str:
    c = city.strip().lower()
    c = re.sub(r"\s+", "-", c)
    return c

def fetch(url, retry=2, backoff=1.6):
    for attempt in range(retry + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=45)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.text
        except Exception:
            if attempt == retry:
                return None
            time.sleep(backoff * (attempt + 1) + random.random())

def text(el):
    return el.get_text(strip=True) if el else ""

def paginate_collect(start_url: str, collector_fn, *collector_args):
    page = 1
    all_items = set()
    while True:
        url = start_url if page == 1 else f"{start_url}?page={page}"
        html = fetch(url)
        if not html:
            break
        page_items = collector_fn(html, *collector_args)
        new_items = page_items - all_items
        if not new_items:
            break
        all_items.update(new_items)
        time.sleep(0.5 + random.random() * 0.8)
        page += 1
    return all_items

# ============= Stage 1: street URLs =============
def extract_street_links(html: str, city_slug: str):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    pattern = re.compile(
        rf"^/ca/contra-costa-county/cities/{re.escape(city_slug)}/streets/[^/]+/?$"
    )
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if pattern.match(href):
            links.add(urljoin(BASE, href))
    return links

def crawl_city_streets(city_slug: str):
    city_index = f"{CITY_BASE}/{city_slug}"
    return paginate_collect(city_index, extract_street_links, city_slug)

# ============= Stage 2: property URLs =============
def extract_property_links(html: str, city_slug: str):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    pattern = re.compile(
        rf"^/ca/contra-costa-county/{re.escape(city_slug)}/property/\d+/[^/]+/?$"
    )
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if pattern.match(href):
            links.add(urljoin(BASE, href))
    return links

def crawl_street_properties(street_url: str, city_slug: str):
    return paginate_collect(street_url, extract_property_links, city_slug)

# ============= Your parsing logic (as-is) =============
def clean_money_to_int(txt):
    if not txt or "N/A" in txt:
        return None
    m = re.sub(r"[^\d.]", "", txt)
    return int(float(m)) if m else None

def fmt_money(val):
    return "" if val is None else "${:,}".format(int(val))

def parse_address_parts(soup):
    street = city = state = zipc = None
    for h5 in soup.select(".presenter-info h5"):
        label = text(h5.find("span")) or ""
        val   = text(h5.find("a"))
        if not val: continue
        if label.strip() == "City":  city  = val
        elif label.strip() == "State": state = val
        elif label.strip() == "Zip":   zipc  = val

    h1_hidden = text(soup.select_one(".main-title h1.d-none"))
    h2_addr   = text(soup.select_one(".navbar-center address h2")) or text(soup.select_one(".main-title h2"))
    street_candidate = (h1_hidden or h2_addr or "").split(",")[0].strip()
    if street_candidate:
        street = street_candidate

    if not (city and state and zipc):
        subtitle = text(soup.select_one(".navbar-center address h3")) or text(soup.select_one(".main-title h2"))
        m = re.search(r"([^,]+),\s*([A-Za-z.]+)\s+(\d{5}(?:-\d{4})?)", subtitle or "")
        if m:
            if not city:  city  = m.group(1).strip()
            if not state: state = m.group(2).strip()
            if not zipc:  zipc  = m.group(3).strip()

    if state == "CA":
        state = "California"
    return street or "", city or "", state or "", zipc or ""

def parse_current_owners(soup):
    info = soup.select_one("section#property-info")
    if info:
        for row in info.select("div.row"):
            label = text(row.select_one(".info-type"))
            val   = text(row.select_one(".info-data"))
            if label and "Current Owners" in label and val:
                return val
    return ""

def party_box_text(box):
    if not box: return ""
    s = box.get_text(" ", strip=True)
    s = re.sub(r'^\s*(Buyer|Seller)\s*:?\s*', '', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def parse_date_to_dt(date_txt):
    if not date_txt: return None
    norm = date_txt.replace("Sept.", "Sep.")
    for fmt in ("%b. %d, %Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(norm, fmt)
        except ValueError:
            continue
    return None

def collect_all_sales(soup):
    events = []
    for card in soup.select("#property-sales .timeline article.card"):
        dates   = card.select("p.sale-date")
        prices  = card.select("p.sale-price")
        buyers  = card.select(".sale-people .sale-buyer")
        sellers = card.select(".sale-people .sale-seller")
        n = max(len(dates), len(prices), len(buyers), len(sellers), 1)
        for i in range(n):
            date_txt   = text(dates[i]) if i < len(dates) else ""
            price_txt  = text(prices[i]) if i < len(prices) else ""
            buyer_txt  = party_box_text(buyers[i])  if i < len(buyers)  else ""
            seller_txt = party_box_text(sellers[i]) if i < len(sellers) else ""
            events.append({
                "date_text": date_txt,
                "date_dt":   parse_date_to_dt(date_txt),
                "price":     clean_money_to_int(price_txt),
                "buyer":     buyer_txt,
                "seller":    seller_txt
            })
    uniq, seen = [], set()
    for e in events:
        key = (e["date_text"], e["buyer"], e["seller"], e["price"])
        if key not in seen:
            uniq.append(e); seen.add(key)
    uniq.sort(key=lambda r: (r["date_dt"] is None, r["date_dt"]))
    return uniq

ORG_NOISE_WORDS = ["trust","trustee","living","revocable"]
STOP = {'the','and','et','al','jr','sr','i','ii','iii','iv','v','ua','fbo','buyer','seller','family'}

def _norm(s: str) -> str:
    if not s: return ""
    s = s.lower().replace("&", " and ")
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def clean_person_segment(seg: str) -> str:
    seg = re.sub(r"\([^)]*\)", " ", seg)
    seg = re.sub(r"\b(" + "|".join(ORG_NOISE_WORDS) + r")\b", " ", seg, flags=re.I)
    seg = re.sub(r"\b\d{2,4}\b", " ", seg)
    seg = re.sub(r"\s+", " ", seg).strip()
    return seg

def _segments_people(s: str):
    if not s: return []
    parts = re.split(r"\s*(?:,|&| and )\s*", s)
    keep = []
    for p in parts:
        p2 = clean_person_segment(p)
        if re.search(r"[A-Za-z]", p2):
            keep.append(p2)
    return keep

def _tokens(s: str) -> set:
    if not s: return set()
    s = _norm(s)
    return {t for t in s.split() if len(t) > 1 and t not in STOP}

def person_tokens(s: str) -> set:
    toks = set()
    for seg in _segments_people(s):
        toks |= _tokens(seg)
    return toks

def last_names(s: str) -> set:
    outs = set()
    for seg in _segments_people(s):
        words = [w for w in re.findall(r"[A-Za-z]+", seg) if w.lower() not in STOP]
        if words: outs.add(words[-1].lower())
    return {x for x in outs if len(x) >= 3}

def first_names(s: str) -> set:
    outs = set()
    for seg in _segments_people(s):
        words = [w for w in re.findall(r"[A-Za-z]+", seg) if w.lower() not in STOP]
        if words: outs.add(words[0].lower())
    return {x for x in outs if len(x) >= 2}

def pick_original_purchase(current_owners: str, all_sales: list[dict]) -> dict | None:
    cur_first  = first_names(current_owners)
    cur_people = person_tokens(current_owners)
    cur_last   = last_names(current_owners)
    owners_have_org = any(w in _norm(current_owners) for w in ORG_NOISE_WORDS)

    if cur_first:
        for ev in all_sales:
            if cur_first.issubset(_tokens(ev["buyer"])):
                return ev
    if cur_people:
        for ev in all_sales:
            if person_tokens(ev["buyer"]) == cur_people:
                return ev
    if cur_last:
        for ev in all_sales:
            if cur_last.issubset(_tokens(ev["buyer"])):
                return ev
    if cur_people:
        for ev in all_sales:
            bt = person_tokens(ev["buyer"])
            if bt and len(cur_people & bt) >= 2:
                return ev
    if not owners_have_org:
        cur_full = _norm(current_owners)
        for ev in all_sales:
            if _norm(ev["buyer"]) == cur_full:
                return ev
    return None

MONTH_MAP = {
    "jan":"January","jan.":"January","feb":"February","feb.":"February",
    "mar":"March","mar.":"March","apr":"April","apr.":"April","may":"May",
    "jun":"June","jun.":"June","jul":"July","jul.":"July","aug":"August","aug.":"August",
    "sep":"September","sept":"September","sep.":"September","sept.":"September",
    "oct":"October","oct.":"October","nov":"November","nov.":"November","dec":"December","dec.":"December",
}
def split_date_parts(date_txt: str):
    if not date_txt:
        return "", None
    m = re.search(r"([A-Za-z\.]+)\s+\d{1,2},\s*(\d{4})", date_txt)
    if not m:
        return "", None
    return MONTH_MAP.get(m.group(1).lower(), m.group(1).title()), int(m.group(2))

def parse_property_live(url: str) -> dict:
    html = fetch(url)
    if not html:
        return {
            "Current Owners": "",
            "Original Purchase Price": "",
            "Purchase Date": "",
            "Purchase Month": "",
            "Purchase Year": None,
            "Buyer Name": "",
            "Seller Name": "",
            "Street": "",
            "City": "",
            "State": "",
            "Zip": "",
            "Address": "",
            "Property URL": url,
        }
    soup = BeautifulSoup(html, "html.parser")
    street, city, state, zipc = parse_address_parts(soup)
    current = parse_current_owners(soup)
    all_sales = collect_all_sales(soup)
    chosen = pick_original_purchase(current, all_sales) if all_sales else None
    purchase_date = chosen["date_text"] if chosen else ""
    month, year = split_date_parts(purchase_date)
    price = fmt_money(chosen["price"]) if chosen else ""
    buyer = chosen["buyer"] if chosen else ""
    seller = chosen["seller"] if chosen else ""
    return {
        "Current Owners": current,
        "Original Purchase Price": price,
        "Purchase Date": purchase_date,
        "Purchase Month": month,
        "Purchase Year": year,
        "Buyer Name": buyer,
        "Seller Name": seller,
        "Street": street,
        "City": city,
        "State": state,
        "Zip": zipc,
        "Address": ", ".join([p for p in [street, city, state, zipc] if p]),
        "Property URL": url,
    }

# ============= Worker that runs crawl + parse, emits progress to queue =============
def run_scrape(cities, out_csv, progress_q, stop_flag):
    all_rows = []
    try:
        for city in cities:
            city_slug = slugify_city(city)
            # Stage 1: Streets
            street_urls = crawl_city_streets(city_slug)

            city_property_urls = set()
            # For each street: collect property URLs and emit per-street count (number only)
            for street_url in sorted(street_urls):
                if stop_flag["stop"]: break
                prop_urls = crawl_street_properties(street_url, city_slug)
                progress_q.put(("street_count", len(prop_urls)))
                city_property_urls.update(prop_urls)
            if stop_flag["stop"]: break

            # City total property URLs (number only)
            city_total = len(city_property_urls)
            progress_q.put(("city_total", (city_slug, city_total)))

            # Stage 3: Parse each property
            prop_urls_sorted = sorted(city_property_urls)
            total = len(prop_urls_sorted)
            for idx, url in enumerate(prop_urls_sorted, start=1):
                if stop_flag["stop"]: break
                row = parse_property_live(url)
                all_rows.append(row)
                left = total - idx
                progress_q.put(("property_progress", (idx, left)))
                time.sleep(1.2 + random.random())
            if stop_flag["stop"]: break
        # Save CSV
        if all_rows and not stop_flag["stop"]:
            cols = ["Current Owners","Original Purchase Price","Purchase Date","Purchase Month","Purchase Year",
                    "Buyer Name","Seller Name","Street","City","State","Zip","Address","Property URL"]
            pd.DataFrame(all_rows, columns=cols).to_csv(out_csv, index=False)
            progress_q.put(("saved", out_csv))
        else:
            progress_q.put(("saved", ""))  # nothing saved (stopped/empty)
    except Exception as e:
        progress_q.put(("error", str(e)))
    finally:
        progress_q.put(("done", None))

# ============= Tkinter UI =============
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("BlockShopper Scraper")
        self.geometry("760x560")
        self.minsize(720, 520)
        self.configure(bg="#0f172a")  # slate-900 vibe

        # state
        self.worker = None
        self.progress_q = Queue()
        self.stop_flag = {"stop": False}

        # --- Top frame: city selection ---
        top = ttk.Frame(self)
        top.pack(fill="x", padx=16, pady=12)

        ttk.Label(top, text="Select Cities:", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w")
        self.city_vars = []
        for i, name in enumerate(DEFAULT_CITIES):
            var = tk.BooleanVar(value=(i == 0))  # preselect first
            cb = ttk.Checkbutton(top, text=name, variable=var)
            cb.grid(row=1 + i // 4, column=i % 4, sticky="w", padx=8, pady=2)
            self.city_vars.append((name, var))

        ttk.Label(top, text="Custom (comma-separated):").grid(row=4, column=0, sticky="w", pady=(8, 0))
        self.custom_entry = ttk.Entry(top, width=80)
        self.custom_entry.grid(row=5, column=0, columnspan=4, sticky="we", pady=4)

        # --- Output file chooser ---
        outf = ttk.Frame(self)
        outf.pack(fill="x", padx=16, pady=8)
        ttk.Label(outf, text="Output CSV:").grid(row=0, column=0, sticky="w")
        self.out_path = tk.StringVar(value=os.path.abspath("client_output_split.csv"))
        self.out_entry = ttk.Entry(outf, textvariable=self.out_path, width=64)
        self.out_entry.grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(outf, text="Browse…", command=self.browse_out).grid(row=0, column=2)
        outf.columnconfigure(1, weight=1)

        # --- Controls ---
        ctrls = ttk.Frame(self)
        ctrls.pack(fill="x", padx=16, pady=8)
        self.start_btn = ttk.Button(ctrls, text="Start", command=self.on_start)
        self.stop_btn  = ttk.Button(ctrls, text="Stop", command=self.on_stop, state="disabled")
        self.start_btn.pack(side="left")
        self.stop_btn.pack(side="left", padx=8)

        # --- Progress boxes ---
        body = ttk.Frame(self)
        body.pack(fill="both", expand=True, padx=16, pady=10)

        # numbers list (minimal numeric lines like your terminal output)
        ttk.Label(body, text="Numeric Progress:", font=("Segoe UI", 10, "bold")).pack(anchor="w")
        self.numbox = tk.Text(body, height=10, bg="#0b1220", fg="#e5e7eb", insertbackground="white")
        self.numbox.pack(fill="x", pady=(4,8))

        # overall bars and labels
        bars = ttk.Frame(body)
        bars.pack(fill="x", pady=4)

        self.city_label = ttk.Label(bars, text="City total: -")
        self.city_label.grid(row=0, column=0, sticky="w")

        self.prop_label = ttk.Label(bars, text="Property progress: 0 done / 0 left")
        self.prop_label.grid(row=1, column=0, sticky="w", pady=(6,0))

        self.style = ttk.Style(self)
        self.style.theme_use("clam")
        self.style.configure("TFrame", background="#0f172a")
        self.style.configure("TLabel", background="#0f172a", foreground="#e5e7eb")
        self.style.configure("TCheckbutton", background="#0f172a", foreground="#e5e7eb")
        self.style.configure("TButton", padding=6)
        # rounded-ish feel (limited in ttk but cleaner theme)
        # Text widget already styled above

        # poll progress
        self.after(120, self.drain_queue)

    def browse_out(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile="client_output_split.csv",
            title="Save output CSV"
        )
        if path:
            self.out_path.set(path)

    def gather_cities(self):
        selected = [name for name, var in self.city_vars if var.get()]
        custom = [c.strip() for c in self.custom_entry.get().split(",") if c.strip()]
        # de-dupe while keeping order
        seen = set(); out = []
        for c in selected + custom:
            slug = slugify_city(c)
            if slug and slug not in seen:
                out.append(slug); seen.add(slug)
        return out

    def on_start(self):
        cities = self.gather_cities()
        if not cities:
            messagebox.showwarning("Cities required", "Please select or enter at least one city.")
            return
        out_csv = self.out_path.get().strip()
        if not out_csv:
            messagebox.showwarning("Output path", "Please choose an output CSV path.")
            return
        self.stop_flag["stop"] = False
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.numbox.delete("1.0", "end")
        self.city_label.config(text="City total: -")
        self.prop_label.config(text="Property progress: 0 done / 0 left")

        self.worker = threading.Thread(
            target=run_scrape,
            args=(cities, out_csv, self.progress_q, self.stop_flag),
            daemon=True
        )
        self.worker.start()

    def on_stop(self):
        self.stop_flag["stop"] = True
        self.stop_btn.config(state="disabled")

    def append_numline(self, line):
        self.numbox.insert("end", str(line) + "\n")
        self.numbox.see("end")

    def drain_queue(self):
        try:
            while True:
                typ, payload = self.progress_q.get_nowait()
                if typ == "street_count":
                    # print number only (per street)
                    self.append_numline(payload)
                elif typ == "city_total":
                    city_slug, total = payload
                    self.append_numline(total)  # number line like terminal
                    self.city_label.config(text=f"City total ({city_slug}): {total}")
                    # reset property progress for next stage
                    self.prop_label.config(text="Property progress: 0 done / %d left" % total)
                elif typ == "property_progress":
                    done, left = payload
                    self.append_numline(f"{done} {left}")  # "done left" numbers
                    self.prop_label.config(text=f"Property progress: {done} done / {left} left")
                elif typ == "saved":
                    out_csv = payload
                    if out_csv:
                        self.append_numline(f"Saved -> {out_csv}")
                elif typ == "error":
                    messagebox.showerror("Error", payload)
                elif typ == "done":
                    self.start_btn.config(state="normal")
                    self.stop_btn.config(state="disabled")
        except Empty:
            pass
        self.after(120, self.drain_queue)

if __name__ == "__main__":
    app = App()
    app.mainloop()
