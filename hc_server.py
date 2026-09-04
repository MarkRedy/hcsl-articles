# -*- coding: utf-8 -*-
"""
行测申论 · 每日时政积累 —— 本地后端
纯 Python 标准库，无第三方依赖。

用法：
  python hc_server.py          # 启动服务 http://127.0.0.1:8642 （启动时自动抓取当天缺失的数据）
  python hc_server.py update   # 只抓取一次并入库后退出（可挂 Windows 计划任务）

数据保存在同目录 hc_articles.db（SQLite）：文章按 URL 去重、永久累积；
收藏、来源配置也存库里，换浏览器打开同样生效。
"""
import hashlib
import hmac
import json
import os
import re
import secrets
import html as htmllib
import sqlite3
import sys
import threading
import time
import urllib.request
import email.utils
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "hc_articles.db")
HOST, PORT = "127.0.0.1", 8642
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
HTML_FILES = {"行测申论每日积累.html", "行测申论每日积累-离线版.html"}

# ---------------- 来源配置（可在此增删；页面“源设置”里也能改，改完存进数据库） ----------------
CN = "https://www.chinanews.com.cn/rss/"
DEFAULT_SOURCES = [
    {"id": "cn-scroll",  "name": "中新网·即时要闻", "type": "rss",  "url": CN + "scroll-news.xml", "on": True},
    {"id": "cn-china",   "name": "中新网·国内时政", "type": "rss",  "url": CN + "china.xml",       "on": True},
    {"id": "cn-world",   "name": "中新网·国际视野", "type": "rss",  "url": CN + "world.xml",       "on": True},
    {"id": "cn-fz",      "name": "中新网·法治中国", "type": "rss",  "url": CN + "fz.xml",          "on": True},
    {"id": "cn-culture", "name": "中新网·文化文艺", "type": "rss",  "url": CN + "culture.xml",     "on": True},
    {"id": "cn-edu",     "name": "中新网·教育科技", "type": "rss",  "url": CN + "edu.xml",         "on": True},
    {"id": "cn-finance", "name": "中新网·财经经济", "type": "rss",  "url": CN + "finance.xml",     "on": True},
    {"id": "cn-society", "name": "中新网·社会民生", "type": "rss",  "url": CN + "society.xml",     "on": True},
    {"id": "cn-life",    "name": "中新网·生活健康", "type": "rss",  "url": CN + "life.xml",        "on": False},
    {"id": "pp-op",      "name": "人民日报·人民时评", "type": "html", "url": "http://opinion.people.com.cn/", "on": True, "parse": "ppop"},
    {"id": "sh-gov",     "name": "上海市政府·要闻动态", "type": "html", "url": "https://www.shanghai.gov.cn/nw2315/index.html", "on": True, "parse": "shgov"},
    {"id": "gov-zc",     "name": "国务院·最新政策", "type": "html", "url": "https://www.gov.cn/zhengce/zuixin/", "on": False, "parse": "govzc"},
]

# ---------------- 自动打标（关键词 → 标签） ----------------
TAG_RULES = [
    ("上海",     r"上海|沪上|浦东|临港|长三角|进博会|黄浦江|苏州河|崇明|虹桥|洋山|一网通办"),
    ("常识·法律", r"法庭?|法院|检察院|立法|草案|民法典|刑法|反诈|治安|律师|司法|普法|维权|仲裁|判决|条例"),
    ("常识·科技", r"航天|发射|嫦娥|天问|空间站|北斗|卫星|量子|芯片|人工智能|AI|大模型|机器人|深海|超导|诺贝尔|科研|5G|6G|算力|新能源|探月|祝融"),
    ("常识·人文", r"文物|考古|遗址|非遗|世界遗产|博物馆|节气|申遗|典籍|汉字|古籍|传统村落|文化遗产|历史"),
    ("政策热点", r"印发|出台|施行|征求意见|规划|方案|意见|部署|会议|发布会|国务院|中央|改革|纲要|白皮书"),
    ("民生热点", r"就业|毕业生|医疗|养老|托育|住房|生育|社保|物价|食品安全|交通|快递|外卖|教育|双减|医保|老旧小区|充电|停车"),
    ("经济金融", r"经济|消费|外贸|出口|企业|市场|投资|金融|民营|制造业|服务业|文旅|假日经济|数字"),
    ("国际视野", r"美国|日本|俄罗斯|欧盟|联合国|世卫|外交部|外交|国际|全球|中法|中美|中俄|中东|东盟"),
]
FANWEN_RE = re.compile(r"人民时评|壹时评|人民论坛|观点频道|评论员")

# ---------------- 数据库 ----------------
def db():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS articles(
        id TEXT PRIMARY KEY, title TEXT NOT NULL, url TEXT UNIQUE NOT NULL,
        source TEXT NOT NULL, src_id TEXT NOT NULL, summary TEXT DEFAULT '',
        pub_ts INTEGER, tags TEXT DEFAULT '[]', title_norm TEXT, first_seen TEXT NOT NULL)""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_seen ON articles(first_seen)")
    conn.execute("""CREATE TABLE IF NOT EXISTS favs(
        id TEXT NOT NULL, title TEXT, url TEXT, source TEXT, summary TEXT,
        tags TEXT, pub_ts INTEGER, saved_ts INTEGER, user_id INTEGER DEFAULT 0,
        PRIMARY KEY(id, user_id))""")
    conn.execute("CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT)")
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL,
        pw_hash TEXT NOT NULL, salt TEXT NOT NULL, created_ts INTEGER)""")
    conn.execute("CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, created_ts INTEGER)")
    # 旧库升级：favs 补 user_id 列；老结构主键只有 id，多用户会互相覆盖 → 重建为复合主键
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(favs)")]
    pk_cols = [r["name"] for r in conn.execute("PRAGMA table_info(favs)") if r["pk"]]
    if "user_id" not in cols or pk_cols != ["id", "user_id"]:
        if "user_id" not in cols:
            conn.execute("ALTER TABLE favs ADD COLUMN user_id INTEGER DEFAULT 0")
        conn.execute("DROP TABLE IF EXISTS favs_new")
        conn.execute("""CREATE TABLE favs_new(
            id TEXT NOT NULL, title TEXT, url TEXT, source TEXT, summary TEXT,
            tags TEXT, pub_ts INTEGER, saved_ts INTEGER, user_id INTEGER DEFAULT 0,
            PRIMARY KEY(id, user_id))""")
        conn.execute("INSERT INTO favs_new(id,title,url,source,summary,tags,pub_ts,saved_ts,user_id) "
                     "SELECT id,title,url,source,summary,tags,pub_ts,saved_ts,user_id FROM favs")
        conn.execute("DROP TABLE favs")
        conn.execute("ALTER TABLE favs_new RENAME TO favs")
    conn.commit()
    return conn

def kv_get(conn, k, default=None):
    row = conn.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return json.loads(row["v"]) if row else default

def kv_set(conn, k, v):
    conn.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, json.dumps(v, ensure_ascii=False)))
    conn.commit()

# ---------------- 用户与会话 ----------------
def hash_pw(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100000).hex()

def create_user(conn, username, password):
    username = (username or "").strip()
    if not re.fullmatch(r"[\w\u4e00-\u9fa5-]{2,20}", username):
        raise ValueError("用户名需 2-20 位，可用中英文、数字、下划线、连字符")
    if len(password or "") < 6:
        raise ValueError("密码至少 6 位")
    if conn.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone():
        raise ValueError("用户名已被注册")
    first = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0
    salt = secrets.token_hex(8)
    cur = conn.execute("INSERT INTO users(username,pw_hash,salt,created_ts) VALUES(?,?,?,?)",
                       (username, hash_pw(password, salt), salt, int(time.time())))
    if first:  # 首个注册用户继承此前（无主）收藏
        conn.execute("UPDATE favs SET user_id=? WHERE user_id=0", (cur.lastrowid,))
    conn.commit()
    return conn.execute("SELECT * FROM users WHERE id=?", (cur.lastrowid,)).fetchone()

def verify_login(conn, username, password):
    row = conn.execute("SELECT * FROM users WHERE username=?", ((username or "").strip(),)).fetchone()
    if row and hmac.compare_digest(row["pw_hash"], hash_pw(password or "", row["salt"])):
        return row
    return None

def new_session(conn, user_id):
    conn.execute("DELETE FROM sessions WHERE created_ts < ?", (int(time.time()) - 30 * 86400,))
    token = secrets.token_hex(16)
    conn.execute("INSERT INTO sessions(token,user_id,created_ts) VALUES(?,?,?)", (token, user_id, int(time.time())))
    conn.commit()
    return token

def str_hash(s):
    h = 5381
    for ch in s:
        h = ((h * 33) + ord(ch)) & 0xFFFFFFFF
    return format(h, "x")

def title_norm(t):
    return re.sub(r"[\s\W_]+", "", t, flags=re.UNICODE)

def clean_text(s):
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", htmllib.unescape(s)).strip()

def parse_when(pub, url):
    if pub:
        pub = pub.strip()
        m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2}))?", pub)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hh = int(m.group(4) or 9); mi = int(m.group(5) or 0)
            try:
                return int(datetime(y, mo, d, hh, mi).timestamp() * 1000)
            except ValueError:
                pass
        try:
            return int(email.utils.parsedate_to_datetime(pub).timestamp() * 1000)
        except Exception:
            pass
    if url:
        m = re.search(r"/(\d{4})(\d{2})(\d{2})/", url)
        if m:
            try:
                return int(datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), 9).timestamp() * 1000)
            except ValueError:
                pass
        m = re.search(r"/n1/(\d{4})/(\d{2})(\d{2})/", url)
        if m:
            try:
                return int(datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), 9).timestamp() * 1000)
            except ValueError:
                pass
    return int(time.time() * 1000)

def tag_it(title, url, summary):
    hay = title + " " + summary
    tags = []
    if FANWEN_RE.search(title) or "opinion.people.com.cn/n1" in url:
        tags.append("申论范文")
    for tag, pat in TAG_RULES:
        if re.search(pat, hay) and tag not in tags:
            tags.append(tag)
    return tags

# ---------------- 抓取 ----------------
def _raw_get(url, timeout):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Accept-Encoding": "identity",
        "Accept": "text/html,application/xml,application/json;q=0.9,*/*;q=0.8",
        "Connection": "close",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def http_get(url, timeout=20):
    """带硬超时的抓取：urlopen 的超时覆盖不了 DNS/SSL 握手挂起，用工作线程兜底。"""
    result = {}
    def work():
        try:
            result["text"] = _raw_get(url, timeout)
        except Exception as e:
            result["err"] = e
    th = threading.Thread(target=work, daemon=True)
    th.start()
    th.join(timeout + 10)
    if "text" in result:
        return result["text"]
    if "err" in result:
        raise result["err"]
    raise TimeoutError("抓取超时（>%.0fs）: %s" % (timeout + 10, url))

def parse_rss(text):
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []
    out = []
    for n in root.iter():
        local = n.tag.rsplit("}", 1)[-1]
        if local not in ("item", "entry"):
            continue
        def first(name, el=n):
            for c in el.iter():
                if c is not el and c.tag.rsplit("}", 1)[-1] == name:
                    return c
            return None
        t = first("title")
        title = clean_text("".join(t.itertext()) if t is not None else "")
        le = first("link")
        url = ""
        if le is not None:
            url = (le.get("href") or (le.text or "")).strip()
        de = first("description") or first("summary") or first("content")
        summary = clean_text("".join(de.itertext()))[:170] if de is not None else ""
        pe = first("pubDate") or first("published") or first("updated")
        pub = (pe.text or "").strip() if pe is not None else ""
        if title and url:
            out.append({"title": title, "url": url, "summary": summary, "pub": pub})
    return out

A_TAG = re.compile(r"<a\s+([^>]*?)>(.*?)</a>", re.S | re.I)

def iter_links(html_text):
    for m in A_TAG.finditer(html_text):
        attrs, inner = m.group(1), m.group(2)
        h = re.search(r'href\s*=\s*"([^"]+)"', attrs, re.I) or re.search(r"href\s*=\s*'([^']+)'", attrs, re.I)
        if not h:
            continue
        t = re.search(r'title\s*=\s*"([^"]+)"', attrs, re.I) or re.search(r"title\s*=\s*'([^']+)'", attrs, re.I)
        title = clean_text(t.group(1) if t else inner)
        yield h.group(1), title

def parse_ppop(text):
    out = []
    for href, title in iter_links(text):
        if not re.search(r"opinion\.people\.com\.cn/n1/\d{4}/\d{4}/[A-Za-z0-9-]+\.s?html", href, re.I):
            continue
        if len(title) < 8:
            continue
        out.append({"title": title, "url": href if href.startswith("http") else "http:" + href, "summary": "", "pub": ""})
    return out

def parse_shgov(text):
    out = []
    for href, title in iter_links(text):
        m = re.match(r"^/(nw\d+)/(\d{8})/([0-9a-f]+)\.html$", href, re.I)
        if m:
            if len(title) < 6:
                continue
            out.append({"title": title, "url": "https://www.shanghai.gov.cn" + href, "summary": "", "pub": m.group(2), "extra": "上海市政府"})
            continue
        if re.search(r"gov\.cn/(yaowen|home|zhengce)/.*content_\d+\.htm", href):
            if len(title) < 8:
                continue
            out.append({"title": title, "url": href, "summary": "", "pub": "", "extra": "国务院要闻"})
    return out

def parse_govzc(text):
    out = []
    for href, title in iter_links(text):
        if not re.search(r"content_\d+\.htm", href):
            continue
        if len(title) < 6:
            continue
        out.append({"title": title, "url": href if href.startswith("http") else "https://www.gov.cn" + href, "summary": "", "pub": ""})
    return out

HTML_PARSERS = {"ppop": parse_ppop, "shgov": parse_shgov, "govzc": parse_govzc}

class Fetcher:
    def __init__(self):
        self.lock = threading.Lock()

    def log(self, conn, msg):
        st = kv_get(conn, "status", {"running": True, "log": []})
        st["log"].append(time.strftime("[%H:%M:%S] ") + msg)
        st["log"] = st["log"][-60:]
        kv_set(conn, "status", st)
        print(msg, flush=True)

    def running(self, conn):
        st = kv_get(conn, "status", {})
        return bool(st.get("running"))

    def fetch_all(self, force=False):
        with self.lock:
            conn = db()
            try:
                st = kv_get(conn, "status", {})
                # 运行锁自愈：上次任务超过 15 分钟仍标记 running 视为卡死，允许重跑
                stuck = st.get("running") and (time.time() - st.get("start", 0)) > 900
                if st.get("running") and not stuck and not force:
                    return False, "已有抓取任务在进行中"
                today = time.strftime("%Y-%m-%d")
                kv_set(conn, "status", {"running": True, "log": [], "start": time.time()})
                self.log(conn, "开始抓取 " + today)
                sources = [s for s in kv_get(conn, "sources", None) or DEFAULT_SOURCES if s.get("on", True)]
                stats = {}
                for s in sources:
                    try:
                        if s["type"] == "rss":
                            raw = parse_rss(http_get(s["url"]))
                            via = "RSS"
                        else:
                            raw = HTML_PARSERS.get(s.get("parse", ""), lambda t: [])(http_get(s["url"]))
                            via = "网页"
                        n = self.insert(conn, s, raw)
                        stats[s["id"]] = {"st": "ok", "n": n, "via": via}
                        self.log(conn, "%s：%d 条（新增 %d）" % (s["name"], len(raw), n))
                    except Exception as e:
                        stats[s["id"]] = {"st": "fail", "n": 0, "via": str(e)[:80]}
                        self.log(conn, "%s：失败（%s）" % (s["name"], str(e)[:80]))
                kv_set(conn, "last_stats", stats)
                kv_set(conn, "last_fetch", int(time.time() * 1000))
                kv_set(conn, "last_update_date", today)
                st = kv_get(conn, "status")
                st["running"] = False
                total = conn.execute("SELECT COUNT(*) c FROM articles WHERE first_seen=?", (today,)).fetchone()["c"]
                self.log(conn, "抓取完成，今日共 %d 条" % total)
                kv_set(conn, "status", st)
                return True, "完成"
            finally:
                conn.close()

    def insert(self, conn, src, raw_items):
        today = time.strftime("%Y-%m-%d")
        n = 0
        for it in raw_items:
            url = it["url"].strip()
            title = it["title"]
            if not url or not title:
                continue
            tn = title_norm(title)
            dup = conn.execute("SELECT 1 FROM articles WHERE url=? OR title_norm=?", (url, tn)).fetchone()
            if dup:
                continue
            source = it.get("extra") + "（" + re.sub(r"^[^·]*·", "", src["name"]) + "）" if it.get("extra") else src["name"]
            ts = parse_when(it.get("pub", ""), url)
            tags = tag_it(title, url, it.get("summary", ""))
            conn.execute(
                "INSERT OR IGNORE INTO articles(id,title,url,source,src_id,summary,pub_ts,tags,title_norm,first_seen) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (str_hash(url), title, url, source, src["id"], it.get("summary", ""), ts,
                 json.dumps(tags, ensure_ascii=False), tn, today))
            n += 1
        conn.commit()
        return n

FETCHER = Fetcher()

def maybe_autoupdate():
    conn = db()
    try:
        today = time.strftime("%Y-%m-%d")
        cnt = conn.execute("SELECT COUNT(*) c FROM articles WHERE first_seen=?", (today,)).fetchone()["c"]
        last = kv_get(conn, "last_fetch", 0)
        stale = (time.time() * 1000 - last) > 4 * 3600 * 1000
        if cnt == 0 or stale:
            threading.Thread(target=FETCHER.fetch_all, daemon=True).start()
    finally:
        conn.close()

# ---------------- HTTP 服务 ----------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _json(self, obj, code=200, cookie=None, clear_cookie=False):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if cookie:
            self.send_header("Set-Cookie", "hcsid=%s; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000" % cookie)
        if clear_cookie:
            self.send_header("Set-Cookie", "hcsid=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sid(self):
        m = re.search(r"(?:^|;\s*)hcsid=([0-9a-f]+)", self.headers.get("Cookie") or "")
        return m.group(1) if m else None

    def _user(self):
        if not hasattr(self, "_cached_user"):
            conn = db()
            try:
                self._cached_user = None
                if self._sid():
                    self._cached_user = conn.execute(
                        "SELECT u.* FROM users u JOIN sessions s ON s.user_id=u.id WHERE s.token=?",
                        (self._sid(),)).fetchone()
            finally:
                conn.close()
        return self._cached_user

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8"))
        except Exception:
            return None

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs, unquote
        p = urlparse(self.path)
        path = unquote(p.path)
        q = parse_qs(p.query)
        if path.startswith("/api/") and path != "/api/me":
            if not self._user():
                return self._json({"error": "未登录"}, 401)
        try:
            if path in ("/", "/index.html", "/" + "行测申论每日积累.html"):
                return self._serve_html("行测申论每日积累.html")
            if path == "/" + "行测申论每日积累-离线版.html":
                return self._serve_html("行测申论每日积累-离线版.html")
            if path == "/api/me":
                u = self._user()
                return self._json({"user": ({"id": u["id"], "username": u["username"]} if u else None)})
            if path == "/api/articles":
                conn = db()
                try:
                    uid = self._user()["id"]
                    date = (q.get("date") or [time.strftime("%Y-%m-%d")])[0]
                    rows = conn.execute(
                        "SELECT * FROM articles WHERE first_seen=? ORDER BY pub_ts DESC LIMIT 800", (date,)).fetchall()
                    counts = {r["src_id"]: r["c"] for r in conn.execute(
                        "SELECT src_id, COUNT(*) c FROM articles WHERE first_seen=? GROUP BY src_id", (date,))}
                    sources = kv_get(conn, "sources", None) or DEFAULT_SOURCES
                    fav_ids = {r["id"] for r in conn.execute("SELECT id FROM favs WHERE user_id=?", (uid,))}
                    items = [{
                        "id": r["id"], "title": r["title"], "url": r["url"], "source": r["source"],
                        "srcId": r["src_id"], "ts": r["pub_ts"] or 0,
                        "tags": json.loads(r["tags"] or "[]"), "summary": r["summary"] or "",
                        "faved": r["id"] in fav_ids,
                    } for r in rows]
                    return self._json({
                        "date": date, "items": items, "sources": sources,
                        "counts": counts, "lastStats": kv_get(conn, "last_stats", {}),
                        "lastFetch": kv_get(conn, "last_fetch", 0),
                        "favs": {r["id"]: 1 for r in conn.execute("SELECT id FROM favs WHERE user_id=?", (uid,))},
                    })
                finally:
                    conn.close()
            if path == "/api/favs":
                conn = db()
                try:
                    rows = conn.execute("SELECT * FROM favs WHERE user_id=? ORDER BY saved_ts DESC",
                                        (self._user()["id"],)).fetchall()
                    return self._json({"favs": [{
                        "id": r["id"], "title": r["title"], "url": r["url"], "source": r["source"],
                        "ts": r["pub_ts"] or 0, "tags": json.loads(r["tags"] or "[]"),
                        "summary": r["summary"] or "",
                    } for r in rows]})
                finally:
                    conn.close()
            if path == "/api/sources":
                conn = db()
                try:
                    return self._json({"sources": kv_get(conn, "sources", None) or DEFAULT_SOURCES,
                                       "lastStats": kv_get(conn, "last_stats", {})})
                finally:
                    conn.close()
            if path == "/api/status":
                conn = db()
                try:
                    st = kv_get(conn, "status", {"running": False, "log": []})
                    return self._json({"running": st.get("running", False), "log": st.get("log", []),
                                       "lastFetch": kv_get(conn, "last_fetch", 0)})
                finally:
                    conn.close()
            return self._json({"error": "not found"}, 404)
        except Exception as e:
            return self._json({"error": str(e)}, 500)

    def do_POST(self):
        from urllib.parse import urlparse, unquote
        p = urlparse(self.path)
        path = unquote(p.path)
        body = self._body()
        if body is None:
            return self._json({"error": "请求体不是有效的 JSON"}, 400)
        if not isinstance(body, dict):
            return self._json({"error": "请求体必须是 JSON 对象"}, 400)
        if path.startswith("/api/") and path not in ("/api/login", "/api/register", "/api/logout"):
            if not self._user():
                return self._json({"error": "未登录"}, 401)
        try:
            if path == "/api/register":
                conn = db()
                try:
                    u = create_user(conn, body.get("username"), body.get("password"))
                    token = new_session(conn, u["id"])
                    return self._json({"ok": True, "user": {"id": u["id"], "username": u["username"]}}, cookie=token)
                except ValueError as e:
                    return self._json({"error": str(e)}, 400)
                finally:
                    conn.close()
            if path == "/api/login":
                conn = db()
                try:
                    u = verify_login(conn, body.get("username"), body.get("password"))
                    if not u:
                        return self._json({"error": "用户名或密码错误"}, 401)
                    token = new_session(conn, u["id"])
                    return self._json({"ok": True, "user": {"id": u["id"], "username": u["username"]}}, cookie=token)
                finally:
                    conn.close()
            if path == "/api/logout":
                if self._sid():
                    conn = db()
                    try:
                        conn.execute("DELETE FROM sessions WHERE token=?", (self._sid(),))
                        conn.commit()
                    finally:
                        conn.close()
                return self._json({"ok": True}, clear_cookie=True)
            if path == "/api/refresh":
                ok, msg = FETCHER.fetch_all(force=bool(body.get("force")))
                return self._json({"ok": ok, "message": msg}, 200 if ok else 409)
            if path == "/api/fav":
                conn = db()
                try:
                    uid = self._user()["id"]
                    iid = body.get("id") or str_hash(body.get("url", ""))
                    row = conn.execute("SELECT id FROM favs WHERE id=? AND user_id=?", (iid, uid)).fetchone()
                    if row:
                        conn.execute("DELETE FROM favs WHERE id=? AND user_id=?", (iid, uid))
                        conn.commit()
                        return self._json({"faved": False})
                    conn.execute(
                        "INSERT OR REPLACE INTO favs(id,title,url,source,summary,tags,pub_ts,saved_ts,user_id) VALUES(?,?,?,?,?,?,?,?,?)",
                        (iid, body.get("title", ""), body.get("url", ""), body.get("source", ""),
                         body.get("summary", ""), json.dumps(body.get("tags", []), ensure_ascii=False),
                         body.get("ts", 0), int(time.time() * 1000), uid))
                    conn.commit()
                    return self._json({"faved": True})
                finally:
                    conn.close()
            if path == "/api/sources":
                srcs = body.get("sources")
                if not isinstance(srcs, list) or not srcs:
                    return self._json({"error": "sources 必须是非空数组"}, 400)
                conn = db()
                try:
                    kv_set(conn, "sources", srcs)
                    return self._json({"ok": True})
                finally:
                    conn.close()
            if path == "/api/reset-sources":
                conn = db()
                try:
                    kv_set(conn, "sources", DEFAULT_SOURCES)
                    return self._json({"ok": True, "sources": DEFAULT_SOURCES})
                finally:
                    conn.close()
            return self._json({"error": "not found"}, 404)
        except Exception as e:
            return self._json({"error": str(e)}, 500)

    def _serve_html(self, name):
        path = os.path.join(BASE, name)
        if name not in HTML_FILES or not os.path.exists(path):
            return self._json({"error": "not found"}, 404)
        with open(path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def main():
    args = sys.argv[1:]
    host, port = HOST, PORT
    i = 0
    while i < len(args):
        if args[i] == "--host" and i + 1 < len(args):
            host = args[i + 1]; i += 2
        elif args[i] == "--port" and i + 1 < len(args):
            port = int(args[i + 1]); i += 2
        else:
            i += 1
    if args and args[0] == "update":
        ok, msg = FETCHER.fetch_all(force=True)
        print(msg)
        sys.exit(0 if ok else 1)
    maybe_autoupdate()
    print("服务已启动：http://%s:%d  （数据文件 %s）" % (host, port, DB_PATH), flush=True)
    ThreadingHTTPServer((host, port), Handler).serve_forever()

if __name__ == "__main__":
    main()
