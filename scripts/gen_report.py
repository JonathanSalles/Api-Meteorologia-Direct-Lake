"""Gera fundos (SVG -> PNG) + 3 páginas PBIR para DIRECT LAKE METEOROLOGIA API.Report.

Uso: python gen_report.py "<pasta>.Report" <pasta_temporaria>
"""
import json, math, os, shutil, subprocess, sys, secrets, pathlib

REPORT = pathlib.Path(sys.argv[1])
SCRATCH = pathlib.Path(sys.argv[2])
EDGE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

DEF = REPORT / "definition"
RES = REPORT / "StaticResources" / "RegisteredResources"
VC_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.4.0/schema.json"
PAGE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"

# ---- paleta do tema do relatório ----
G_DARK, G, G_LIGHT, G_PALE, G_MIST = "#1F4D19", "#367C2B", "#7DC242", "#A9CC9A", "#D6E8C8"
GOLD, GOLD_L = "#C9A400", "#E8C300"
BLUE, TEAL, PETROL = "#3599B8", "#4AC5BB", "#28738A"
INK, MUTED, LINE, CANVAS = "#1C2A1E", "#68736A", "#E1E5DE", "#F2F4F0"


def hid():
    return secrets.token_hex(10)


def dump(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------- PBIR helpers
def L(v):
    return {"expr": {"Literal": {"Value": v}}}


def C(hexc):
    return {"solid": {"color": L(f"'{hexc}'")}}


def S(s):
    return L("'" + s.replace("'", "''") + "'")


def col(entity, prop):
    return {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def meas(prop):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": "Medidas"}}, "Property": prop}}


def P(field, display=None, fmt=None, active=None):
    if "Measure" in field:
        ent, prop = "Medidas", field["Measure"]["Property"]
    else:
        ent, prop = field["Column"]["Expression"]["SourceRef"]["Entity"], field["Column"]["Property"]
    p = {"field": field, "queryRef": f"{ent}.{prop}", "nativeQueryRef": prop}
    if display:
        p["displayName"] = display
    if fmt:
        p["format"] = fmt
    if active:
        p["active"] = True
    return p


COR_DIA = {"solid": {"color": {"expr": meas("Cor Dia Campo")}}}
WILD = {"dataViewWildcard": {"matchingOption": 1}}

FMT_T = '0.0 "°C"'
FMT_MM = '#,0.0 "mm"'
FMT_PCT = "0.0%"
FMT_D = '0 "dias"'


def container(title=None, bg=True, border=True):
    o = {}
    if title:
        o["title"] = [{"properties": {"show": L("true"), "text": S(title), "fontColor": C(INK),
                                       "fontSize": L("12D"), "bold": L("true"), "alignment": S("left")}}]
    else:
        o["title"] = [{"properties": {"show": L("false")}}]
    o["background"] = [{"properties": {"show": L("true" if bg else "false"), "color": C("#FFFFFF"),
                                        "transparency": L("0D")}}]
    o["border"] = [{"properties": {"show": L("true" if border else "false"), "color": C(LINE),
                                    "width": L("1D"), "radius": L("10D")}}]
    o["padding"] = [{"properties": {k: L(v) for k, v in
                                     dict(top="10D", bottom="8D", left="12D", right="12D").items()}}]
    o["dropShadow"] = [{"properties": {"show": L("false")}}]
    return o


def visual(page_dir, vtype, x, y, w, h, z, query=None, objects=None, vco=None, sort=None):
    name = hid()
    v = {"visualType": vtype}
    if query:
        v["query"] = {"queryState": query}
        if sort:
            v["query"]["sortDefinition"] = {"sort": sort, "isDefaultSort": True}
    if objects:
        v["objects"] = objects
    if vco is not None:
        v["visualContainerObjects"] = vco
    v["drillFilterOtherVisuals"] = True
    dump(page_dir / "visuals" / name / "visual.json", {
        "$schema": VC_SCHEMA, "name": name,
        "position": {"x": x, "y": y, "z": z, "height": h, "width": w, "tabOrder": z},
        "visual": v})


AXES = {
    "categoryAxis": [{"properties": {"showAxisTitle": L("false"), "labelColor": C(MUTED), "fontSize": L("9D")}}],
    "valueAxis": [{"properties": {"showAxisTitle": L("false"), "labelColor": C(MUTED), "fontSize": L("9D"),
                                  "gridlineShow": L("true"), "gridlineColor": C("#EEF1EC"),
                                  "gridlineStyle": S("dotted")}}],
}

# ---------------------------------------------------------------- reusable visuals


def nav(page_dir):
    st = lambda sid, props: {"properties": props, "selector": {"id": sid}}
    visual(page_dir, "pageNavigator", 838, 22, 418, 38, 50000, objects={
        "text": [st("default", {"fontColor": C("#FFFFFF"), "fontSize": L("10D"), "bold": L("true")}),
                 st("selected", {"fontColor": C(G_DARK)}),
                 st("hover", {"fontColor": C("#FFFFFF")})],
        "fill": [st("default", {"show": L("true"), "fillColor": C("#FFFFFF"), "transparency": L("88D")}),
                 st("hover", {"fillColor": C("#FFFFFF"), "transparency": L("72D")}),
                 st("selected", {"fillColor": C(GOLD_L), "transparency": L("0D")})],
        "outline": [st("default", {"show": L("false")})],
        "shape": [{"properties": {"roundEdge": L("18L")}}],
    }, vco={"background": [{"properties": {"show": L("false")}}],
            "border": [{"properties": {"show": L("false")}}],
            "title": [{"properties": {"show": L("false")}}],
            "visualHeader": [{"properties": {"show": L("false")}}]})


def slicer(page_dir, field, display, x, w, z, mode="Dropdown"):
    visual(page_dir, "slicer", x, 96, w, 80, z,
           query={"Values": {"projections": [P(field, display, active=True)]}},
           objects={"data": [{"properties": {"mode": S(mode)}}],
                    "header": [{"properties": {"show": L("true"), "fontColor": C(MUTED), "textSize": L("9D")}}],
                    "items": [{"properties": {"fontColor": C(INK), "textSize": L("11D")}}],
                    "general": [{"properties": {"outlineColor": C(LINE)}}]},
           vco=container())


def kpi(page_dir, m, display, fmt, x, w, z):
    # o "cartão" é desenhado no fundo (SVG); o visual fica transparente, à direita do ícone
    sel = {"id": "default"}
    visual(page_dir, "cardVisual", x + 62, 100, w - 70, 72, z,
           query={"Data": {"projections": [P(meas(m), display, fmt)]}},
           objects={
               "layout": [{"properties": {"columnCount": L("1L"), "autoGrid": L("true")}},
                          {"properties": {"backgroundShow": L("false")}, "selector": sel}],
               "value": [{"properties": {"fontSize": L("20D"), "fontColor": C(INK), "bold": L("true"),
                                         "horizontalAlignment": S("left")}, "selector": sel}],
               "label": [{"properties": {"show": L("true"), "fontSize": L("9D"), "fontColor": C(MUTED),
                                         "position": S("belowValue"), "horizontalAlignment": S("left")},
                          "selector": sel}],
               "outline": [{"properties": {"show": L("false")}, "selector": sel}],
               "divider": [{"properties": {"show": L("false")}, "selector": sel}],
               "accentBar": [{"properties": {"show": L("false")}, "selector": sel}],
               "fillCustom": [{"properties": {"show": L("false")}}],
           },
           vco={"background": [{"properties": {"show": L("false")}}],
                "border": [{"properties": {"show": L("false")}}],
                "title": [{"properties": {"show": L("false")}}],
                "padding": [{"properties": {k: L("0D") for k in ("top", "bottom", "left", "right")}}],
                "visualHeader": [{"properties": {"show": L("false")}}]})


def combo_chuva_temp(page_dir, x, y, w, h, z, title):
    visual(page_dir, "lineClusteredColumnComboChart", x, y, w, h, z,
           query={"Category": {"projections": [P(col("dim_calendario", "data"), "Data", "dd/MM", active=True)]},
                  "Y": {"projections": [P(meas("Chuva Total (mm)"), "Chuva (mm)", FMT_MM)]},
                  "Y2": {"projections": [P(meas("Temp Máx Média"), "Temp. máx (°C)", FMT_T)]}},
           objects={**AXES,
                    "dataPoint": [
                        {"properties": {"fill": COR_DIA},
                         "selector": {"data": [WILD], "metadata": "Medidas.Chuva Total (mm)"}},
                        {"properties": {"fill": C(GOLD)}, "selector": {"metadata": "Medidas.Temp Máx Média"}}],
                    "lineStyles": [{"properties": {"strokeWidth": L("2L"), "lineStyle": S("solid"),
                                                    "showMarker": L("false")}}],
                    "legend": [{"properties": {"show": L("true"), "position": S("TopRight"),
                                               "fontSize": L("9D"), "labelColor": C(MUTED)}}]},
           vco=container(title),
           sort=[{"field": col("dim_calendario", "data"), "direction": "Ascending"}])


# ---------------------------------------------------------------- SVG backgrounds
def icon(kind, cx, cy, color):
    c = color
    if kind == "thermo":
        return (f'<rect x="{cx-3.5}" y="{cy-13}" width="7" height="17" rx="3.5" fill="none" stroke="{c}" stroke-width="2.2"/>'
                f'<circle cx="{cx}" cy="{cy+7}" r="5.5" fill="{c}"/><line x1="{cx}" y1="{cy-5}" x2="{cx}" y2="{cy+4}" stroke="{c}" stroke-width="2.6"/>')
    if kind == "drop":
        return (f'<path d="M{cx},{cy-13} C{cx+10},{cy-1} {cx+9},{cy+11} {cx},{cy+11} C{cx-9},{cy+11} {cx-10},{cy-1} {cx},{cy-13} Z" fill="{c}"/>'
                f'<path d="M{cx-4},{cy+2} q0,5 5,5" fill="none" stroke="#FFFFFF" stroke-width="1.8" stroke-linecap="round"/>')
    if kind == "cloud":
        return (f'<path d="M{cx-11},{cy+2} a6,6 0 0 1 2,-11 a8,8 0 0 1 15,-1 a6,6 0 0 1 5,12 z" fill="{c}"/>'
                + "".join(f'<line x1="{cx-7+i*6}" y1="{cy+6}" x2="{cx-9+i*6}" y2="{cy+12}" stroke="{c}" stroke-width="2" stroke-linecap="round"/>' for i in range(3)))
    if kind == "leaf":
        return (f'<path d="M{cx-11},{cy+11} C{cx-12},{cy-6} {cx-2},{cy-13} {cx+12},{cy-12} C{cx+12},{cy+2} {cx+4},{cy+12} {cx-11},{cy+11} Z" fill="{c}"/>'
                f'<path d="M{cx-11},{cy+11} L{cx+5},{cy-5}" stroke="#FFFFFF" stroke-width="1.8" stroke-linecap="round"/>')
    if kind == "calendar":
        return (f'<rect x="{cx-11}" y="{cy-9}" width="22" height="20" rx="3" fill="none" stroke="{c}" stroke-width="2.2"/>'
                f'<line x1="{cx-11}" y1="{cy-3}" x2="{cx+11}" y2="{cy-3}" stroke="{c}" stroke-width="2.2"/>'
                f'<line x1="{cx-5}" y1="{cy-13}" x2="{cx-5}" y2="{cy-7}" stroke="{c}" stroke-width="2.2" stroke-linecap="round"/>'
                f'<line x1="{cx+5}" y1="{cy-13}" x2="{cx+5}" y2="{cy-7}" stroke="{c}" stroke-width="2.2" stroke-linecap="round"/>'
                f'<path d="M{cx-5},{cy+4} l3,3 l7,-7" fill="none" stroke="{c}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>')
    if kind == "sum":
        return (f'<rect x="{cx-11}" y="{cy+3}" width="5" height="8" rx="1" fill="{c}"/>'
                f'<rect x="{cx-3}" y="{cy-3}" width="5" height="14" rx="1" fill="{c}"/>'
                f'<rect x="{cx+5}" y="{cy-11}" width="5" height="22" rx="1" fill="{c}"/>')
    return ""


def contour_paths(width, height, n, seed, amp, stroke, opacity, x0=0):
    out = []
    for i in range(n):
        base = height * (0.15 + 0.85 * i / max(n - 1, 1))
        pts = []
        for xx in range(0, width + 1, 16):
            yy = (base + amp * math.sin((xx + seed * 37 + i * 55) / 90.0)
                  + amp * 0.6 * math.sin((xx * 1.7 + seed * 11 + i * 23) / 53.0))
            pts.append(f"{x0 + xx:.1f},{yy:.1f}")
        out.append(f'<polyline points="{" ".join(pts)}" fill="none" stroke="{stroke}" '
                   f'stroke-opacity="{opacity}" stroke-width="1.2"/>')
    return "".join(out)


def background_svg(subtitle, kpis, legend=False):
    W, H = 1280, 720
    s = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
         f'font-family="Segoe UI, Segoe UI Semibold, sans-serif">',
         '<defs>'
         f'<linearGradient id="hdr" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="{G_DARK}"/>'
         f'<stop offset="0.65" stop-color="#2A6322"/><stop offset="1" stop-color="{G}"/></linearGradient>'
         f'<linearGradient id="gold" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="{GOLD}"/>'
         f'<stop offset="1" stop-color="{GOLD_L}"/></linearGradient>'
         '<filter id="sh" x="-5%" y="-5%" width="110%" height="130%"><feDropShadow dx="0" dy="1.5" '
         'stdDeviation="2" flood-color="#1F4D19" flood-opacity="0.08"/></filter>'
         '</defs>',
         f'<rect width="{W}" height="{H}" fill="{CANVAS}"/>',
         # curvas de nível bem sutis no corpo (lado direito inferior)
         f'<g>{contour_paths(W, 260, 7, 3, 14, G, 0.05)}</g>'.replace("<polyline", '<polyline transform="translate(0,470)"'),
         # header
         f'<rect width="{W}" height="80" fill="url(#hdr)"/>',
         f'<g>{contour_paths(W, 80, 6, 1, 9, G_LIGHT, 0.16)}</g>',
         f'<rect y="80" width="{W}" height="4" fill="url(#gold)"/>',
         # marca: sol dourado + folha
         f'<g transform="translate(46,40)">'
         + "".join(f'<line x1="0" y1="-21" x2="0" y2="-16" stroke="{GOLD_L}" stroke-width="2.4" stroke-linecap="round" '
                   f'transform="rotate({a})"/>' for a in range(0, 360, 45))
         + f'<circle r="12" fill="{GOLD_L}"/>'
         f'<path d="M-8,10 C-9,-2 -1,-8 10,-8 C10,3 4,10 -8,10 Z" fill="{G_LIGHT}" stroke="{G_DARK}" stroke-width="1.4"/>'
         f'<path d="M-8,10 L4,-2" stroke="{G_DARK}" stroke-width="1.4" stroke-linecap="round"/></g>',
         f'<text x="80" y="38" fill="#FFFFFF" font-size="22" font-weight="600">Clima nas Filiais</text>',
         f'<text x="80" y="60" fill="{G_MIST}" font-size="12">{subtitle}</text>',
         # trilho do navegador
         f'<rect x="830" y="17" width="434" height="48" rx="24" fill="#FFFFFF" fill-opacity="0.07" '
         f'stroke="#FFFFFF" stroke-opacity="0.18"/>',
         ]
    for (x, w, kind, color) in kpis:
        y, h = 96, 80
        s.append(f'<g filter="url(#sh)"><rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="#FFFFFF" '
                 f'stroke="{LINE}"/></g>')
        s.append(f'<path d="M{x+10},{y} h-0 a10,10 0 0 0 -10,10 v{h-20} a10,10 0 0 0 10,10 h4 v-{h} z" fill="{color}"/>')
        s.append(f'<circle cx="{x+36}" cy="{y+40}" r="20" fill="{color}" fill-opacity="0.13"/>')
        s.append(icon(kind, x + 36, y + 40, color))
    s.append(f'<text x="24" y="714" fill="{MUTED}" font-size="9">Fonte: Open-Meteo API · Lakehouse (Direct Lake) · '
             f'Modelo SM_Clima_Filiais</text>')
    s.append("</svg>")
    return "".join(s)


def render_png(svg, out_png):
    html = SCRATCH / (out_png.stem + ".html")
    html.write_text('<!doctype html><html><head><meta charset="utf-8"><style>html,body{margin:0;padding:0;'
                    'overflow:hidden;background:#F2F4F0}svg{display:block}</style></head><body>'
                    + svg + "</body></html>", encoding="utf-8")
    (SCRATCH / (out_png.stem + ".svg")).write_text(svg, encoding="utf-8")
    tmp = SCRATCH / out_png.name
    if tmp.exists():
        tmp.unlink()
    subprocess.run([EDGE, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
                    f"--user-data-dir={SCRATCH / 'edge_profile'}",
                    "--force-device-scale-factor=2", "--window-size=1280,720",
                    f"--screenshot={tmp}", html.as_uri()], check=True, timeout=120,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    shutil.copyfile(tmp, out_png)


# ---------------------------------------------------------------- pages
def kpi_slots(x0, total, n):
    gap = 12
    w = (total - gap * (n - 1)) / n
    return [(round(x0 + i * (w + gap), 2), round(w, 2)) for i in range(n)]


def tipo_filter(value):
    return {"filters": [{
        "name": hid(), "field": col("gold_clima_filial", "tipo"), "type": "Categorical",
        "filter": {"Version": 2, "From": [{"Name": "g", "Entity": "gold_clima_filial", "Type": 0}],
                   "Where": [{"Condition": {"In": {
                       "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "g"}}, "Property": "tipo"}}],
                       "Values": [[{"Literal": {"Value": f"'{value}'"}}]]}}}]}}]}


def write_page(name, display, bg_file, filter_cfg=None):
    pdir = DEF / "pages" / name
    page = {"$schema": PAGE_SCHEMA, "name": name, "displayName": display, "displayOption": "FitToPage",
            "height": 720, "width": 1280,
            "objects": {
                "background": [{"properties": {
                    "image": {"image": {"name": S(bg_file), "url": {"expr": {"ResourcePackageItem": {
                        "PackageName": "RegisteredResources", "PackageType": 1, "ItemName": bg_file}}},
                        "scaling": S("Fit")}},
                    "transparency": L("0D")}}],
                "outspace": [{"properties": {"color": C(CANVAS), "transparency": L("0D")}}]}}
    if filter_cfg:
        page["filterConfig"] = filter_cfg
    dump(pdir / "page.json", page)
    return pdir


def main():
    pages_dir = DEF / "pages"
    if pages_dir.exists():
        shutil.rmtree(pages_dir)
    RES.mkdir(parents=True, exist_ok=True)
    FIL = col("dim_localidade", "filial_nome")

    # ============ 1. VISÃO GERAL ============
    p1 = hid()
    slots = kpi_slots(256, 1000, 5)
    k1 = [("Temp Máx Média", "Temp. máxima média", FMT_T, "thermo", GOLD),
          ("Temp Mín Média", "Temp. mínima média", FMT_T, "thermo", BLUE),
          ("Chuva Observada (mm)", "Chuva observada", FMT_MM, "drop", PETROL),
          ("Chuva Prevista (mm)", "Chuva prevista (16 dias)", FMT_MM, "cloud", TEAL),
          ("% Dias Favoráveis", "Dias favoráveis a campo", FMT_PCT, "leaf", G)]
    bg1 = "bg_visao_geral.png"
    render_png(background_svg("Visão Geral  ·  observado (90 dias) + previsão (16 dias)",
                              [(x, w, k[3], k[4]) for (x, w), k in zip(slots, k1)]), RES / bg1)
    d = write_page(p1, "Visão Geral", bg1)
    nav(d)
    slicer(d, FIL, "Filial", 24, 220, 1000)
    for i, ((x, w), k) in enumerate(zip(slots, k1)):
        kpi(d, k[0], k[1], k[2], x, w, 2000 + i * 100)
    visual(d, "azureMap", 24, 188, 600, 512, 3000,
           query={"Series": {"projections": [P(FIL, "Filial")]},
                  "X": {"projections": [P(col("dim_localidade", "longitude"), "Longitude")]},
                  "Y": {"projections": [P(col("dim_localidade", "latitude"), "Latitude")]},
                  "Size": {"projections": [P(meas("Chuva Total (mm)"), "Chuva (mm)", FMT_MM)]},
                  "Tooltips": {"projections": [P(meas("Temp Máx Média"), "Temp. máx", FMT_T),
                                               P(meas("Temp Mín Média"), "Temp. mín", FMT_T),
                                               P(meas("% Dias Favoráveis"), "% dias favoráveis", FMT_PCT)]}},
           objects={"mapControls": [{"properties": {
                        "defaultStyle": S("grayscale_light"), "showStylePicker": L("false"),
                        "showNavigationControls": L("false"), "showSelectionControl": L("false"),
                        "autoZoom": L("false"), "zoom": L("8.6D"),
                        "centerLongitude": L("-40.25D"), "centerLatitude": L("-19.12D")}}],
                    "bubbleLayer": [{"properties": {"show": L("true"), "minBubbleRadius": L("10L"),
                                                    "maxRadius": L("30L"), "bubbleStrokeWidth": L("2L"),
                                                    "autoStrokeColor": L("false"), "bubbleStrokeColor": C("#FFFFFF")}}],
                    "legend": [{"properties": {"show": L("true"), "showTitle": L("false"), "position": S("TopLeft")}}]},
           vco=container("Filiais no mapa  ·  tamanho = chuva acumulada"))
    combo_chuva_temp(d, 636, 188, 620, 250, 4000,
                     "Chuva diária × temperatura máxima  ·  barras verdes = dia favorável a campo")
    visual(d, "clusteredBarChart", 636, 450, 620, 250, 5000,
           query={"Category": {"projections": [P(FIL, "Filial", active=True)]},
                  "Y": {"projections": [P(meas("% Dias Favoráveis"), "% dias favoráveis", FMT_PCT)]}},
           objects={**AXES,
                    "valueAxis": [{"properties": {"show": L("false"), "gridlineShow": L("false")}}],
                    "dataPoint": [{"properties": {"fill": C(G)}}],
                    "labels": [{"properties": {"show": L("true"), "color": C(INK), "fontSize": L("10D"),
                                               "labelPosition": S("InsideEnd")}}]},
           vco=container("% de dias favoráveis a trabalho de campo por filial"),
           sort=[{"field": meas("% Dias Favoráveis"), "direction": "Descending"}])

    # ============ 2. PLANEJAMENTO DE CAMPO ============
    p2 = hid()
    slots = kpi_slots(256, 1000, 4)
    k2 = [("Chuva Próx 7 Dias (mm)", "Chuva prevista – próx. 7 dias", FMT_MM, "cloud", TEAL),
          ("Dias Favoráveis", "Dias favoráveis (filial × dia)", FMT_D, "calendar", G),
          ("% Dias Favoráveis", "% de dias favoráveis", FMT_PCT, "leaf", G_LIGHT),
          ("Temp Máx Média", "Temp. máxima média prevista", FMT_T, "thermo", GOLD)]
    bg2 = "bg_planejamento.png"
    render_png(background_svg("Planejamento de Campo  ·  previsão dos próximos 16 dias por filial",
                              [(x, w, k[3], k[4]) for (x, w), k in zip(slots, k2)]), RES / bg2)
    d = write_page(p2, "Planejamento de Campo", bg2, tipo_filter("previsao"))
    nav(d)
    slicer(d, FIL, "Filial", 24, 220, 1000)
    for i, ((x, w), k) in enumerate(zip(slots, k2)):
        kpi(d, k[0], k[1], k[2], x, w, 2000 + i * 100)
    visual(d, "pivotTable", 24, 188, 1232, 236, 3000,
           query={"Rows": {"projections": [P(FIL, "Filial", active=True)]},
                  "Columns": {"projections": [P(col("dim_calendario", "data"), "Dia", "ddd dd/MM", active=True)]},
                  "Values": {"projections": [P(meas("Chuva Total (mm)"), "Chuva (mm)", "0.0")]}},
           objects={"values": [
                        {"properties": {"fontColor": C(INK), "fontSize": L("10D")}},
                        {"properties": {"backColor": COR_DIA},
                         "selector": {"data": [WILD], "metadata": "Medidas.Chuva Total (mm)"}}],
                    "columnHeaders": [{"properties": {"fontColor": C("#FFFFFF"), "backColor": C(G_DARK),
                                                      "fontSize": L("9D"), "alignment": S("Center"),
                                                      "wordWrap": L("true")}}],
                    "rowHeaders": [{"properties": {"fontColor": C(INK), "fontSize": L("10D"), "bold": L("true")}}],
                    "subTotals": [{"properties": {"rowSubtotals": L("false"), "columnSubtotals": L("false")}}],
                    "grid": [{"properties": {"gridVertical": L("true"), "gridVerticalColor": C("#FFFFFF"),
                                             "gridVerticalWeight": L("2D"), "gridHorizontal": L("true"),
                                             "gridHorizontalColor": C("#FFFFFF"), "gridHorizontalWeight": L("2D"),
                                             "rowPadding": L("8D")}}]},
           vco=container("Calendário de campo  ·  chuva prevista (mm) por filial e dia"),
           sort=[{"field": col("dim_calendario", "data"), "direction": "Ascending"}])
    visual(d, "textbox", 900, 192, 348, 30, 3500, objects={"general": [{"properties": {"paragraphs": [{
        "textRuns": [
            {"value": "■ ", "textStyle": {"fontSize": "14px", "color": G_PALE}},
            {"value": "Favorável     ", "textStyle": {"fontSize": "10px", "color": MUTED}},
            {"value": "■ ", "textStyle": {"fontSize": "14px", "color": "#F4C7C3"}},
            {"value": "Desfavorável", "textStyle": {"fontSize": "10px", "color": MUTED}}],
        "horizontalTextAlignment": "right"}]}}]},
        vco={"background": [{"properties": {"show": L("false")}}], "border": [{"properties": {"show": L("false")}}],
             "visualHeader": [{"properties": {"show": L("false")}}]})
    visual(d, "clusteredColumnChart", 24, 436, 610, 264, 4000,
           query={"Category": {"projections": [P(col("dim_calendario", "data"), "Dia", "dd/MM", active=True)]},
                  "Y": {"projections": [P(meas("Chuva Total (mm)"), "Chuva (mm)", FMT_MM)]}},
           objects={**AXES,
                    "dataPoint": [{"properties": {"fill": COR_DIA}, "selector": {"data": [WILD]}}],
                    "labels": [{"properties": {"show": L("true"), "color": C(INK), "fontSize": L("8D")}}]},
           vco=container("Chuva prevista por dia (todas as filiais selecionadas)"),
           sort=[{"field": col("dim_calendario", "data"), "direction": "Ascending"}])
    visual(d, "lineChart", 646, 436, 610, 264, 5000,
           query={"Category": {"projections": [P(col("dim_calendario", "data"), "Dia", "dd/MM", active=True)]},
                  "Y": {"projections": [P(meas("Temp Máx Média"), "Máxima", FMT_T),
                                        P(meas("Temp Mín Média"), "Mínima", FMT_T)]}},
           objects={**AXES,
                    "dataPoint": [{"properties": {"fill": C(GOLD)}, "selector": {"metadata": "Medidas.Temp Máx Média"}},
                                  {"properties": {"fill": C(BLUE)}, "selector": {"metadata": "Medidas.Temp Mín Média"}}],
                    "lineStyles": [{"properties": {"strokeWidth": L("3L"), "showMarker": L("true"),
                                                    "markerSize": L("4D")}}],
                    "legend": [{"properties": {"show": L("true"), "position": S("TopRight"), "fontSize": L("9D")}}]},
           vco=container("Faixa de temperatura prevista (°C)"),
           sort=[{"field": col("dim_calendario", "data"), "direction": "Ascending"}])

    # ============ 3. HISTÓRICO ============
    p3 = hid()
    slots = kpi_slots(508, 748, 4)
    k3 = [("Chuva Observada (mm)", "Chuva observada no período", FMT_MM, "drop", PETROL),
          ("Chuva Acum 30d (mm)", "Chuva acumulada 30 dias", FMT_MM, "sum", TEAL),
          ("Temp Máx Média", "Temp. máxima média", FMT_T, "thermo", GOLD),
          ("% Dias Favoráveis", "Dias favoráveis a campo", FMT_PCT, "leaf", G)]
    bg3 = "bg_historico.png"
    render_png(background_svg("Histórico  ·  clima observado nas filiais (últimos 90 dias)",
                              [(x, w, k[3], k[4]) for (x, w), k in zip(slots, k3)]), RES / bg3)
    d = write_page(p3, "Histórico", bg3, tipo_filter("observado"))
    nav(d)
    slicer(d, FIL, "Filial", 24, 200, 1000)
    slicer(d, col("dim_calendario", "data"), "Período", 236, 260, 1100, mode="Between")
    for i, ((x, w), k) in enumerate(zip(slots, k3)):
        kpi(d, k[0], k[1], k[2], x, w, 2000 + i * 100)
    combo_chuva_temp(d, 24, 188, 1232, 236, 3000,
                     "Chuva diária observada × temperatura máxima  ·  barras verdes = dia favorável a campo")
    visual(d, "clusteredColumnChart", 24, 436, 610, 264, 4000,
           query={"Category": {"projections": [P(col("dim_calendario", "ano_mes"), "Mês", active=True)]},
                  "Series": {"projections": [P(FIL, "Filial")]},
                  "Y": {"projections": [P(meas("Chuva Total (mm)"), "Chuva (mm)", FMT_MM)]}},
           objects={**AXES,
                    "legend": [{"properties": {"show": L("true"), "position": S("TopRight"), "fontSize": L("9D")}}]},
           vco=container("Chuva mensal por filial (mm)"),
           sort=[{"field": col("dim_calendario", "ano_mes"), "direction": "Ascending"}])
    visual(d, "tableEx", 646, 436, 610, 264, 5000,
           query={"Values": {"projections": [
               P(FIL, "Filial"),
               P(meas("Chuva Total (mm)"), "Chuva", FMT_MM),
               P(meas("Temp Máx Média"), "Máx", FMT_T),
               P(meas("Temp Mín Média"), "Mín", FMT_T),
               P(meas("Dias Favoráveis"), "Dias fav.", "0"),
               P(meas("% Dias Favoráveis"), "% fav.", FMT_PCT)]}},
           objects={"columnHeaders": [{"properties": {"fontColor": C("#FFFFFF"), "backColor": C(G_DARK),
                                                      "fontSize": L("10D")}}],
                    "values": [{"properties": {"fontColor": C(INK), "fontSize": L("10D"),
                                               "backColorSecondary": C("#F6F8F4"), "backColorPrimary": C("#FFFFFF")}},
                               {"properties": {"dataBars": {"positiveColor": C(G_PALE), "negativeColor": C("#F4C7C3"),
                                                            "axisColor": C(LINE), "reverseDirection": L("false"),
                                                            "hideText": L("false")}},
                                "selector": {"metadata": "Medidas.% Dias Favoráveis"}}],
                    "total": [{"properties": {"totals": L("true"), "fontColor": C(G_DARK), "backColor": C(G_MIST)}}],
                    "grid": [{"properties": {"rowPadding": L("6D")}}]},
           vco=container("Resumo por filial"),
           sort=[{"field": meas("Chuva Total (mm)"), "direction": "Descending"}])

    dump(DEF / "pages" / "pages.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [p1, p2, p3], "activePageName": p1})

    # registra as imagens no report.json
    rpath = DEF / "report.json"
    rep = json.loads(rpath.read_text(encoding="utf-8"))
    for pkg in rep["resourcePackages"]:
        if pkg["name"] == "RegisteredResources":
            pkg["items"] = [i for i in pkg["items"] if i.get("type") != "Image"]
            for b in (bg1, bg2, bg3):
                pkg["items"].append({"name": b, "path": b, "type": "Image"})
    dump(rpath, rep)
    print("ok", p1, p2, p3)


if __name__ == "__main__":
    main()
