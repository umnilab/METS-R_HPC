"""Small, dependency-free dashboard renderer shared by the security demos.

The generated HTML is self-contained, works offline, and keeps the raw scenario
payload embedded in the page so a result can be inspected without a Python
process.  This is deliberately not a web framework: research runs should finish
and persist their evidence even when no browser is attached.
"""

from __future__ import annotations

import functools
import json
import os
import threading
import webbrowser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from html import escape
from pathlib import Path
from typing import Any, Mapping, Sequence


_PALETTE = ("#42d3a5", "#ffbd59", "#ff6b6b", "#75a7ff", "#c38cff", "#52c7ea")


def _json_for_script(value: Any) -> str:
    """Serialize data without allowing a value to terminate the script tag."""

    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )
    return (
        encoded.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )


def _normalise_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    data = dict(payload)
    scenarios = []
    for index, original in enumerate(data.get("scenarios", ())):
        scenario = dict(original)
        scenario.setdefault("id", f"scenario-{index + 1}")
        scenario.setdefault("label", str(scenario["id"]).replace("_", " ").title())
        scenario.setdefault("color", _PALETTE[index % len(_PALETTE)])
        scenario.setdefault("metrics", {})
        scenario.setdefault("series", {})
        scenario.setdefault("events", [])
        scenarios.append(scenario)
    if not scenarios:
        raise ValueError("dashboard payload requires at least one scenario")
    data["scenarios"] = scenarios
    data.setdefault("eyebrow", "METS-R HPC · security replication")
    data.setdefault("subtitle", "Reproducible experiment results")
    data.setdefault("metric_cards", [])
    data.setdefault("charts", [])
    data.setdefault("notes", [])
    data.setdefault("parameters", {})
    data.setdefault("paper", {})
    return data


def write_dashboard(
    payload: Mapping[str, Any], output_dir: str | os.PathLike[str], filename: str = "dashboard.html"
) -> Path:
    """Write a standalone interactive dashboard and return its absolute path.

    Expected payload fields are documented in ``tutorials/security_demos/README.md``.
    Unknown fields are retained in the embedded JSON for provenance.
    """

    data = _normalise_payload(payload)
    destination = Path(output_dir).expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / filename
    target.write_text(_page_html(data), encoding="utf-8")
    return target


def serve_dashboard(
    dashboard_path: str | os.PathLike[str],
    host: str = "127.0.0.1",
    port: int = 0,
    open_browser: bool = False,
) -> None:
    """Serve a generated dashboard until Ctrl-C.

    ``port=0`` asks the operating system for a free port.  The server is scoped
    to the dashboard's output directory and does not expose the repository root.
    """

    path = Path(dashboard_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(path.parent))
    server = ThreadingHTTPServer((host, int(port)), handler)
    actual_host, actual_port = server.server_address[:2]
    display_host = "127.0.0.1" if actual_host in ("0.0.0.0", "::") else actual_host
    url = f"http://{display_host}:{actual_port}/{path.name}"
    print(f"Dashboard: {url}")
    if open_browser:
        threading.Timer(0.2, webbrowser.open, args=(url,)).start()
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        print("\nDashboard server stopped.")
    finally:
        server.server_close()


def _page_html(data: Mapping[str, Any]) -> str:
    title = str(data.get("title", "Security replication dashboard"))
    encoded = _json_for_script(data)
    # The only interpolated values are JSON encoded above and a title that is
    # re-encoded through JSON before insertion into a text-only title element.
    safe_title = escape(title, quote=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{ color-scheme: dark; --bg:#07111f; --panel:#101d2d; --panel2:#142438;
      --ink:#ecf5ff; --muted:#94a9bf; --line:#29405a; --accent:#42d3a5; }}
    * {{ box-sizing:border-box }} body {{ margin:0; background:radial-gradient(circle at 85% 0,#17314a 0,transparent 34%),var(--bg);
      color:var(--ink); font:15px/1.5 Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif; }}
    a {{ color:#7dc7ff }} .shell {{ width:min(1480px,calc(100% - 32px)); margin:auto; padding:38px 0 70px }}
    header {{ display:grid; grid-template-columns:minmax(0,1fr) auto; gap:24px; align-items:end; margin-bottom:24px }}
    .eyebrow {{ color:var(--accent); text-transform:uppercase; letter-spacing:.16em; font-size:12px; font-weight:800 }}
    h1 {{ margin:.2rem 0 .35rem; font-size:clamp(28px,4vw,52px); line-height:1.06; letter-spacing:-.035em }}
    .subtitle {{ margin:0; color:var(--muted); max-width:850px; font-size:17px }}
    .paper {{ max-width:360px; text-align:right; color:var(--muted) }} .paper strong {{ display:block; color:var(--ink) }}
    .toolbar,.panel {{ background:linear-gradient(145deg,rgba(20,36,56,.96),rgba(12,25,40,.96)); border:1px solid var(--line); border-radius:16px }}
    .toolbar {{ display:flex; flex-wrap:wrap; gap:9px; padding:12px; margin-bottom:18px; position:sticky; top:8px; z-index:5; backdrop-filter:blur(12px) }}
    button {{ appearance:none; border:1px solid var(--line); background:#0b1828; color:var(--muted); border-radius:999px; padding:8px 13px; cursor:pointer; font-weight:750 }}
    button.active {{ color:#06121d; border-color:transparent; background:var(--scenario-color,var(--accent)) }}
    .grid {{ display:grid; grid-template-columns:repeat(12,1fr); gap:16px }}
    .kpis {{ grid-column:span 12; display:grid; grid-template-columns:repeat(auto-fit,minmax(165px,1fr)); gap:12px }}
    .kpi {{ padding:16px; min-height:113px }} .kpi .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em }}
    .kpi .value {{ font-size:29px; font-weight:850; letter-spacing:-.025em; margin-top:5px }} .kpi .delta {{ font-size:12px; color:var(--muted) }}
    .chart {{ grid-column:span 6; padding:18px; min-height:340px }} .wide {{ grid-column:span 12 }}
    h2 {{ font-size:17px; margin:0 0 4px }} .chart .caption {{ color:var(--muted); font-size:12px; min-height:19px }}
    svg {{ width:100%; height:255px; overflow:visible }} .axis {{ stroke:#60778e; stroke-width:1 }} .gridline {{ stroke:#263b50; stroke-width:1 }}
    .axis-label {{ fill:#8da3b8; font-size:10px }} .series-line {{ fill:none; stroke-width:2.4; vector-effect:non-scaling-stroke }}
    .legend {{ display:flex; flex-wrap:wrap; gap:12px; margin-top:4px; color:var(--muted); font-size:12px }}
    .swatch {{ width:9px; height:9px; border-radius:50%; display:inline-block; margin-right:5px }}
    .comparison {{ grid-column:span 8; padding:18px; overflow:auto }} .details {{ grid-column:span 4; padding:18px }}
    table {{ border-collapse:collapse; width:100%; font-size:13px }} th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; white-space:nowrap }}
    th:first-child,td:first-child {{ text-align:left; position:sticky; left:0; background:var(--panel) }} th {{ color:var(--muted); font-weight:700 }}
    .notes {{ margin:8px 0 0; padding-left:18px; color:var(--muted) }} .notes li {{ margin:7px 0 }}
    dl {{ display:grid; grid-template-columns:minmax(90px,.7fr) 1.3fr; margin:0 }} dt,dd {{ padding:7px 0; border-bottom:1px solid var(--line) }}
    dt {{ color:var(--muted) }} dd {{ margin:0; text-align:right; overflow-wrap:anywhere }}
    .event-list {{ grid-column:span 12; padding:18px; overflow:auto }} .event-pill {{ display:inline-block; padding:4px 8px; margin:3px; border:1px solid var(--line); border-radius:7px; color:var(--muted); font-size:12px }}
    footer {{ color:var(--muted); font-size:12px; margin-top:22px; text-align:center }}
    @media (max-width:900px) {{ header {{ grid-template-columns:1fr }} .paper {{ text-align:left }} .chart,.comparison,.details {{ grid-column:span 12 }} .toolbar {{ position:static }} }}
  </style>
</head>
<body><main class="shell">
  <header><div><div class="eyebrow" id="eyebrow"></div><h1 id="title"></h1><p class="subtitle" id="subtitle"></p></div><div class="paper" id="paper"></div></header>
  <nav class="toolbar" id="scenario-tabs" aria-label="Scenario selection"></nav>
  <section class="grid"><div class="kpis" id="kpis"></div><div id="charts" style="display:contents"></div>
    <section class="panel comparison"><h2>Scenario comparison</h2><div id="comparison"></div></section>
    <aside class="panel details"><h2>Experiment parameters</h2><dl id="parameters"></dl></aside>
    <section class="panel event-list" id="events-panel"><h2>Selected-scenario events</h2><div id="events"></div></section>
  </section>
  <section class="panel" style="padding:18px;margin-top:16px"><h2>Interpretation and fidelity boundary</h2><ul class="notes" id="notes"></ul></section>
  <footer>Standalone artifact · generated by tutorials/security_demos · no external JavaScript or network requests</footer>
</main>
<script id="payload" type="application/json">{encoded}</script>
<script>
(() => {{
  const data=JSON.parse(document.getElementById('payload').textContent), $=id=>document.getElementById(id);
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
  const format=(v,card={{}})=>{{ if(v===null||v===undefined||Number.isNaN(Number(v))) return '—';
    if(typeof v!=='number') return esc(v); const p=card.precision??(Math.abs(v)<10?2:1); return v.toLocaleString(undefined,{{maximumFractionDigits:p,minimumFractionDigits:card.fixed?p:0}})+(card.unit?` ${{esc(card.unit)}}`:''); }};
  $('eyebrow').textContent=data.eyebrow; $('title').textContent=data.title; $('subtitle').textContent=data.subtitle;
  const paper=data.paper||{{}}; $('paper').innerHTML=paper.title?`<strong>${{esc(paper.title)}}</strong>${{esc(paper.venue||'')}}${{paper.url?` · <a href="${{esc(paper.url)}}" target="_blank" rel="noreferrer">paper</a>`:''}}`:'';
  let selected=0;
  function tabs() {{ $('scenario-tabs').innerHTML=data.scenarios.map((s,i)=>`<button style="--scenario-color:${{esc(s.color)}}" class="${{i===selected?'active':''}}" data-index="${{i}}">${{esc(s.label)}}</button>`).join('');
    $('scenario-tabs').querySelectorAll('button').forEach(b=>b.onclick=()=>{{selected=Number(b.dataset.index); render();}}); }}
  function kpis() {{ const s=data.scenarios[selected], baseline=data.scenarios[0]; $('kpis').innerHTML=(data.metric_cards||[]).map(card=>{{
    const value=s.metrics?.[card.key], base=baseline.metrics?.[card.key]; let delta='';
    if(selected&&typeof value==='number'&&typeof base==='number'&&base!==0) {{ const pct=(value-base)/Math.abs(base)*100; delta=`${{pct>=0?'+':''}}${{pct.toFixed(1)}}% vs ${{esc(baseline.label)}}`; }}
    return `<article class="panel kpi"><div class="label">${{esc(card.label)}}</div><div class="value" style="color:${{esc(s.color)}}">${{format(value,card)}}</div><div class="delta">${{esc(delta||card.hint||'')}}</div></article>`; }}).join(''); }}
  function lineChart(chart) {{ const active=data.scenarios.filter(s=>Array.isArray(s.series?.[chart.key])&&s.series[chart.key].length);
    if(!active.length) return `<p class="caption">No samples recorded.</p>`;
    const points=active.flatMap(s=>s.series[chart.key]), xs=points.map(p=>Number(Array.isArray(p)?p[0]:p.x)), ys=points.map(p=>Number(Array.isArray(p)?p[1]:p.y));
    let xmin=Math.min(...xs),xmax=Math.max(...xs),ymin=Math.min(...ys),ymax=Math.max(...ys); if(xmin===xmax)xmax=xmin+1;if(ymin===ymax){{ymin-=1;ymax+=1}}
    const W=720,H=250,L=58,R=14,T=12,B=38,sx=x=>L+(x-xmin)/(xmax-xmin)*(W-L-R),sy=y=>T+(ymax-y)/(ymax-ymin)*(H-T-B);
    const grid=[0,.25,.5,.75,1].map(f=>{{const y=T+f*(H-T-B),val=ymax-f*(ymax-ymin);return `<line class="gridline" x1="${{L}}" y1="${{y}}" x2="${{W-R}}" y2="${{y}}"/><text class="axis-label" x="${{L-7}}" y="${{y+3}}" text-anchor="end">${{Number(val.toPrecision(3))}}</text>`}}).join('');
    const paths=active.map(s=>{{const d=s.series[chart.key].map((p,i)=>`${{i?'L':'M'}}${{sx(Number(Array.isArray(p)?p[0]:p.x)).toFixed(1)}},${{sy(Number(Array.isArray(p)?p[1]:p.y)).toFixed(1)}}`).join(' ');return `<path class="series-line" stroke="${{esc(s.color)}}" d="${{d}}"><title>${{esc(s.label)}}</title></path>`}}).join('');
    return `<svg viewBox="0 0 ${{W}} ${{H}}" role="img" aria-label="${{esc(chart.title)}}"><line class="axis" x1="${{L}}" y1="${{H-B}}" x2="${{W-R}}" y2="${{H-B}}"/><line class="axis" x1="${{L}}" y1="${{T}}" x2="${{L}}" y2="${{H-B}}"/>${{grid}}${{paths}}<text class="axis-label" x="${{(L+W-R)/2}}" y="${{H-7}}" text-anchor="middle">${{esc(chart.x_label||'time')}}</text></svg><div class="legend">${{active.map(s=>`<span><i class="swatch" style="background:${{esc(s.color)}}"></i>${{esc(s.label)}}</span>`).join('')}}</div>`; }}
  function charts() {{ $('charts').innerHTML=(data.charts||[]).map((c,i)=>`<section class="panel chart ${{c.wide?'wide':''}}"><h2>${{esc(c.title)}}</h2><div class="caption">${{esc(c.caption||c.y_label||'')}}</div>${{lineChart(c)}}</section>`).join(''); }}
  function comparison() {{ const cards=data.metric_cards||[]; let h='<table><thead><tr><th>Metric</th>'+data.scenarios.map(s=>`<th style="color:${{esc(s.color)}}">${{esc(s.label)}}</th>`).join('')+'</tr></thead><tbody>';
    h+=cards.map(c=>`<tr><td>${{esc(c.label)}}</td>${{data.scenarios.map(s=>`<td>${{format(s.metrics?.[c.key],c)}}</td>`).join('')}}</tr>`).join(''); $('comparison').innerHTML=h+'</tbody></table>'; }}
  function parameters() {{ $('parameters').innerHTML=Object.entries(data.parameters||{{}}).map(([k,v])=>`<dt>${{esc(k)}}</dt><dd>${{esc(Array.isArray(v)?v.join(', '):v)}}</dd>`).join(''); }}
  function events() {{ const items=data.scenarios[selected].events||[]; $('events').innerHTML=items.length?items.map(e=>`<span class="event-pill">${{esc(typeof e==='string'?e:(e.label||JSON.stringify(e)))}}</span>`).join(''):'<span class="caption">No discrete events recorded for this scenario.</span>'; $('events-panel').style.display=items.length?'block':'none'; }}
  $('notes').innerHTML=(data.notes||[]).map(n=>`<li>${{esc(n)}}</li>`).join('');
  function render() {{ tabs(); kpis(); charts(); comparison(); parameters(); events(); }} render();
}})();
</script></body></html>"""


__all__ = ["serve_dashboard", "write_dashboard"]
