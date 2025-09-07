# report.py
from jinja2 import Template
from pathlib import Path
from typing import Dict
import json
from utils import sanitize_for_json
from datetime import datetime

HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Nifty ETL Report</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    table { border-collapse: collapse; margin-bottom: 20px; width: 100%; }
    table, th, td { border: 1px solid #ddd; padding: 6px; text-align: left; font-size: 13px; }
    th { background: #f4f7fb; }
    .ok { color: green; font-weight: 600; }
    .fail { color: red; font-weight: 600; }
    .small { font-size: 12px; color: #444; }
    pre { background: #f2f2f2; padding: 8px; overflow: auto; }
  </style>
</head>
<body>
  <h1>Nifty ETL Report</h1>
  <p class="small">Generated: {{ generated_at }}</p>

  <h2>Summary</h2>
  <table>
    <thead><tr><th>Ticker</th><th>Rows</th><th>Start</th><th>End</th><th>DQ Pass</th><th>Notes</th></tr></thead>
    <tbody>
    {% for t in summary %}
      <tr>
        <td><b>{{ t.ticker }}</b></td>
        <td>{{ t.rows }}</td>
        <td>{{ t.start_date or '-' }}</td>
        <td>{{ t.end_date or '-' }}</td>
        <td class="{{ 'ok' if t.dq_pass else 'fail' }}">{{ 'PASS' if t.dq_pass else 'FAIL' }}</td>
        <td>{{ t.dq_reasons|join(', ') }}</td>
      </tr>
    {% endfor %}
    </tbody>
  </table>

  <h2>Details</h2>
  {% for t in details %}
    <h3>{{ t.ticker }} <span class="small">({{ t.start_date }} → {{ t.end_date }})</span></h3>
    <div style="display:flex; gap:20px;">
      <div style="flex:1;">
        <h4>DQ Report</h4>
        <pre>{{ t.dq_report_json }}</pre>
      </div>
      <div style="flex:1;">
        <h4>Sample rows</h4>
        {{ t.sample_html | safe }}
      </div>
    </div>
    <hr/>
  {% endfor %}
</body>
</html>
"""

def generate_html_report(dq_map: Dict[str, dict], data_map: Dict[str, 'pd.DataFrame'], output_path: Path):
    summary_list = []
    details_list = []
    for ticker, dq in dq_map.items():
        df = data_map.get(ticker)
        sample_html = df.head(10).to_html(classes="sample", index=True, border=0) if df is not None else "<i>no data</i>"
        summary_list.append({
            "ticker": ticker,
            "rows": dq.get('rows', 0),
            "start_date": dq.get('start_date'),
            "end_date": dq.get('end_date'),
            "dq_pass": dq.get('dq_pass', False),
            "dq_reasons": dq.get('dq_reasons', [])
        })
        # Ensure dq is JSON-serializable
        dq_sanitized = sanitize_for_json(dq)
        details_list.append({
            "ticker": ticker,
            "dq_report_json": json.dumps(dq_sanitized, indent=2),
            "sample_html": sample_html,
            "start_date": dq.get('start_date'),
            "end_date": dq.get('end_date'),
        })

    template = Template(HTML_TEMPLATE)
    html = template.render(generated_at=datetime.utcnow().isoformat() + "Z", summary=summary_list, details=details_list)
    output_path.write_text(html, encoding='utf8')
