"""
utils/reporter.py
-----------------
ETL test result reporter.
Generates structured JSON and human-readable HTML reports
summarizing all validation check results.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict

logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).parent.parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


class ETLTestReporter:

    def __init__(self):
        self.results: List[Dict] = []
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    def add_result(self, result: dict, suite: str = "general"):
        """Add a validator result to the report."""
        result["suite"] = suite
        result["timestamp"] = datetime.now().isoformat()
        self.results.append(result)

    def get_summary(self) -> dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.get("passed"))
        failed = total - passed
        return {
            "run_id": self.run_id,
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{(passed / total * 100):.1f}%" if total > 0 else "N/A",
            "status": "PASS" if failed == 0 else "FAIL",
        }

    def save_json(self, filename: str = None) -> Path:
        """Save results as a JSON report."""
        fname = filename or f"etl_results_{self.run_id}.json"
        path = REPORTS_DIR / fname
        report = {
            "summary": self.get_summary(),
            "results": self.results,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"JSON report saved: {path}")
        return path

    def save_html(self, filename: str = None) -> Path:
        """Save results as a styled HTML report."""
        fname = filename or f"etl_report_{self.run_id}.html"
        path = REPORTS_DIR / fname
        summary = self.get_summary()

        rows = ""
        for r in self.results:
            icon = "✅" if r.get("passed") else "❌"
            status_class = "pass" if r.get("passed") else "fail"
            rows += f"""
            <tr class="{status_class}">
                <td>{icon}</td>
                <td>{r.get('suite', '')}</td>
                <td>{r.get('check', '')}</td>
                <td>{r.get('details', '')}</td>
                <td>{r.get('timestamp', '')}</td>
            </tr>"""

        overall_class = "pass" if summary["status"] == "PASS" else "fail"
        html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>ETL Test Report — {self.run_id}</title>
  <style>
    body {{ font-family: 'Segoe UI', sans-serif; background: #f5f5f5; margin: 20px; }}
    h1 {{ color: #333; }}
    .summary {{ background: white; padding: 20px; border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }}
    .badge {{ display: inline-block; padding: 6px 16px; border-radius: 20px;
              font-weight: bold; font-size: 1.2em; }}
    .pass {{ color: #155724; background: #d4edda; }}
    .fail {{ color: #721c24; background: #f8d7da; }}
    table {{ width: 100%; border-collapse: collapse; background: white;
             border-radius: 8px; overflow: hidden;
             box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
    th {{ background: #343a40; color: white; padding: 12px; text-align: left; }}
    td {{ padding: 10px 12px; border-bottom: 1px solid #dee2e6; }}
    tr.pass td {{ background: #f8fff9; }}
    tr.fail td {{ background: #fff8f8; }}
    .stat {{ font-size: 2em; font-weight: bold; }}
  </style>
</head>
<body>
  <h1>🔍 ETL Test Automation Report</h1>
  <div class="summary">
    <p><strong>Run ID:</strong> {summary['run_id']}</p>
    <p>
      Overall Status: <span class="badge {overall_class}">{summary['status']}</span>
    </p>
    <p>
      <span class="stat">{summary['passed']}</span> Passed &nbsp;|&nbsp;
      <span class="stat">{summary['failed']}</span> Failed &nbsp;|&nbsp;
      <span class="stat">{summary['total_checks']}</span> Total &nbsp;|&nbsp;
      Pass Rate: <strong>{summary['pass_rate']}</strong>
    </p>
  </div>
  <table>
    <thead>
      <tr>
        <th>Status</th><th>Suite</th><th>Check</th>
        <th>Details</th><th>Timestamp</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>"""
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"HTML report saved: {path}")
        return path

    def print_summary(self):
        s = self.get_summary()
        print(f"\n{'=' * 60}")
        print(f"  ETL TEST RUN SUMMARY — {s['run_id']}")
        print(f"{'=' * 60}")
        print(f"  Total Checks : {s['total_checks']}")
        print(f"  ✅ Passed    : {s['passed']}")
        print(f"  ❌ Failed    : {s['failed']}")
        print(f"  Pass Rate    : {s['pass_rate']}")
        print(f"  Status       : {s['status']}")
        print(f"{'=' * 60}\n")
