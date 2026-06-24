#!/usr/bin/env python3
import os
import subprocess
import xml.etree.ElementTree as ET
import re
from datetime import datetime

def run_command(cmd, env=None):
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, env=env)
        return res.stdout, res.stderr, res.returncode
    except Exception as e:
        return "", str(e), -1

def check_ruff():
    cmd = ["uv", "run", "ruff", "check", "src/"]
    stdout, stderr, code = run_command(cmd)
    issues = []
    for line in stdout.splitlines():
        if line.strip():
            issues.append(line.strip())
    return issues

def check_pytest():
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    cmd = ["uv", "run", "pytest", "--cov=src", "--cov-branch", "--cov-report=xml", "tests/", "--ignore=tests/test_sql_guard_llm.py"]
    stdout, stderr, code = run_command(cmd, env=env)
    
    # Check if pytest failed completely
    if code != 0 and not os.path.exists("coverage.xml"):
        return 0.0, 0.0, 0, 1, ["Pytest failed to run. Error: " + stderr]
        
    line_cov = 0.0
    branch_cov = 0.0
    file_coverages = []
    
    try:
        tree = ET.parse("coverage.xml")
        root = tree.getroot()
        line_cov = float(root.attrib.get("line-rate", 0)) * 100
        branch_cov = float(root.attrib.get("branch-rate", 0)) * 100
        
        # Parse individual file coverages
        for package in root.findall(".//package"):
            for cls in package.findall(".//class"):
                filename = cls.attrib.get("filename", "")
                if filename.startswith("src/"):
                    cls_line_cov = float(cls.attrib.get("line-rate", 0)) * 100
                    file_coverages.append((filename, cls_line_cov))
    except Exception as e:
        return 0.0, 0.0, 0, 0, [f"Error parsing coverage.xml: {e}"]
        
    # Count passed/failed tests
    passed = 0
    failed = 0
    match = re.search(r"(\d+) passed", stdout)
    if match:
        passed = int(match.group(1))
    match_failed = re.search(r"(\d+) failed", stdout)
    if match_failed:
        failed = int(match_failed.group(1))
        
    return line_cov, branch_cov, passed, failed, file_coverages

def check_interrogate():
    cmd = ["uv", "run", "interrogate", "-vv", "src/"]
    stdout, stderr, code = run_command(cmd)
    
    total_coverage = 0.0
    missing_items = []
    lines = stdout.splitlines()
    
    for line in lines:
        if "TOTAL" in line and "|" in line:
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) >= 5:
                cov_str = parts[4].replace("%", "")
                try:
                    total_coverage = float(cov_str)
                except ValueError:
                    pass
                    
    current_file = ""
    for line in lines:
        if "|" in line:
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) == 2:
                name, status = parts[0], parts[1]
                if "Status" in status or "---" in name:
                    continue
                if "(module)" in name:
                    current_file = name.replace(" (module)", "")
                if status == "MISSED":
                    if "(module)" in name:
                        missing_items.append(f"Module `{current_file}` missing docstring")
                    else:
                        missing_items.append(f"`{name}` in `{current_file}` missing docstring")
                        
    return total_coverage, missing_items

def main():
    print("Running Ruff check...")
    ruff_issues = check_ruff()
    
    print("Running Pytest and coverage check...")
    line_cov, branch_cov, passed, failed, file_coverages = check_pytest()
    
    print("Running Interrogate docstring check...")
    doc_cov, missing_docs = check_interrogate()
    
    # Generate report
    report_path = "QUALITY_REPORT.md"
    print(f"Generating report at {report_path}...")
    
    # Determine Statuses
    code_status = "🟢 Passed" if not ruff_issues else "🟡 Warnings"
    test_status = "🟢 Passed" if failed == 0 and line_cov >= 80 else "🔴 Action Required"
    doc_status = "🟢 Passed" if doc_cov >= 80 else "🟡 Needs Docs"
    
    report_content = f"""# 🛡️ SQL Data Guard Quality Report

**Generated on:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 📊 Quality Summary

| Pillar | Metric Checked | Current Score | Status |
| :--- | :--- | :---: | :---: |
| 🐍 **Code Quality** | Ruff Style Violations | **{len(ruff_issues)} Issues** | {code_status} |
| 🧪 **Test Quality** | Pytest Unit Coverage | **{line_cov:.1f}%** | {test_status} |
| 📝 **Doc Quality** | Public API Docstring Coverage | **{doc_cov:.1f}%** | {doc_status} |

---

## 🐍 Code Quality Details (Ruff)
"""
    if not ruff_issues:
        report_content += "- **Lint Errors:** 0 violations found. Code style is clean.\n"
    else:
        report_content += f"- **Violations Found:** {len(ruff_issues)} issue(s):\n"
        for issue in ruff_issues:
            report_content += f"  - `{issue}`\n"
            
    report_content += f"""
## 🧪 Test Quality Details (Pytest-Cov)
- **Total Tests Executed:** {passed + failed} (Passed: {passed}, Failed: {failed})
- **Branch Coverage:** {branch_cov:.1f}%
- **Statement Coverage:** {line_cov:.1f}%
"""
    if file_coverages:
        report_content += "\n### Per-File Coverage:\n"
        for filename, cov in sorted(file_coverages, key=lambda x: x[1]):
            report_content += f"- `{filename}`: **{cov:.1f}%**\n"
            
    report_content += f"""
## 📝 Documentation Quality Details (Interrogate)
- **Total Docstring Coverage:** {doc_cov:.1f}%
"""
    if missing_docs:
        report_content += f"\n### Missing Docstrings ({len(missing_docs)}):\n"
        for item in missing_docs[:15]:
            report_content += f"- {item}\n"
        if len(missing_docs) > 15:
            report_content += f"- *...and {len(missing_docs) - 15} more.*\n"
            
    with open(report_path, "w") as f:
        f.write(report_content)
        
    print("Report generated successfully!")

if __name__ == "__main__":
    main()
