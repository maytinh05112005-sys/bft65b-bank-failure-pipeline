"""
Convert Markdown report to a beautifully formatted Word document (DOCX)
with embedded tables and high-resolution figures.
"""

import os
import re
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

def set_cell_background(cell, fill_hex):
    tcPr = cell._element.get_or_add_tcPr()
    shd = OxmlElement('w:shd')
    shd.set(qn('w:val'), 'clear')
    shd.set(qn('w:color'), 'auto')
    shd.set(qn('w:fill'), fill_hex)
    tcPr.append(shd)

def create_docx_report():
    doc = Document()

    # Set page margins (1 inch)
    sections = doc.sections
    for section in sections:
        section.top_margin = Inches(1)
        section.bottom_margin = Inches(1)
        section.left_margin = Inches(1)
        section.right_margin = Inches(1)

    # Base styling
    normal_style = doc.styles['Normal']
    normal_style.font.name = 'Calibri'
    normal_style.font.size = Pt(11)
    normal_style.font.color.rgb = RGBColor(0x22, 0x22, 0x22)

    # 1. Header & Title Page Info
    p_inst = doc.add_paragraph()
    r_inst = p_inst.add_run("NATIONAL ECONOMICS UNIVERSITY (NEU)\nFACULTY OF BANKING AND FINANCE\n")
    r_inst.font.size = Pt(12)
    r_inst.font.bold = True
    r_inst.font.color.rgb = RGBColor(0x1B, 0x36, 0x5D) # Navy Blue
    p_inst.alignment = WD_ALIGN_PARAGRAPH.CENTER

    p_course = doc.add_paragraph()
    r_course = p_course.add_run("BFT65B – BIG DATA ANALYTICS IN FINANCE\nFINAL EXAMINATION – INDIVIDUAL RESEARCH REPORT")
    r_course.font.size = Pt(13)
    r_course.font.bold = True
    p_course.alignment = WD_ALIGN_PARAGRAPH.CENTER

    p_title = doc.add_paragraph()
    p_title.paragraph_format.space_before = Pt(18)
    p_title.paragraph_format.space_after = Pt(18)
    r_title = p_title.add_run("A Big-Data Pipeline for Bank Failure Early Warning:\nScalable Architecture, Point-in-Time Data Engineering, and Honest Supervisory Validation")
    r_title.font.size = Pt(18)
    r_title.font.bold = True
    r_title.font.color.rgb = RGBColor(0x0F, 0x20, 0x4B)
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Submission Metadata Table
    table = doc.add_table(rows=8, cols=2)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False

    metadata = [
        ("Candidate Name", "[Họ và Tên Sinh Viên]"),
        ("Student ID", "[Mã Số Sinh Viên]"),
        ("GitHub Repository", "https://github.com/[Your-Username]/bft65b-bank-failure-pipeline"),
        ("Data Download Method & Date", "FDIC Public BankFind API (REST) | November 12, 2026"),
        ("Total Records Downloaded", "1,682,616 Call Report rows (170 Quarters: 1984Q1 - 2026Q2)"),
        ("Institutions & Failure Universe", "27,835 Institutions | 4,117 Failures & Assistance Cases"),
        ("Development Period (Train)", "1984Q1 – 2006Q4 (1,194,887 bank-quarters | 8,697 failures / 0.728%)"),
        ("Out-of-Time Test Period", "2008Q1 – 2012Q4 (157,851 bank-quarters | 1,882 failures / 1.192%)"),
    ]

    for i, (k, v) in enumerate(metadata):
        row = table.rows[i]
        c0, c1 = row.cells[0], row.cells[1]
        c0.width = Inches(2.5)
        c1.width = Inches(4.5)
        
        p0 = c0.paragraphs[0]
        r0 = p0.add_run(k)
        r0.font.bold = True
        r0.font.size = Pt(10)
        set_cell_background(c0, "F0F4F8")
        
        p1 = c1.paragraphs[0]
        r1 = p1.add_run(v)
        r1.font.size = Pt(10)
        if "Họ và Tên" in v or "Mã Số" in v or "Your-Username" in v:
            r1.font.color.rgb = RGBColor(0xC0, 0x39, 0x2B) # Highlight placeholder in red
            r1.font.bold = True

    doc.add_page_break()

    # Read Markdown content and build document sections
    with open("report/FINAL_REPORT_BFT65B.md", "r", encoding="utf-8") as f:
        md_text = f.read()

    # Split into lines and parse
    lines = md_text.split("\n")
    in_table = False
    table_lines = []

    def flush_table(tbl_lines):
        if not tbl_lines:
            return
        parsed = []
        for line in tbl_lines:
            if re.match(r"^\|\s*[-:]+\s*\|", line):
                continue # separator
            parts = [c.strip() for c in line.split("|")[1:-1]]
            if parts:
                parsed.append(parts)
        if len(parsed) < 2:
            return
        
        n_rows = len(parsed)
        n_cols = max(len(r) for r in parsed)
        tbl = doc.add_table(rows=n_rows, cols=n_cols)
        tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
        for r_idx, row_data in enumerate(parsed):
            for c_idx in range(n_cols):
                val = row_data[c_idx] if c_idx < len(row_data) else ""
                cell = tbl.cell(r_idx, c_idx)
                p = cell.paragraphs[0]
                p.paragraph_format.space_before = Pt(2)
                p.paragraph_format.space_after = Pt(2)
                r = p.add_run(val)
                r.font.size = Pt(9.5)
                if r_idx == 0:
                    r.font.bold = True
                    r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
                    set_cell_background(cell, "1B365D") # Dark navy header
                else:
                    if r_idx % 2 == 1:
                        set_cell_background(cell, "F9FAFB")
        doc.add_paragraph().paragraph_format.space_after = Pt(6)

    fig_inserted = set()

    for line in lines:
        line_str = line.strip()

        # Handle tables
        if line_str.startswith("|") and line_str.endswith("|"):
            in_table = True
            table_lines.append(line_str)
            continue
        elif in_table:
            in_table = False
            flush_table(table_lines)
            table_lines = []

        if not line_str:
            continue

        # Skip original header lines already handled on title page
        if line_str.startswith("# NATIONAL ECONOMICS UNIVERSITY") or "BFT65B – BIG DATA" in line_str or line_str.startswith("**Author:**"):
            continue

        # Heading 1
        if line_str.startswith("# ") and not line_str.startswith("# A Big-Data"):
            h = doc.add_heading(line_str[2:], level=1)
            h.style.font.color.rgb = RGBColor(0x1B, 0x36, 0x5D)
            continue

        # Heading 2
        if line_str.startswith("## "):
            h = doc.add_heading(line_str[3:], level=2)
            h.style.font.color.rgb = RGBColor(0x2C, 0x3E, 0x50)
            
            # Embed relevant figure right after section header if applicable
            sec_title = line_str[3:].lower()
            if "part 1" in sec_title and "part1_storage_comparison.png" not in fig_inserted:
                fig_path = "outputs/figures/part1_storage_comparison.png"
                if os.path.exists(fig_path):
                    doc.add_paragraph().paragraph_format.space_after = Pt(4)
                    doc.add_picture(fig_path, width=Inches(6.2))
                    cp = doc.add_paragraph("Figure 1: Storage Layout & Format Query Latency Benchmark (CSV vs. Parquet Snappy/ZSTD)")
                    cp.runs[0].font.italic = True
                    cp.runs[0].font.size = Pt(9.5)
                    cp.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    fig_inserted.add("part1_storage_comparison.png")

            elif "part 2" in sec_title and "part2_bank_population_and_failures.png" not in fig_inserted:
                fig_path = "outputs/figures/part2_bank_population_and_failures.png"
                if os.path.exists(fig_path):
                    doc.add_paragraph().paragraph_format.space_after = Pt(4)
                    doc.add_picture(fig_path, width=Inches(6.2))
                    cp = doc.add_paragraph("Figure 2: 40-Year Evolution of U.S. Banking Sector: Bank Consolidation vs. Failure Waves")
                    cp.runs[0].font.italic = True
                    cp.runs[0].font.size = Pt(9.5)
                    cp.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    fig_inserted.add("part2_bank_population_and_failures.png")

                fig_path2 = "outputs/figures/part2_missingness_heatmap.png"
                if os.path.exists(fig_path2) and "part2_missingness_heatmap.png" not in fig_inserted:
                    doc.add_picture(fig_path2, width=Inches(5.5))
                    cp2 = doc.add_paragraph("Figure 3: Reporting Missingness Heatmap (%) Across Regulatory Eras (Basel Standards Shift)")
                    cp2.runs[0].font.italic = True
                    cp2.runs[0].font.size = Pt(9.5)
                    cp2.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    fig_inserted.add("part2_missingness_heatmap.png")

            elif "part 3" in sec_title and "part3_engine_speedup_comparison.png" not in fig_inserted:
                fig_path = "outputs/figures/part3_engine_speedup_comparison.png"
                if os.path.exists(fig_path):
                    doc.add_paragraph().paragraph_format.space_after = Pt(4)
                    doc.add_picture(fig_path, width=Inches(6.2))
                    cp = doc.add_paragraph("Figure 4: Engine Architecture & Execution Performance on 1.68M Bank Records (Polars vs. Pandas vs. PySpark)")
                    cp.runs[0].font.italic = True
                    cp.runs[0].font.size = Pt(9.5)
                    cp.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    fig_inserted.add("part3_engine_speedup_comparison.png")

            elif "part 5" in sec_title and "part5_roc_and_pr_curves.png" not in fig_inserted:
                fig_path = "outputs/figures/part5_roc_and_pr_curves.png"
                if os.path.exists(fig_path):
                    doc.add_paragraph().paragraph_format.space_after = Pt(4)
                    doc.add_picture(fig_path, width=Inches(6.2))
                    cp = doc.add_paragraph("Figure 5: Out-of-Time Receiver Operating Characteristic (ROC) & Precision-Recall (PR) Curves")
                    cp.runs[0].font.italic = True
                    cp.runs[0].font.size = Pt(9.5)
                    cp.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    fig_inserted.add("part5_roc_and_pr_curves.png")

                fig_path2 = "outputs/figures/part5_cost_threshold_curve.png"
                if os.path.exists(fig_path2) and "part5_cost_threshold_curve.png" not in fig_inserted:
                    doc.add_picture(fig_path2, width=Inches(5.2))
                    cp2 = doc.add_paragraph("Figure 6: Supervisory Cost Function Optimization: Deriving Optimal Alarm Threshold p* = 0.040")
                    cp2.runs[0].font.italic = True
                    cp2.runs[0].font.size = Pt(9.5)
                    cp2.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    fig_inserted.add("part5_cost_threshold_curve.png")

                fig_path3 = "outputs/figures/part5_feature_importance.png"
                if os.path.exists(fig_path3) and "part5_feature_importance.png" not in fig_inserted:
                    doc.add_picture(fig_path3, width=Inches(5.5))
                    cp3 = doc.add_paragraph("Figure 7: Top 15 Banking Failure Risk Predictors (LightGBM Split Feature Importance)")
                    cp3.runs[0].font.italic = True
                    cp3.runs[0].font.size = Pt(9.5)
                    cp3.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    fig_inserted.add("part5_feature_importance.png")
            continue

        # Heading 3
        if line_str.startswith("### "):
            h = doc.add_heading(line_str[4:], level=3)
            h.style.font.color.rgb = RGBColor(0x34, 0x49, 0x5E)
            continue

        # Bullet items
        if line_str.startswith("* ") or line_str.startswith("- "):
            p = doc.add_paragraph(style='List Bullet')
            clean_text = line_str[2:]
            parts = re.split(r'(\*\*.*?\*\*)', clean_text)
            for part in parts:
                if part.startswith('**') and part.endswith('**'):
                    r = p.add_run(part[2:-2])
                    r.font.bold = True
                else:
                    p.add_run(part)
            p.paragraph_format.space_after = Pt(3)
            continue

        # Numbered items
        if re.match(r"^\d+\.\s", line_str):
            p = doc.add_paragraph(style='List Number')
            clean_text = re.sub(r"^\d+\.\s", "", line_str)
            parts = re.split(r'(\*\*.*?\*\*)', clean_text)
            for part in parts:
                if part.startswith('**') and part.endswith('**'):
                    r = p.add_run(part[2:-2])
                    r.font.bold = True
                else:
                    p.add_run(part)
            p.paragraph_format.space_after = Pt(3)
            continue

        # Normal text paragraphs
        p = doc.add_paragraph()
        parts = re.split(r'(\*\*.*?\*\*)', line_str)
        for part in parts:
            if part.startswith('**') and part.endswith('**'):
                r = p.add_run(part[2:-2])
                r.font.bold = True
            else:
                p.add_run(part)
        p.paragraph_format.space_after = Pt(6)

    # Save Word document
    out_docx = "report/FINAL_REPORT_BFT65B.docx"
    doc.save(out_docx)
    print(f"Successfully generated formatted Word report: {out_docx}")

if __name__ == "__main__":
    create_docx_report()
