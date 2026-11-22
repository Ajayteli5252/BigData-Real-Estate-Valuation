"""
Update and regenerate Advanced_Real_Estate_Valuation_Report.docx
Replaces legacy King County / DuckDB / XGBoost content with verified
Apache Hadoop MapReduce (HDFS/YARN), UK HM Land Registry, and Ridge Regression workflow.
Preserves professional Arial typography and clean table styling.
"""
import docx
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls, qn

def set_cell_background(cell, fill_hex):
    """Sets background color of a table cell."""
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = parse_xml(f'<w:shd {nsdecls("w")} w:fill="{fill_hex}"/>')
    tc_pr.append(shd)

def set_cell_margins(cell, top=100, bottom=100, left=150, right=150):
    """Sets cell padding."""
    tc_pr = cell._tc.get_or_add_tcPr()
    tc_mar = parse_xml(
        f'<w:tcMar {nsdecls("w")}>'
        f'<w:top w:w="{top}" w:type="dxa"/>'
        f'<w:bottom w:w="{bottom}" w:type="dxa"/>'
        f'<w:left w:w="{left}" w:type="dxa"/>'
        f'<w:right w:w="{right}" w:type="dxa"/>'
        f'</w:tcMar>'
    )
    tc_pr.append(tc_mar)

def add_heading_1(doc, text):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(14)
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.keep_with_next = True
    run = p.add_run(text)
    run.font.name = "Arial"
    run.font.size = Pt(14)
    run.bold = True
    run.font.color.rgb = RGBColor(15, 23, 42) # Slate-900
    return p

def add_heading_2(doc, text):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(10)
    p.paragraph_format.space_after = Pt(4)
    p.paragraph_format.keep_with_next = True
    run = p.add_run(text)
    run.font.name = "Arial"
    run.font.size = Pt(12)
    run.bold = True
    run.font.color.rgb = RGBColor(30, 41, 59) # Slate-800
    return p

def add_body_p(doc, text, bold_prefix=None):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(2)
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.line_spacing = 1.15
    if bold_prefix:
        r_pre = p.add_run(bold_prefix)
        r_pre.font.name = "Arial"
        r_pre.font.size = Pt(10.5)
        r_pre.bold = True
        r_pre.font.color.rgb = RGBColor(30, 41, 59)
    run = p.add_run(text)
    run.font.name = "Arial"
    run.font.size = Pt(10.5)
    run.font.color.rgb = RGBColor(51, 65, 85) # Slate-700
    return p

def add_bullet_p(doc, bold_label, text):
    p = doc.add_paragraph(style='List Bullet')
    p.paragraph_format.space_before = Pt(1)
    p.paragraph_format.space_after = Pt(3)
    p.paragraph_format.line_spacing = 1.15
    r_bold = p.add_run(bold_label)
    r_bold.font.name = "Arial"
    r_bold.font.size = Pt(10.5)
    r_bold.bold = True
    r_bold.font.color.rgb = RGBColor(30, 41, 59)
    
    r_text = p.add_run(f" {text}")
    r_text.font.name = "Arial"
    r_text.font.size = Pt(10.5)
    r_text.font.color.rgb = RGBColor(51, 65, 85)
    return p

def style_table_header(row, col_widths, bg_hex="1E293B"):
    for idx, cell in enumerate(row.cells):
        set_cell_background(cell, bg_hex)
        set_cell_margins(cell, top=120, bottom=120, left=140, right=140)
        cell.width = col_widths[idx]
        cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        for p in cell.paragraphs:
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            for r in p.runs:
                r.font.name = "Arial"
                r.font.size = Pt(9.5)
                r.bold = True
                r.font.color.rgb = RGBColor(255, 255, 255)

def style_table_data_row(row, col_widths, is_even=False):
    bg_hex = "F8FAFC" if is_even else "FFFFFF"
    for idx, cell in enumerate(row.cells):
        set_cell_background(cell, bg_hex)
        set_cell_margins(cell, top=100, bottom=100, left=140, right=140)
        cell.width = col_widths[idx]
        cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        for p in cell.paragraphs:
            for r in p.runs:
                r.font.name = "Arial"
                r.font.size = Pt(9.0)
                r.font.color.rgb = RGBColor(30, 41, 59)

def generate_report():
    doc = docx.Document()
    
    # Page setup - 0.8 in margins
    for sec in doc.sections:
        sec.top_margin = Inches(0.8)
        sec.bottom_margin = Inches(0.8)
        sec.left_margin = Inches(0.8)
        sec.right_margin = Inches(0.8)

    # Title
    p_title = doc.add_paragraph()
    p_title.paragraph_format.space_before = Pt(0)
    p_title.paragraph_format.space_after = Pt(2)
    r_title = p_title.add_run("Real Estate Price Analysis and Prediction Using Hadoop")
    r_title.font.name = "Arial"
    r_title.font.size = Pt(22)
    r_title.bold = True
    r_title.font.color.rgb = RGBColor(15, 23, 42)

    # Subtitle
    p_sub = doc.add_paragraph()
    p_sub.paragraph_format.space_before = Pt(0)
    p_sub.paragraph_format.space_after = Pt(14)
    r_sub = p_sub.add_run(
        "CSE3712 — Big Data Analytics Capstone Report: Data Preparation, Apache Hadoop MapReduce "
        "(HDFS & YARN), Scikit-Learn Ridge Regression Pipeline, and Interactive Streamlit Analytics"
    )
    r_sub.font.name = "Arial"
    r_sub.font.size = Pt(11)
    r_sub.font.color.rgb = RGBColor(71, 85, 105)

    # -------------------------------------------------------------------------
    # 1. Problem Statement & Big Data Architecture
    # -------------------------------------------------------------------------
    add_heading_1(doc, "1. Problem Statement & Big Data Architecture")
    add_body_p(
        doc,
        "Real estate transaction analysis presents classic Big Data challenges: high record volumes, unstandardized "
        "and noisy administrative inputs, quoted text fields containing embedded commas, duplicate property entries, "
        "and conflicting updates across recording periods. Standard single-machine desktop tools (such as Microsoft Excel "
        "or basic in-memory Python scripts) fail when ingesting multi-million transaction archives due to memory bottlenecks, "
        "silent field misalignments, and untracked record attrition."
    )
    add_body_p(
        doc,
        "The core objective of this project is to implement an enterprise-grade Big Data Analytics pipeline using "
        "Apache Hadoop MapReduce (HDFS and YARN) to clean, validate, and standardize real-world property sales data, "
        "followed by predictive pricing evaluation and interactive visual analytics. Specifically, the project delivers:",
        bold_prefix="Core Objectives: "
    )
    add_bullet_p(
        doc,
        "Distributed Ingestion & Storage:",
        "Storing original, uncompressed HM Land Registry CSV archives directly in Hadoop Distributed File System "
        "(HDFS) in raw format, supporting both a fast classroom demonstration sample and full multi-million record processing."
    )
    add_bullet_p(
        doc,
        "Scalable MapReduce Data Cleaning:",
        "Executing custom Python Mapper and Reducer jobs via Hadoop Streaming on YARN, handling quote parsing, "
        "attribute normalization, out-of-range price filtering, duplicate resolution, and exclusion of conflicting IDs."
    )
    add_bullet_p(
        doc,
        "Strict Mathematical Conservation Accounting:",
        "Tracking every single record with native Hadoop counters so that raw input rows equal rejected rows plus "
        "duplicates removed plus conflicts excluded plus final clean rows with zero untracked loss."
    )
    add_bullet_p(
        doc,
        "Temporal Predictive Modeling & Web Delivery:",
        "Training an interpretable Scikit-Learn Ridge regression pipeline evaluated strictly out-of-time (trained on 2022, "
        "tested on 2023) and delivering live insights through a single-page interactive Streamlit dashboard."
    )

    # -------------------------------------------------------------------------
    # 2. Dataset Explanation & Hadoop HDFS Storage
    # -------------------------------------------------------------------------
    add_heading_1(doc, "2. Dataset Architecture & Hadoop HDFS Storage")
    add_body_p(
        doc,
        "The empirical data source for this study is the official United Kingdom HM Land Registry Price Paid Data for the "
        "complete calendar years 2022 and 2023. Unlike synthetic or sample Kaggle datasets, this comprises official "
        "government land registration filings covering England and Wales, containing 1,939,188 total raw sales transactions "
        "(1,077,506 transactions in 2022 and 861,682 in 2023)."
    )
    add_heading_2(doc, "Raw CSV Layout & Field Normalization")
    add_body_p(
        doc,
        "Each raw record contains 16 comma-separated fields. Crucially, several street and locality address fields are quoted "
        "and contain internal commas, requiring robust CSV parser handling rather than naive string splits:"
    )
    add_bullet_p(doc, "Transaction Identifier (UUID):", "Unique registry identifier enclosed in curly braces (e.g., '{01EB45F0-49DB-40F3-E063-4704A8C05FDE}').")
    add_bullet_p(doc, "Sale Price:", "Integer transaction price in British Pounds Sterling (GBP).")
    add_bullet_p(doc, "Sale Date:", "Date and time of transfer in 'YYYY-MM-DD HH:MM' format.")
    add_bullet_p(doc, "Property Type Code:", "Single character category: D (Detached), S (Semi-Detached), T (Terraced), F (Flat / Maisonette), O (Other).")
    add_bullet_p(doc, "Tenure Duration Code:", "Ownership structure: F (Freehold) or L (Leasehold).")
    add_bullet_p(doc, "County Field:", "Administrative county directly present at column index 13, eliminating any requirement for external postcode geocoding tables.")

    add_heading_2(doc, "Project Price Filter Rule (£15,000 to £20,000,000)")
    add_body_p(
        doc,
        "To ensure robust and realistic residential valuation, an explicit price boundary filter of £15,000 to £20,000,000 "
        "is enforced directly in the Hadoop Mapper. Transactions below £15,000 represent non-market nominal transfers, family inheritances, "
        "or fractional garage/land lease re-assignments rather than true residential open-market sales. Conversely, transactions exceeding "
        "£20,000,000 represent institutional bulk apartment portfolio acquisitions or major commercial redevelopments that distort "
        "consumer residential pricing distributions. Out-of-range records are tracked separately from format errors."
    )

    add_heading_2(doc, "Hadoop HDFS Directory Architecture")
    add_body_p(
        doc,
        "Original raw CSV files are uploaded directly into HDFS without local modification. The project isolates demonstration "
        "runs from complete dataset processing through segregated HDFS storage paths:"
    )
    add_bullet_p(doc, "Sample Raw HDFS Path:", "/data/realestate/raw/sample/sample_raw.csv (20,020 records across 2022 & 2023).")
    add_bullet_p(doc, "Sample Clean HDFS Path:", "/data/realestate/clean/sample/part-* (Cleaned partitioned output).")
    add_bullet_p(doc, "Full Raw HDFS Path:", "/data/realestate/raw/full/pp-2022.csv and pp-2023.csv (1.939M total rows).")
    add_bullet_p(doc, "Full Clean HDFS Path:", "/data/realestate/clean/full/part-* (Production cleaned partitions).")

    # -------------------------------------------------------------------------
    # 3. Data Cleaning via Hadoop MapReduce Streaming (Mapper & Reducer)
    # -------------------------------------------------------------------------
    add_heading_1(doc, "3. Data Cleaning via Hadoop MapReduce Streaming Job")
    add_body_p(
        doc,
        "The core Big Data computation is implemented via Apache Hadoop Streaming running on YARN. Processing is divided "
        "cleanly between mapper.py and reducer.py:"
    )
    add_bullet_p(
        doc,
        "Mapper Mechanics (scripts/mapper.py):",
        "Reads raw lines from sys.stdin via Python's standard csv.reader module. Strips surrounding curly braces from the "
        "transaction ID, standardizes the county name to uppercase, parses and validates the price integer, validates the "
        "date format (YYYY-MM-DD) and year range (2022-2023), enforces property type (D, S, T, F, O) and tenure (F, L) "
        "whitelists, derives sale_year and sale_month, and emits a tab-separated key-value stream where the key is the clean "
        "transaction ID and the value comprises the 7 normalized attributes."
    )
    add_bullet_p(
        doc,
        "Reducer Mechanics (scripts/reducer.py):",
        "Reads sorted key-value pairs from sys.stdin grouped by transaction ID. For multiple records sharing an identical ID: "
        "if all attribute values are identical, exactly one clean record is emitted and extra records increment duplicate_rows_removed. "
        "If records sharing an ID have conflicting prices or attributes, all records for that ID are dropped to prevent corrupted "
        "data entry, incrementing conflicting_rows_excluded."
    )

    add_heading_2(doc, "Real Transaction Side-by-Side Comparison")
    add_body_p(
        doc,
        "Below is an actual transaction (ID: 01EB45F0-49DB-40F3-E063-4704A8C05FDE) traced through the Hadoop MapReduce pipeline:"
    )

    # Table 1: Raw vs Clean
    col_widths_t1 = [Inches(1.8), Inches(2.2), Inches(1.8), Inches(2.2)]
    t1 = doc.add_table(rows=7, cols=4)
    t1.alignment = WD_TABLE_ALIGNMENT.CENTER
    
    headers_t1 = ["Field Attribute", "Raw Input Value (CSV)", "Clean Output Value (TSV)", "Transformation / Rule"]
    for i, h in enumerate(headers_t1):
        t1.rows[0].cells[i].paragraphs[0].text = h
    style_table_header(t1.rows[0], col_widths_t1)

    t1_data = [
        ("Transaction ID", '"{01EB45F0-49DB-40F3-E063-4704A8C05FDE}"', "01EB45F0-49DB-40F3-E063-4704A8C05FDE", "Stripped outer quotes and curly braces {}"),
        ("Sale Price", "169950", "169950", "Parsed integer; validated £15k–£20M filter"),
        ("Sale Date", '"2023-07-05 00:00"', "2023-07-05", "Stripped timestamp; formatted YYYY-MM-DD"),
        ("Derived Temporal", "(Not in raw CSV)", "Year: 2023 | Month: 07", "Extracted sale_year & sale_month for ML"),
        ("County", '"TYNE AND WEAR"', "TYNE AND WEAR", "Trimmed quotes; uppercase standardized"),
        ("Property Type & Tenure", 'Type: "T" | Tenure: "F"', "Type: T | Tenure: F", "Validated against accepted codes (T, F)")
    ]

    for r_idx, row_data in enumerate(t1_data, start=1):
        for c_idx, val in enumerate(row_data):
            t1.rows[r_idx].cells[c_idx].paragraphs[0].text = val
        style_table_data_row(t1.rows[r_idx], col_widths_t1, is_even=(r_idx % 2 == 0))

    doc.add_paragraph().paragraph_format.space_after = Pt(4)

    add_heading_2(doc, "Strict Hadoop Counter Reconciliation Accounting")
    add_body_p(
        doc,
        "Every raw transaction is audited with complete mathematical conservation according to the formula: "
        "raw_rows = rejected_rows + duplicate_rows_removed + conflicting_rows_excluded + final_clean_rows. "
        "Zero records are discarded without an explicit logged counter."
    )

    # Table 2: Counters
    col_widths_t2 = [Inches(2.8), Inches(1.8), Inches(1.8), Inches(1.6)]
    t2 = doc.add_table(rows=11, cols=4)
    t2.alignment = WD_TABLE_ALIGNMENT.CENTER

    headers_t2 = ["Pipeline Audit Metric", "Sample Mode (Demo)", "Full Mode (Complete)", "Conservation Term"]
    for i, h in enumerate(headers_t2):
        t2.rows[0].cells[i].paragraphs[0].text = h
    style_table_header(t2.rows[0], col_widths_t2)

    t2_data = [
        ("Total Raw Input Rows", "20,020", "1,939,188", "raw_rows"),
        ("[-] Total Mapper Rejected Rows", "54", "10,077", "rejected_rows"),
        ("  • Out-of-Range Price (<£15k or >£20M)", "46", "10,077", "rejected_out_of_range"),
        ("  • Missing Required Fields / Null County", "2", "0", "rejected_missing_field"),
        ("  • Invalid Price Formats", "2", "0", "rejected_invalid_price"),
        ("  • Invalid Date Formats", "2", "0", "rejected_invalid_date"),
        ("  • Unsupported Category Codes", "2", "0", "rejected_unsupported_cat"),
        ("[-] Reducer Duplicate Rows Removed", "5", "0", "duplicate_rows_removed"),
        ("[-] Reducer Conflicting Rows Excluded", "4", "0", "conflicting_rows_excluded"),
        ("[=] Final Clean Output Rows", "19,957", "1,929,111", "final_clean_rows")
    ]

    for r_idx, row_data in enumerate(t2_data, start=1):
        for c_idx, val in enumerate(row_data):
            t2.rows[r_idx].cells[c_idx].paragraphs[0].text = val
        style_table_data_row(t2.rows[r_idx], col_widths_t2, is_even=(r_idx % 2 == 0))

    doc.add_paragraph().paragraph_format.space_after = Pt(4)
    add_body_p(
        doc,
        "Mathematical Verification: In Sample Mode, 20,020 = 54 + 5 + 4 + 19,957 (Exact Match ✅). "
        "In Full Mode, 1,939,188 = 10,077 + 0 + 0 + 1,929,111 (Exact Match ✅).",
        bold_prefix="Accounting Status: "
    )
    add_body_p(
        doc,
        "The sample dataset was created by extracting 10,000 real records from 2022 and 10,000 real records from 2023. "
        "To test all edge-case branches in the classroom, 20 demonstration rows were injected with explicit labels "
        "(5 identical duplicates of records 100-500, 4 conflicting rows under '{DEMO-CONFLICT-*}', 3 out-of-range prices, "
        "2 non-numeric prices, 2 malformed dates, 2 empty counties, and 2 invalid category codes). In addition, 43 real "
        "transactions naturally fell outside the £15k–£20M price rule, producing exactly 46 out-of-range price rejections.",
        bold_prefix="Sample Composition Note: "
    )

    # -------------------------------------------------------------------------
    # 4. Machine Learning Modeling & Comparative Evaluation
    # -------------------------------------------------------------------------
    add_heading_1(doc, "4. Machine Learning Modeling & Comparative Evaluation")
    add_body_p(
        doc,
        "To predict residential property values from the cleaned Hadoop output, we implemented a single, highly interpretable "
        "Scikit-Learn Ridge regression pipeline. The pipeline uses TransformedTargetRegressor to model sales prices on the log scale "
        "[y_trans = log(1 + price)], which stabilizes variance across high-value luxury tiers and prevents negative price outputs."
    )
    add_bullet_p(doc, "Categorical Features:", "County, property type, and tenure duration are encoded via OneHotEncoder(handle_unknown='ignore').")
    add_bullet_p(doc, "Temporal Features:", "Derived sale_year and sale_month are normalized via StandardScaler.")
    add_bullet_p(doc, "Evaluation Protocol:", "Strict out-of-time temporal validation: trained strictly on 2022 sales transactions, evaluated on 2023 sales.")

    # Table 3: ML Comparison
    col_widths_t3 = [Inches(2.6), Inches(1.8), Inches(1.8), Inches(1.8)]
    t3 = doc.add_table(rows=6, cols=4)
    t3.alignment = WD_TABLE_ALIGNMENT.CENTER

    headers_t3 = ["Evaluation Metric", "Naive Baseline (Median)", "Ridge Regression Pipeline", "Improvement / Status"]
    for i, h in enumerate(headers_t3):
        t3.rows[0].cells[i].paragraphs[0].text = h
    style_table_header(t3.rows[0], col_widths_t3)

    t3_data = [
        ("Training Set (2022 Sales)", "9,997 (Sample) / 1,071,952 (Full)", "9,997 (Sample) / 1,071,952 (Full)", "Historical baseline fitting"),
        ("Testing Set (2023 Sales)", "9,960 (Sample) / 857,159 (Full)", "9,960 (Sample) / 857,159 (Full)", "Out-of-time evaluation"),
        ("Sample Mode Test MAE", "£179,543 (Median: £290k)", "£142,205", "+£37,338 (20.8% error reduction)"),
        ("Full Mode Test MAE", "£209,672 (Median: £280k)", "£155,438", "+£54,234 (25.9% error reduction)"),
        ("Log-Scale R² Score", "0.0000 (Baseline)", "0.3341 (Sample) / 0.4884 (Full)", "Substantial variance captured")
    ]

    for r_idx, row_data in enumerate(t3_data, start=1):
        for c_idx, val in enumerate(row_data):
            t3.rows[r_idx].cells[c_idx].paragraphs[0].text = val
        style_table_data_row(t3.rows[r_idx], col_widths_t3, is_even=(r_idx % 2 == 0))

    doc.add_paragraph().paragraph_format.space_after = Pt(4)
    add_body_p(
        doc,
        '"The Ridge regression model achieves an out-of-time test MAE of £142,205 in sample mode (£155,438 in full mode), '
        'which reduces prediction error by over 20% to 25.9% compared to the naive baseline of always predicting the historical training median price."',
        bold_prefix="One-Sentence Viva Summary: "
    )

    # -------------------------------------------------------------------------
    # 5. Interactive Streamlit Web Dashboard
    # -------------------------------------------------------------------------
    add_heading_1(doc, "5. Interactive Streamlit Web Dashboard")
    add_body_p(
        doc,
        "A single-page, responsive Streamlit dashboard (app/dashboard.py) connects the exported Hadoop MapReduce clean output "
        "to end-user analytics and live valuation. The dashboard delivers:"
    )
    add_bullet_p(
        doc,
        "1. Live Data Cleaning Audit Cards:",
        "Real-time visual display of total raw rows, mapper rejections, duplicates removed, conflicts excluded, and clean output rows, "
        "coupled with an automated mathematical conservation check (PASSED ✅)."
    )
    add_bullet_p(
        doc,
        "2. Top 10 Counties Volume & Median Price Chart:",
        "Identifies the highest volume housing markets across England and Wales (e.g., Greater London, Greater Manchester, West Midlands) "
        "with corresponding median prices."
    )
    add_bullet_p(
        doc,
        "3. Property Type Price Distribution Chart:",
        "Clearly visualizes property type price hierarchy (Detached: ~£410,000 > Semi-Detached: ~£265,000 > Terraced: ~£215,000 > Flats: ~£210,000)."
    )
    add_bullet_p(
        doc,
        "4. Chronological 24-Month Price Trend Chart:",
        "Monthly median trajectory tracking market shifts from January 2022 to December 2023."
    )
    add_bullet_p(
        doc,
        "5. Interactive 'Estimate Sale Price' Form:",
        "Allows real-time user selection of county, property type, tenure, sale year, and month, instantly returning the Ridge regression "
        "price estimate (e.g., Greater London Flat, Leasehold, June 2023 estimates at £470,725.95)."
    )

    doc.save("Advanced_Real_Estate_Valuation_Report.docx")
    print("[SUCCESS] Regenerated Advanced_Real_Estate_Valuation_Report.docx with complete Hadoop pipeline documentation.")

if __name__ == "__main__":
    generate_report()
