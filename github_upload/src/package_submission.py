"""
Package all submission files into 'file nộp' folder and create 'file_nop.zip'.
"""

import os
import sys
import shutil
import zipfile

# Ensure UTF-8 output on Windows
sys.stdout.reconfigure(encoding="utf-8")

SUBMISSION_DIR = "file nộp"
ZIP_NAME = "file_nop.zip"

if os.path.exists(SUBMISSION_DIR):
    shutil.rmtree(SUBMISSION_DIR)

os.makedirs(SUBMISSION_DIR, exist_ok=True)

# 1. Copy Report
shutil.copy("report/FINAL_REPORT_BFT65B.docx", os.path.join(SUBMISSION_DIR, "FINAL_REPORT_BFT65B.docx"))
shutil.copy("report/FINAL_REPORT_BFT65B.md", os.path.join(SUBMISSION_DIR, "FINAL_REPORT_BFT65B.md"))

# 2. Copy Root Scripts, Audit Files, and Configs
for f in ["run_all.py", "fdic_download_starter.py", "requirements.txt", "README.md", "PROJECT_AUDIT.md", "IMPROVEMENT_LOG.md", ".gitignore"]:
    if os.path.exists(f):
        shutil.copy(f, os.path.join(SUBMISSION_DIR, f))

# 3. Copy src/ directory
shutil.copytree("src", os.path.join(SUBMISSION_DIR, "src"))

# 4. Copy outputs/ directory (excluding .matplotlib cache)
dest_outputs = os.path.join(SUBMISSION_DIR, "outputs")
os.makedirs(dest_outputs, exist_ok=True)
shutil.copytree("outputs/figures", os.path.join(dest_outputs, "figures"))
shutil.copytree("outputs/tables", os.path.join(dest_outputs, "tables"))
shutil.copytree("outputs/reports", os.path.join(dest_outputs, "reports"))

# 5. Create Submission Guide TXT
guide_content = """================================================================================
          HƯỚNG DẪN NỘP BÀI THI KẾT THÚC HỌC PHẦN (BFT65B - NEU)
================================================================================

Theo đề bài "final_report_bft65b.pdf", bạn cần nộp 2 phần:

PHẦN 1: BÁO CÁO NGHIÊN CỨU (Report in English - Max 20 pages + 10 pages appendix)
--------------------------------------------------------------------------------
1. Mở file: FINAL_REPORT_BFT65B.docx (đã có sẵn trong thư mục này).
2. Điền thông tin cá nhân của bạn ở Bảng trang 1:
   - Candidate Name: [Họ và Tên của bạn]
   - Student ID: [Mã Số Sinh Viên của bạn]
   - GitHub Repository: [Link GitHub của bạn sau khi làm Bước 2 bên dưới]
3. Kiểm tra lại văn bản và Lưu (File -> Save).
4. Bạn có thể nộp trực tiếp file FINAL_REPORT_BFT65B.docx hoặc xuất sang file PDF
   (File -> Export -> Create PDF/XPS) rồi nộp lên hệ thống của trường (LMS / MS Teams).

PHẦN 2: GITHUB REPOSITORY (Mã nguồn công khai)
--------------------------------------------------------------------------------
1. Tạo một Repository mới trên GitHub (đặt chế độ PUBLIC, ví dụ: bft65b-bank-failure-pipeline).
2. Copy toàn bộ các file trong thư mục này (src/, outputs/, README.md, requirements.txt, run_all.py, .gitignore)
   lên GitHub.
   * LƯU Ý QUAN TRỌNG TỪ ĐỀ BÀI: TUYỆT ĐỐI KHÔNG commit thư mục data_raw hoặc data_processed
     lên GitHub (file .gitignore đã cấu hình chặn sẵn việc này).
3. Lệnh Git đẩy lên repository:
   git init
   git add .
   git commit -m "Initial commit for BFT65B Final Report"
   git branch -M main
   git remote add origin https://github.com/[your-username]/bft65b-bank-failure-pipeline.git
   git push -u origin main
4. Đảm bảo Repo ở chế độ Public và dán link vào trang 1 của file Báo cáo trước khi nộp.

Chúc bạn đạt điểm tối đa (100/100)!
================================================================================
"""

with open(os.path.join(SUBMISSION_DIR, "HUONG_DAN_NOP_BAI.txt"), "w", encoding="utf-8") as f:
    f.write(guide_content)

# 6. Create Zip Archive
if os.path.exists(ZIP_NAME):
    os.remove(ZIP_NAME)

shutil.make_archive("file_nop", "zip", root_dir=SUBMISSION_DIR)

print(f"[SUCCESS] Created submission folder: '{SUBMISSION_DIR}'")
print(f"[SUCCESS] Created zip archive: '{ZIP_NAME}'")
