# Đưa dự án lên GitHub

Tên repository đề xuất: `azure-retail-lakehouse-platform`.

```powershell
git init
git add .
git status
git diff --cached --check
git commit -m "feat: build Azure retail lakehouse platform"
git branch -M main
git remote add origin https://github.com/TAI_KHOAN/azure-retail-lakehouse-platform.git
git push -u origin main
```

Trước `git add`, xác nhận các mục sau không xuất hiện trong `git status`:

- `.env`;
- `data/generated/`;
- connection string, token hoặc subscription ID;
- ảnh chụp có secret;
- `.venv/`, build output và Databricks local state.

GitHub Actions `CI` chạy tự động. Workflow deploy Databricks chỉ chạy thủ công và
cần cấu hình GitHub Environment secrets/variables; không thêm secret thật vào YAML.

