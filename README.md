# 🌥 Smart Cloud Cost Analytics Dashboard

A powerful, AI-enhanced analytics dashboard built during my internship at *Bluecore, designed to monitor, analyze, and optimize **Google Cloud Platform (GCP)* billing and cost performance in real time.

> 🚀 Built using *Go, **GCP Billing API, **BigQuery, **Google Sheets API, **AI/ML APIs, **Docker, **Terraform, and **Cloud Run*

---

## 📌 Key Features

- 📊 *Real-Time Billing Analytics*: Pull and analyze cost breakdowns using GCP Billing API + BigQuery.
- 🤖 *AI-Powered Insights*: Leverage ML models for anomaly detection and future cost forecasting.
- 📈 *Google Sheets Integration*: Automatically export summarized billing reports to Google Sheets.
- ☁ *Cloud-Native Deployment*: Seamlessly deployable with Docker + Cloud Run.
- 🧱 *Infra-as-Code*: Reproducible infrastructure using Terraform.

---

## 🧭 Architecture Overview

lua
Copy
Edit
  +-------------------+       +----------------------+
  | GCP Billing API   |-----> | BigQuery             |
  +-------------------+       +----------+-----------+
                                          |
                                          v
  +----------------------------+   +-------------+
  | Go Backend (Billing Logic) |-->| AI/ML Layer |
  +----------------------------+   +-------------+
                 |
                 v
       +------------------+
       | Google Sheets API|
       +------------------+
                 |
          +--------------+
          | Export Report|
          +--------------+
yaml
Copy
Edit

*(See docs/architecture.png for full details.)*

---

## 🗃 Project Structure

smart-cloud-cost-analytics-dashboard/
├── backend/ # Go backend services
├── ml/ # Python ML models
├── terraform/ # Infrastructure-as-code
├── deploy/ # Docker + Cloud Run configs
├── scripts/ # Google Sheets, data loaders
├── docs/ # Architecture diagram, documentation
├── README.md
├── LICENSE
└── .gitignore

yaml
Copy
Edit

---

## ⚙ Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/<your-username>/smart-cloud-cost-analytics-dashboard.git
cd smart-cloud-cost-analytics-dashboard
2. Configure Terraform
Edit variables in:

bash
Copy
Edit
terraform/variables.tf
Then run:

bash
Copy
Edit
cd terraform
terraform init
terraform apply
3. Build and Run Backend (Locally)
bash
Copy
Edit
cd backend
go mod tidy
go run main.go
4. Export Reports to Google Sheets
bash
Copy
Edit
cd scripts
python3 export_to_sheets.py
🧠 AI/ML Layer
The ML module handles:

Anomaly detection on sudden spikes

Forecasting cloud spend using time series data

Implemented using:

scikit-learn

Prophet or ARIMA

BigQuery ML integration (optional)

🐳 Docker + Cloud Run
Build Docker Image
bash
Copy
Edit
docker build -t cost-dashboard-backend ./deploy
Deploy to Cloud Run
bash
Copy
Edit
gcloud run deploy cost-dashboard \
  --image=gcr.io/<your-project-id>/cost-dashboard-backend \
  --region=us-central1 \
  --allow-unauthenticated
✅ Tech Stack
Layer	Tech
Language	Go, Python
Infra-as-Code	Terraform
ML/AI	scikit-learn, Prophet
GCP Services	Billing API, BigQuery, Cloud Run
External	Google Sheets API
Containerization	Docker
CI/CD (optional)	GitHub Actions

📃 License
This project is licensed under the MIT License – see the LICENSE file for details.
