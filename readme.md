# Outlook Mail Classifier API

This FastAPI project connects to a mailbox using Microsoft Graph API, processes emails and attachments, classifies them, and pushes structured data into a Dataverse table.

The API is hosted in Azure client and is connected to this GitHub repo. Everytime a change is made to this repo, the API rebuilds itself.

## API access Links

- To check if the API is running: [`https://mail-classification-api.azurewebsites.net/`](https://mail-classification-api.azurewebsites.net/)
- To make a GET call (automatically fetch the emails): https://mail-classification-api.azurewebsites.net/mails
- For the interactive FAST API interface: https://mail-classification-api.azurewebsites.net/docs

---

## 📁 Project Structure

```

MAIL-CLASSIFICATION-API/
│
├── utils/
│ ├── auth.py                   # Handles Azure AD authentication
│ ├── classify.py               # Rule-based classification logic (category & priority)
│ ├── dataverse.py              # Dataverse API push logic
│ └── extract_attachments.py    # Extracts text from attachments and body
│
├── .env                        # Environment variables (not committed)
├── Dockerfile                  # Docker configuration
├── main.py                     # FastAPI app entry point
├── requirements.txt            # Python dependencies
├── run_mail_api.bat            # One-click script to rebuild and run the API
└── readme.md                   # 📖 You're reading this!
```

---

## ⚙️ Setup Instructions

### 1. Clone the Repo (If not already)

```bash
git clone <repo-url>
cd <project-folder>
```

### 2. Install dependencies

```
pip install -r requirements.txt
```

### 3. Dockerized Deployment

#### Local Setup

##### To Run the Docker Locally

```
docker stop mail-api
docker rm mail-api
docker build -t mail-api .
docker run -d --name mail-api -p 8000:8000 --env-file .env mail-api
```

OR

Run:

```
run_mail_api.bat // In Windows
```

#### To Check the logs

```
docker logs -f mail-api
```

#### To Test the API

###### To see if the API is Running...

```
http://localhost:8000
```

###### Simple GET request

```
http://localhost:8000/mails
```

###### Interactive API Test Page

```
http://localhost:8000/docs
```

### Notes

- The Dataverse table name used in the API payload should exactly match schema names like:
  crabb_sender, crabb_subject, crabb_attachment_names, etc.
- The table must be created in Power Apps and accessible to the registered app.
- If you are not seeing changes, check whether the Application User has permissions to that table.
