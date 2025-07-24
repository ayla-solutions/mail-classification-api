# Outlook Mail Classifier API

This FastAPI project connects to a mailbox using Microsoft Graph API, processes emails and attachments, classifies them, and pushes structured data into a Dataverse table.

The API is hosted in Azure client and is connected to this GitHub repo. Everytime a change is made to this repo, the API rebuilds itself.

## API access Links

- To check if the API is running: [`https://mail-classification-api.azurewebsites.net/`](https://mail-classification-api.azurewebsites.net/)
- To make a GET call (automatically fetch the emails): [`https://mail-classification-api.azurewebsites.net/mails`](https://mail-classification-api.azurewebsites.net/mails)
- For the interactive FAST API interface: [`https://mail-classification-api.azurewebsites.net/docs`](https://mail-classification-api.azurewebsites.net/docs)

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
├── .env                        # Environment variables 
├── Dockerfile                  # Docker configuration
├── main.py                     # FastAPI app entry point
├── requirements.txt            # Python dependencies
├── run_mail_api.bat            # One-click script to rebuild and run the API
└── readme.md                   # 📖 You're reading this!
```

---

## Functionality

#### 📂 utils/
- **auth.py**  
  Handles Azure Active Directory (AD) authentication to obtain and refresh access tokens for Microsoft Graph or Dataverse API.

- **classify.py**  
  Contains rule-based classification logic for assigning categories and priorities to incoming emails based on subject, body, and other metadata.

- **dataverse.py**  
  Responsible for formatting and sending HTTP POST requests to the Dataverse API to store classified email information into a specified table.

- **extract_attachments.py**  
  Parses email content and attachments. Extracts readable text from supported file types and prepares it for downstream processing.

---

#### 📄 .env  
Environment variables file used to store sensitive data like client credentials, tenant ID, table name, and API URLs. 

#### 📄 Dockerfile  
Defines the Docker environment and instructions to containerize the FastAPI application for deployment.

#### 📄 main.py  
The entry point of the FastAPI app. Starts the server and orchestrates the email classification and Dataverse push workflow.

#### 📄 requirements.txt  
Lists Python dependencies required to run the project, used during Docker image builds or manual environment setup.

#### 📄 run_mail_api.bat  
Windows batch script that automates the following:
1. Stops and removes existing container (`mail-api`)
2. Rebuilds the Docker image
3. Launches the API in a detached container with environment variables

#### 📄 readme.md  
Documentation file you're currently reading. Provides project overview, usage instructions, and other relevant notes.

---

## Permissions

The API has the following permissions:
1. To read from a mailbox mentioned in `.env` file.
2. To write to the dataverse table `Arth_Main`

---

## ⚙️ Setup Instructions

### 1. Clone the Repo (If not already)

```bash
git clone https://github.com/ayla-solutions/mail-categorisation-api.git
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

### 4. Azure CLI Setup

The GitHub Repo is connected to the Azure CLI,. Whenever you push a change, the API will rebuild itself.



### Notes

- The Dataverse table name used in the API payload should exactly match schema names like:
  crabb_sender, crabb_subject, crabb_attachment_names, etc.
- The table must be created in Power Apps and accessible to the registered app.
- If you are not seeing changes, check whether the Application User has permissions to that table.
