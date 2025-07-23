from fastapi import FastAPI
from utils.auth import get_graph_token
from utils.classify import classify_mail
from utils.extract_attachments import fetch_messages_with_attachments
from utils.dataverse import push_to_dataverse

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Mail Classification API is running"}

@app.get("/mails")
def process_mails():
    token = get_graph_token()
    mails = fetch_messages_with_attachments(token)

    for mail in mails:
        classification = classify_mail(mail)
        mail.update(classification)
        push_to_dataverse(mail)

    return {"status": "Processed", "count": len(mails)}
