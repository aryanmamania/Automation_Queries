#!/usr/bin/env python3
import imaplib
import email
import re
import subprocess
import os
import tempfile
import smtplib
import time
from email.header import decode_header
from email.message import EmailMessage
from email.utils import parseaddr, getaddresses

EMAIL = 'aryan.mamania@fosteringlinux.com'
PASSWORD = '**** **** '
IMAP_SERVER = 'imap.gmail.com'
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 465
IMAP_FOLDER = 'INBOX'
ALLOWED_SUBJECT = "Execute the query Luffy"

def decode_mime_words(s):
    decoded = decode_header(s)
    return ''.join([str(t[0], t[1] or 'utf-8') if isinstance(t[0], bytes) else t[0] for t in decoded])

def get_all_recipients(msg):
    addresses = []
    for field in ["From", "To", "Cc"]:
        raw = msg.get_all(field, [])
        addresses += getaddresses(raw)
    unique_emails = set(addr.lower() for name, addr in addresses if addr)
    return list(unique_emails)

def get_unread_emails():
    print("[*] Connecting to mail server...")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL, PASSWORD)
    mail.select(IMAP_FOLDER)
    typ, data = mail.search(None, f'(UNSEEN SUBJECT "{ALLOWED_SUBJECT}")')
    mail_ids = data[0].split()
    print(f"[*] Found {len(mail_ids)} unread emails.")
    messages = []

    for num in mail_ids[-10:]:
        typ, msg_data = mail.fetch(num, '(RFC822)')
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                subject = decode_mime_words(msg["Subject"])
                sender = parseaddr(msg["From"])[1]
                message_id = msg["Message-ID"]
                body = ""

                if msg.is_multipart():
                    for part in msg.walk():
                        content_type = part.get_content_type()
                        content_disposition = str(part.get("Content-Disposition"))
                        if content_type == "text/plain" and "attachment" not in content_disposition:
                            try:
                                body += part.get_payload(decode=True).decode(errors="ignore")
                            except Exception as e:
                                print(f"[!] Error decoding part: {e}")
                else:
                    try:
                        body = msg.get_payload(decode=True).decode(errors="ignore")
                    except Exception as e:
                        print(f"[!] Error decoding message: {e}")
                recipients = get_all_recipients(msg)
                messages.append((subject, sender, recipients, message_id, body))
    return messages

def extract_multiple_queries(body):
    pattern = r'Database:\s*(\w+)\s*Query:\s*\n(.*?)(?=\nDatabase:|\Z)'
    matches = re.findall(pattern, body, re.IGNORECASE | re.DOTALL)
    queries = []

    for db, sql in matches:
        cleaned_sql = sql.strip()
        for footer in ['Thanks', 'Regards', '--']:
            cleaned_sql = cleaned_sql.split(footer)[0]
        queries.append((db.strip(), cleaned_sql.strip()))
    return queries

def run_query_with_ansible(db, query):
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".sql") as tmp:
        tmp.write(query)
        sql_file_path = tmp.name

    try:
        cmd = (
            f"ansible-playbook -i hosts.ini playbooks/run_query.yml "
            f"--extra-vars db_name='{db}' "
            f"--extra-vars sql_file='{sql_file_path}'"
        )
        print(f"[*] Running Ansible: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        match = re.search(r'"msg":\s*"((?:\\.|[^"\\])*)"', result.stdout, re.DOTALL)
        if match:
            raw_msg = match.group(1)
            output = bytes(raw_msg, "utf-8").decode("unicode_escape").strip()
        else:
            output = result.stdout.strip()

        if query.strip().lower().startswith("select"):
            rows = re.findall(r'- \{.*?\}', output)
            return "\n".join(rows) if rows else "Query executed. No rows returned."
        else:
            row_match = re.search(r'(\d+) row\(s\) affected', output)
            if row_match:
                return f"{row_match.group(1)} row(s) affected."
            return "Query executed. No rows returned or affected."
    finally:
        if os.path.exists(sql_file_path):
            os.remove(sql_file_path)

def send_combined_reply_email(sender_email, all_recipients, original_subject, results, original_msg_id=None):
    msg = EmailMessage()
    msg["From"] = EMAIL
    msg["To"] = sender_email

    # Filter out the sender and self from CC list
    cc_list = [email for email in all_recipients if email.lower() not in {sender_email.lower(), EMAIL.lower()}]
    if cc_list:
        msg["Cc"] = ', '.join(cc_list)

    msg["Subject"] = f"Re: {original_subject}"
    if original_msg_id:
        msg["In-Reply-To"] = original_msg_id
        msg["References"] = original_msg_id

    body = "Your queries have been executed.\n\n"
    for result in results:
        body += f"""\
Database: {result['db']}
Query:
{result['sql']}

Result:
{result['output']}

{'='*40}\n
"""
    msg.set_content(body)

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
        smtp.login(EMAIL, PASSWORD)
        smtp.send_message(msg)
        print(f"[+] Sent reply to {sender_email}, cc: {', '.join(cc_list)}")

if __name__ == "__main__":
    print("[*] Starting email monitoring loop...")
    while True:
        try:
            messages = get_unread_emails()
            for (subject, sender_email, recipient_list, msg_id, msg_body) in messages:
                if subject.strip() != ALLOWED_SUBJECT:
                    continue

                print(f"[+] Found matching email: '{subject}' from {sender_email}")
                queries = extract_multiple_queries(msg_body)
                results = []

                for db, query in queries:
                    if db and query:
                        print(f"[+] Running query for DB {db}:\n{query}")
                        try:
                            output = run_query_with_ansible(db, query)
                            results.append({'db': db, 'sql': query, 'output': output})
                        except subprocess.CalledProcessError as e:
                            results.append({'db': db, 'sql': query, 'output': f"Ansible failed: {e}"})

                if results:
                    send_combined_reply_email(
                        sender_email=sender_email,
                        all_recipients=recipient_list,
                        original_subject=subject,
                        results=results,
                        original_msg_id=msg_id
                    )
        except Exception as e:
            print(f"[!] Unexpected error: {e}")
        time.sleep(60)
