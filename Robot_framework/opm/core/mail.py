import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import basename
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/')

# Email Sending Function
def send_email(subject: str, recipient: list, body: str, filenames_list=None):
    sender_email = "rs60@its.jnj.com"
    smtp_server = "smtp.na.jnj.com"  # Replace with your SMTP server

    try:
        # Create the email
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = ", ".join(recipient)
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))

        for f in filenames_list or []:
            with open(f, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=basename(f)
                )
            # After the file is closed
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            message.attach(part)

        # Connect and send the email
        server = smtplib.SMTP(smtp_server)
        server.default_port = 25
        server.send_message(message)
        server.quit()

        print("\nEmail sent successfully with the attached report.\n")
    except Exception as e:
        print(f"\nFailed to send email: {e}\n")


# give the destination like path to send  vis mail
# recipients = ["rs60@its.jnj.com", "ajena10@its.jnj.com"]
# file_path = [BASE_DIR+"/score_report/Auto Validation - 2024-12-19 01-49.xlsx"]
# if __name__ == "__main__":
#     send_email("Scorecrd_Report for NPI OTL", recipients, "Here is the Automation score report attached for test list shoot", file_path)