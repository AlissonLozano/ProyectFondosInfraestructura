"Send email using ses"
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import boto3

class HandlerNotify:
    "HandlerNotify"

    def __init__(self, aws_region_name=str):
        self.__aws_region_name= aws_region_name

    def enviar_email(
        self,
        source_email: str,
        destination_email: list,
        asunto: str,
        body_html: str
    ):
        "Funcion para enviar mensajes al correo"
        ses_client = boto3.client("ses", region_name=self.__aws_region_name)
        msg = MIMEMultipart("mixed")
        msg["Subject"] = asunto
        msg["From"] = source_email
        msg["To"] = ",".join(destination_email)
        msg_body = MIMEMultipart("alternative")
        htmlpart = MIMEText(body_html.encode("utf-8"), "html", "utf-8")

        msg_body.attach(htmlpart)
        msg.attach(msg_body)

        ses_client.send_raw_email(
            Source=source_email,
            Destinations=destination_email,
            RawMessage={
                "Data": msg.as_string(),
            },
        )

    def enviar_celular(self,
        phone_number: str,
        message:str
    ):
        "Send email through SES"
        sns_client = boto3.client("sns", region_name=self.__aws_region_name)
        sns_client.publish(
            PhoneNumber= phone_number,
            Message= message
        )