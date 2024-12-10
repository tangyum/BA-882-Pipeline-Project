import functions_framework
import json
from openai import OpenAI
from google.cloud import secretmanager
import re
import os
import uuid
from elevenlabs import VoiceSettings
from elevenlabs.client import ElevenLabs
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime

now = datetime.now()
current_datetime_string = now.strftime("%Y-%m-%d")

def clean_llm_output(text):
    # Remove unwanted characters
    text = re.sub(r'\n', '', text)

    text = re.sub(r'[^a-zA-Z0-9\s,.]', '', text)

    # Remove extra whitespaces
    text = re.sub(r'\s+', ' ', text).strip()

    # Convert to lowercase (optional)
    text = text.lower()

    return text



project_id = 'ba882-435919'
version_id = '1'
sm = secretmanager.SecretManagerServiceClient()

oname =  f"projects/{project_id}/secrets/openai_token/versions/{version_id}"
response = sm.access_secret_version(request={"name": oname})
openai_token = response.payload.data.decode("UTF-8")


ename =  f"projects/{project_id}/secrets/elevenlabs/versions/{version_id}"
response = sm.access_secret_version(request={"name": ename})
elevenlabs_token = response.payload.data.decode("UTF-8")

aname =  f"projects/{project_id}/secrets/app_password/versions/{version_id}"
response = sm.access_secret_version(request={"name": aname})
app_pass = response.payload.data.decode("UTF-8")


ELEVENLABS_API_KEY = elevenlabs_token
cliente = ElevenLabs(
    api_key=ELEVENLABS_API_KEY,
)

def text_to_speech_file(text: str, model_id:str) -> str:
    # Calling the text_to_speech conversion API with detailed parameters
    response = cliente.text_to_speech.convert(
        voice_id="M0IvLNu6hH3cNnETNLEP", # Lucas Reed
        output_format="mp3_22050_32",
        text=text,
        model_id=model_id, # use the turbo model for low latency
        voice_settings=VoiceSettings(
            stability=0.3,
            similarity_boost=0.6,
            style=0.6,
            use_speaker_boost=True,
        ),
    )

    # Generating a unique file name for the output MP3 file
    save_file_path = f"{uuid.uuid4()}.mp3"

    # Writing the audio to a file
    with open(save_file_path, "wb") as f:
        for chunk in response:
            if chunk:
                f.write(chunk)

    print(f"{save_file_path}: A new audio file was saved successfully!")

    # Return the path of the saved audio file
    return save_file_path


@functions_framework.http
def task(request):
  request_json = request.get_json(silent=True)
  print(f"request: {json.dumps(request_json)}")
  summaries = request_json.get('summaries').values()
  report = ('\n\n').join(summaries)


  # Generate Script

  os.environ['OPENAI_API_KEY'] = openai_token
  clienta = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

  # Summarization prompt
  prompt = f"""Create an energetic podcast script for 'The Markets in Five Minutes or less' by AI host Lucas Reed about 700 words long using the below stock report, 
make it conversational, single speaker, plain text (to be used in TTS model). Dont not include any parenthetical information, and write out symbols in english. Only output exactly what the host will read out loud.
Stock Report: {report}"""

  # Send OpenAI request
  chat_completion = clienta.chat.completions.create(
      messages=[{"role": "user", "content": prompt}],
      model="gpt-4o",  # Adjust model as needed
  )
  script = chat_completion.choices[0].message.content
  print(script)

  # Generate body of newsletter
  # Summarization prompt
  prompt = f"""Create an newsletter in HTML format using the stock report below. Make sure to include all stocks,  with clear, interesting headings and sub headings, with the appropriate formatting in HTML. 
This will be sent by email as a daily stock update newsletter, so do not include anything that should not be mailed to the end user.  
Set the title as: The Markets in Five Minutes or less
Stock Report: {report}"""

# Send OpenAI request
  chat_completion = clienta.chat.completions.create(
messages=[{"role": "user", "content": prompt}],
model="gpt-4o",  # Adjust model as needed
)
  body = chat_completion.choices[0].message.content
  print(body)

  # generate audio
  text = clean_llm_output(script)
  audio_path = text_to_speech_file(text, 'eleven_turbo_v2_5') # eleven_multilingual_v2

  # send email

  # Email configuration
  sender_email = "emelyntang112@gmail.com"
  app_password = app_pass  # Generate this from Google Account settings
  main_receiver = 'drohit@bu.edu'
  bcc_emails = ['jasminek@bu.edu', 'shayanhk@bu.edu', 'tangyum@bu.edu', 'rohitdevanaboina@gmail.com', 'dshashipmp@yahoo.com', 'dhtikna7@gmail.com', 'SHASHI@ANRONASH.COM']
  # Create the email message
  message = MIMEMultipart()
  message["Subject"] = "The Markets in Five Minutes or less - " + current_datetime_string
  message["From"] = sender_email
  body = MIMEText(body, 'html' ) # convert the body to a MIME compatible string
  message.attach(body) # attach it to your main message

  # save summaries to a text file
  with open("raw_summary.txt", "w") as file:
    file.write(report)

  # Attach the text file
  with open("raw_summary.txt", "rb") as file:
      text_attachment = MIMEApplication(file.read(), _subtype="txt")
      text_attachment.add_header(
          "Content-Disposition",
          "attachment",
          filename="raw_summary.txt"
      )
      message.attach(text_attachment)

  # Save script to a text file
  with open("script.txt", "w") as file:
    file.write(script)

  # Attach the text file
  with open("script.txt", "rb") as file:
      text_attachment = MIMEApplication(file.read(), _subtype="txt")
      text_attachment.add_header(
          "Content-Disposition",
          "attachment",
          filename="script.txt"
      )
      message.attach(text_attachment)

  # Attach podcast file
  with open(audio_path, "rb") as file:
      audio_attachment = MIMEApplication(file.read(), _subtype="mp3")
      audio_attachment.add_header(
          "Content-Disposition",
          "attachment",
          filename='podcast.mp3'
      )
      message.attach(audio_attachment)


  # Send the email
  message["To"] = email
  msg['Bcc'] = ', '.join(bcc_emails)


  try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, app_password)
        server.send_message(message)
    print(f"Email sent successfully! to {email}")
  except Exception as e:
    print(f"Error sending email to {email}: {e}")

  return({'response':'mailed successfully!'})
