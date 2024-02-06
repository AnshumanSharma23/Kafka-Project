import streamlit as st
import requests

def send_message(message):
    try:
        response = requests.post("http://localhost:8000/send_message", data=message.encode('utf-8'))
        response.raise_for_status()  # Raise an exception for HTTP errors
        st.success("Message sent successfully!")
        return True
    except requests.exceptions.RequestException as e:
        st.error(f"Error sending message: {e}")
        return False

def fetch_messages():
    try:
        response = requests.get("http://localhost:8000/receive_message")
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json().get('messages', [])
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching messages: {e}")
        return []

st.title("Chat App")

message = st.text_input("Enter your message:")
if st.button("Send"):
    if send_message(message):
        messages = fetch_messages()
        st.write("Message history:")
        for msg in messages:
            st.write(msg)

