import streamlit as st

st.title("🎉 Hello from Streamlit!")
st.write("If you see this, your deployment works!")
st.success("✅ App is running successfully")

# Show Python version
import sys
st.write(f"Python version: {sys.version}")

# Show current directory and files
import os
st.write(f"Current directory: {os.getcwd()}")
st.write("Files in current directory:")
for item in os.listdir(".")[:20]:
    st.write(f"  - {item}")
