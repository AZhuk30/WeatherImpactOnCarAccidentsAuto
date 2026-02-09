import streamlit as st
2
st.title("🎉 Hello from Streamlit!")
4st.write("If you see this, your deployment works!")
5st.success("✅ App is running successfully")
6
7# Show Python version
8import sys
9st.write(f"Python version: {sys.version}")
10
11# Show current directory and files
12import os
13st.write(f"Current directory: {os.getcwd()}")
14st.write("Files in current directory:")
15for item in os.listdir(".")[:20]:
16    st.write(f"  - {item}")
17
