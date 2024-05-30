import os
import openai
import streamlit as st

# Setting the API key
openai.api_key = os.getenv("OPEN_API_KEY")

# Function to interact with the chatbot
def chatbot_response(prompt):
    # Create a chatbot using ChatCompletion.create() function
    completion = openai.ChatCompletion.create(
        # Use GPT 3.5 as the LLM
        model="gpt-3.5-turbo",
        # Pre-define conversation messages for the possible roles 
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ]
    )
    return completion.choices[0].message

# Streamlit app code
def main():
    st.title("Chatbot with OpenAI GPT-3.5")
    # Input prompt from the user
    user_prompt = st.text_input("Ask something:")
    if user_prompt:
        # Get the response from the chatbot
        response = chatbot_response(user_prompt)
        # Display the response
        st.text_area("Response:", value=response, height=200)

if __name__ == "__main__":
    main()
