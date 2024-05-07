import pandas as pd
import numpy as np
from sklearn import preprocessing
from sklearn.preprocessing import LabelEncoder
import faiss
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
 
 
 
df = pd.read_csv("data.csv")
 
 
# Create a label encoder object
label_encoder = LabelEncoder()
 
 
# Fit and transform the 'mobile_name' column
df['mobile_name_encoded'] = label_encoder.fit_transform(df['mobile_name'])
# View the encoded data
print(df[['mobile_name', 'mobile_name_encoded']].head())
 
 
# Vectorize the mobile names
vectorizer = CountVectorizer()
mobile_vectors = vectorizer.fit_transform(df['mobile_name'])
 
# Function to perform vector search
def search_similar_mobile(query_mobile_name, top_n=10):
    # Vectorize the query mobile name
    query_vector = vectorizer.transform([query_mobile_name])
   
    # Calculate cosine similarity between query vector and all mobile vectors
    similarities = cosine_similarity(query_vector, mobile_vectors)
   
    # Get indices of top similar mobiles
    top_indices = similarities.argsort(axis=1)[0][-top_n:][::-1]
   
    # Retrieve top similar mobile names
    similar_mobiles = [(df['mobile_name'][i], df['rating'][i]) for i in top_indices]
       
    return similar_mobiles
 
# Example usage
query_name = "vivo"
similar_mobiles = search_similar_mobile(query_name)
 
print("Similar mobiles to", query_name, ":", similar_mobiles)
 
for mobile,rating in similar_mobiles:
    print(mobile, "- Rating: ",rating)  