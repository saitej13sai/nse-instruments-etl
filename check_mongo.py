from pymongo import MongoClient
# Replace with your MongoDB URI
client = MongoClient("mongodb+srv://myuser:mypassword123@cluster0.kxzttwo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", serverSelectionTimeoutMS=10000)
db = client["market_data"]
collection = db["upstox_nse"]
print(f"I found {collection.count_documents({})} toys in MongoDB!")
for toy in collection.find().limit(5):
    print(toy)
client.close()