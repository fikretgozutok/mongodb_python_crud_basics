from pymongo import MongoClient

URI = "mongodb://localhost:27017"
DB_NAME = "library"
COLLECTION_NAME = "books"

client = None
collection = None

def connect_db():
    global client, collection
    try:
        client = MongoClient(URI)
        db = client.get_database(DB_NAME)
        collection = db.get_collection(COLLECTION_NAME)
        print("Server connection successfull")
        return True
    except Exception as e:
        print(f"Connection error: {e}")
        return False

def close_connection():
    global client

    if not client:
        print("Client not found")
        return
    try:
        client.close()
        print("Connection closed successfully")
    except Exception as e:
        print(f"An error occured when close the connection: {e}")

def create_book(title, author, genre, page_count):
    global collection

    result = None
    
    instance = {
        "bookTitle": title,
        "author": author,
        "genre": genre,
        "pageCount": page_count
    }

    try:
        result = collection.insert_one(instance)
        print("Added successfully")
    except Exception as e:
        print(f"An error occured while add new record: {e}")
    finally:
        return result

def read_all_title():
    try:
        cursor = collection.find()

        for doc in cursor:
            print(doc["bookTitle"])

    except Exception as e:
        print(f"Error: {e}")

def read_with_filter(filter:dict):
    try:
        cursor = collection.find(filter)
        
        for doc in cursor:
            print(doc["bookTitle"])

    except Exception as e:
        print(e)

def update_book(filter, update):
    result = None
    try:
        result = collection.update_one(filter, update)
        print("Updated successfully")
    except Exception as e:
        print(e)
    finally:
        return result

def delete_book(filter):
    result = None
    try:
        result = collection.delete_one(filter)
        print("Deleted successfully")
    except Exception as e:
        print(e)
    finally:
        return result

if __name__ == "__main__":
    if not connect_db():
        print("Error occured")
    
    # print(create_book(author="H.C. Armstrong", genre="Biyografi", page_count=500, title="Bozkurt"))

    # read_all_title()

    # read_with_filter({"pageCount": {"$gt": 300}})

    #print(update_book(where={"bookTitle": "Bozkurt"}, data={"$set": {"pageCount": 350}}))

    delete_book({"bookTitle": "Bozkurt"})

    close_connection()
