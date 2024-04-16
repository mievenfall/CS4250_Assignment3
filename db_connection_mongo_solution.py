#-------------------------------------------------------------------------
# AUTHOR: Evelyn Vu
# FILENAME: db_connection_mongo_solution.py
# SPECIFICATION: Manage inverted index of tables in database corpus using MongoDB
# FOR: CS 4250- Assignment #3
# TIME SPENT: 6
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

# importing some Python libraries
# --> add your Python code here
from pymongo import *;
import datetime

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here

    DB_HOST = "localhost"
    DB_PORT = 27017

    try:
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client.corpus
        return db
    
    except Exception as e:
        print("Connect database error: " + str(e))


def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    freq = {}
    terms = docText.lower().split(" ")

    # remove special characters (non-alphanum)
    for index, term in enumerate(terms):
        valid_term = ""
        for char in term:
            if char.isalnum():
                valid_term += char 
        terms[index] = valid_term

    # count term frequencies
    for term in terms:
        freq[term] = freq.get(term, 0) + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    objs = []

    # remove term repetition
    terms = list(set(terms))

    for term in terms:
        objs.append({
            "term": term,
            "count": freq[term],
            "num_chars": len(term)
        })


    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code here

    # get num chars in doc
    num_chars = 0
    for term in terms:
        num_chars += len(term)
    
    doc = {
        "_id": int(docId),
        "title": docTitle,
        "text":docText,
        "num_chars": num_chars,
        "date": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "category":docCat,
        "terms": objs
    }

    # insert the document
    # --> add your Python code here
    col.insert_one(doc)


def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.find_one({'_id': int(docId)})
    col.delete_one({'_id': int(docId)})


def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)


def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here

    inverted_index = {}

    # get docs
    docs = col.find()
    
    # for each doc
    for doc in docs:
        # get the current doc's terms
        terms = doc['terms']

        # for each term, create dict with document title mapped to term count
        for obj in terms:
            term = obj['term']
            curr_index = {doc['title']: obj['count']}
            curr_index_arr = inverted_index.get(term, [])
            curr_index_arr.append(curr_index)
            inverted_index[term] = curr_index_arr

    # reorder documents alphabetically by term
    sorted_inverted_index = sorted(list(inverted_index.items()))
    inverted_index = {}

    for (term, index) in sorted_inverted_index:
        index.sort( key=lambda obj: list(obj.keys())[0])

        # turn index into string
        index_str = ""
        for obj_position, obj in list(enumerate(index)):
            obj_items = list(obj.items())
            index_str += obj_items[0][0] + ":" + str(obj_items[0][1])
            
            # if this is not the last document/termcount pair, add a comma
            if(obj_position != len(index) - 1):
                index_str += ", "
            
        inverted_index[term] = index_str
        
    
    return inverted_index