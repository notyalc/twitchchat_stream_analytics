from transformers import pipeline
# You will need to "pip install transformers"
# You will also need to "pip install tensorflow"

# This is an example
# classifier = pipeline("sentiment-analysis")
# classifier("We are very happy to show you the 🤗 Transformers library.")

# results = classifier(["We are very happy to show you the 🤗 Transformers library.", "We hope you don't hate it."])
# for result in results:
#     print(f"label: {result['label']}, with score: {round(result['score'], 4)}")
#     print(result)

def senti_menti(twitch_message: dict):
    """ Returns dictionary with sentiment score"""
    classifier = pipeline("sentiment-analysis")
    result = classifier(twitch_message['message'])
    label = result[0]['label']
    score = result[0]['score']

    sentiment_result = {
        'user': twitch_message['user'],
        'sentiment_label': label,
        'sentiment_score': score
    }
    
    return sentiment_result

#Testing if function returns correct values"
# msg = {
#     'user': 'boxbag',
#     'message': 'Gnar is such an underrated carry'
# }

# print(senti_menti(msg))