from transformers import pipeline

def senti_menti(twitch_message: dict) -> dict:
    """
    Returns dictionary with sentiment score

    Args:
        twitch_message: json containing twitch chat
    """
    try:
        classifier = pipeline("sentiment-analysis")
        result = classifier(twitch_message['message'])
        label = result[0]['label']
        score = result[0]['score']

        twitch_message['sentiment_label'] = label
        twitch_message['sentiment_score'] = score
    
        if twitch_message is not None:
            return twitch_message
        else:
            pass

    except Exception:
        print(f'Model not applied to message:{twitch_message}')