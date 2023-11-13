from transformers import pipeline
# You will need to "pip install transformers"
# You will also need to "pip install tensorflow"

classifier = pipeline("sentiment-analysis")
classifier("We are very happy to show you the 🤗 Transformers library.")

results = classifier(["We are very happy to show you the 🤗 Transformers library.", "We hope you don't hate it."])
for result in results:
    print(f"label: {result['label']}, with score: {round(result['score'], 4)}")